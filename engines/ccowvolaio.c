/*
 * volaio engine
 *
 * (c) 2015 Nexenta Systems, Inc.
 *
 * IO engine that uses ccow vol aio interface.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>

#include "../optgroup.h"
#include "../fio.h"
#include "libccowvol.h"

struct volaio_data {
	struct io_u **aio_events;
	unsigned int queued;
};

pthread_mutex_t fio_volaio_init_mutex = PTHREAD_MUTEX_INITIALIZER;

#define FIO_VOLAIO_MAX_OPEN_FILES 255
#define FIO_VOLAIO_BLK_SIZE		4096
#define FIO_VOLAIO_VOL_SIZE (1024 * 1024 * 1024L)

struct open_objects_t {
	char * name;
	struct ccow_info *ci;
};

struct volaio_params {
	char *dummy; /* create some offset */
	char *bucket;
	int chunk_size;
	int repcount;
};

static struct open_objects_t vol_objects[FIO_VOLAIO_MAX_OPEN_FILES];
static pthread_mutex_t vol_obj_mutex = PTHREAD_MUTEX_INITIALIZER;


static int fill_timespec(struct timespec *ts)
{
#ifdef CONFIG_CLOCK_GETTIME
#ifdef CONFIG_CLOCK_MONOTONIC
	clockid_t clk = CLOCK_MONOTONIC;
#else
	clockid_t clk = CLOCK_REALTIME;
#endif
	if (!clock_gettime(clk, ts))
		return 0;

	perror("clock_gettime");
	return 1;
#else
	struct timeval tv;

	gettimeofday(&tv, NULL);
	ts->tv_sec = tv.tv_sec;
	ts->tv_nsec = tv.tv_usec * 1000;
	return 0;
#endif
}

static unsigned long long ts_utime_since_now(struct timespec *t)
{
	long long sec, nsec;
	struct timespec now;

	if (fill_timespec(&now))
		return 0;

	sec = now.tv_sec - t->tv_sec;
	nsec = now.tv_nsec - t->tv_nsec;
	if (sec > 0 && nsec < 0) {
		sec--;
		nsec += 1000000000;
	}

	sec *= 1000000;
	nsec /= 1000;
	return sec + nsec;
}

static int fio_volaio_cancel(struct thread_data fio_unused *td,
			       struct io_u *io_u)
{
	return ccow_vol_cancel(io_u->vol_aio_cb.ci, &io_u->vol_aio_cb);
}

static int fio_volaio_prep(struct thread_data fio_unused *td,
			     struct io_u *io_u)
{
	ccow_aio_t vol_aio = &io_u->vol_aio_cb;
	struct fio_file *f = io_u->file;

	vol_aio->ci = FILE_ENG_DATA(f);
	vol_aio->aio_buf = io_u->xfer_buf;
	vol_aio->aio_nbytes = io_u->xfer_buflen;
	vol_aio->aio_offset = io_u->offset;
	vol_aio->aio_sigevent = CCOW_VOL_SIGEV_NONE;

	io_u->seen = 0;
	return 0;
}

#define SUSPEND_ENTRIES	512

static int fio_volaio_getevents(struct thread_data *td, unsigned int min,
				  unsigned int max, const struct timespec *t)
{
	struct volaio_data *pd = td->io_ops_data;
	ccow_aio_t suspend_list[SUSPEND_ENTRIES];
	struct timespec start;
	int have_timeout = 0;
	int suspend_entries;
	struct io_u *io_u;
	unsigned int r;
	int i;

	if (t && !fill_timespec(&start))
		have_timeout = 1;
	else
		memset(&start, 0, sizeof(start));

	r = 0;
restart:
	memset(suspend_list, 0, sizeof(*suspend_list));
	suspend_entries = 0;
	io_u_qiter(&td->io_u_all, io_u, i) {
		int err;

		if (io_u->seen || !(io_u->flags & IO_U_F_FLIGHT))
			continue;

		err = ccow_vol_error(&io_u->vol_aio_cb);
		if (err == EINPROGRESS) {
			if (suspend_entries < SUSPEND_ENTRIES) {
				suspend_list[suspend_entries] = &io_u->vol_aio_cb;
				suspend_entries++;
			}
			continue;
		}

		io_u->seen = 1;
		pd->queued--;
		pd->aio_events[r++] = io_u;

		if (err == ECANCELED)
			io_u->resid = io_u->xfer_buflen;
		else if (!err) {
			ssize_t retval = ccow_vol_return(&io_u->vol_aio_cb);

			io_u->resid = io_u->xfer_buflen - retval;
		} else
			io_u->error = err;
	}

	if (r >= min)
		return r;

	if (have_timeout) {
		unsigned long long usec;

		usec = (t->tv_sec * 1000000) + (t->tv_nsec / 1000);
		if (ts_utime_since_now(&start) > usec)
			return r;
	}

	/*
	 * must have some in-flight, wait for at least one
	 */
	ccow_vol_suspend((ccow_aio_t *)suspend_list, suspend_entries, t);
	goto restart;
}

static struct io_u *fio_volaio_event(struct thread_data *td, int event)
{
	struct volaio_data *pd = td->io_ops_data;

	return pd->aio_events[event];
}

static int fio_volaio_queue(struct thread_data *td,
			      struct io_u *io_u)
{
	struct volaio_data *pd = td->io_ops_data;
	ccow_aio_t vol_aio = &io_u->vol_aio_cb;
	int ret;

	fio_ro_check(td, io_u);

	if (io_u->ddir == DDIR_READ)
		ret = ccow_vol_read(vol_aio);
	else if (io_u->ddir == DDIR_WRITE)
		ret = ccow_vol_write(vol_aio);
	else {
		if (pd->queued)
			return FIO_Q_BUSY;

		do_io_u_sync(td, io_u);
		return FIO_Q_COMPLETED;
	}

	if (ret) {
		int aio_err = ret;

		/*
		 * At least OSX has a very low limit on the number of pending
		 * IOs, so if it returns EAGAIN, we are out of resources
		 * to queue more. Just return FIO_Q_BUSY to naturally
		 * drop off at this depth.
		 */
		if (aio_err == EAGAIN)
			return FIO_Q_BUSY;

		io_u->error = aio_err;
		td_verror(td, io_u->error, "xfer");
		return FIO_Q_COMPLETED;
	}

	pd->queued++;
	return FIO_Q_QUEUED;
}

static int fio_volaio_open(struct thread_data *td, struct fio_file *f)
{
	int i;
	char path[PATH_MAX];

	pthread_mutex_lock(&vol_obj_mutex);
	// TODO: improve search / insert, narrow mutex scope
	for (i = 3; i < FIO_VOLAIO_MAX_OPEN_FILES; i++) {
		if (vol_objects[i].name &&
				!strcmp(f->file_name, vol_objects[i].name)) {
			f->fd = i;
			pthread_mutex_unlock(&vol_obj_mutex);
			return 0;
		}
	}
	for (i = 3; i < FIO_VOLAIO_MAX_OPEN_FILES; i++) {
		if (vol_objects[i].ci == NULL) {
			uint64_t vol_size;
			struct volaio_params *params = td->eo;

			vol_objects[i].name = strdup(f->file_name);
			if (vol_objects[i].name == NULL)
				goto volaio_open_clean;

			vol_objects[i].ci = calloc(1, sizeof(struct ccow_info));
			if (vol_objects[i].ci == NULL)
				goto volaio_open_clean;

			snprintf(path, PATH_MAX, "%s/%s",
					params->bucket, f->file_name);
			if (ccow_vol_open(vol_objects[i].ci, path, 0, &vol_size)) {
				goto volaio_open_clean;
			}
			f->fd = i;
			FILE_SET_ENG_DATA(f, vol_objects[i].ci);
			pthread_mutex_unlock(&vol_obj_mutex);
			return 0;
		}
	}
	/* Max number of object opened */
	return 1;
volaio_open_clean:
	pthread_mutex_unlock(&vol_obj_mutex);
	if (vol_objects[i].ci) {
		free(vol_objects[i].ci);
		vol_objects[i].ci = 0;
	}
	if(vol_objects[i].name) {
		free(vol_objects[i].name);
		vol_objects[i].name = 0;
	}
	return 1;
}

static int fio_volaio_close(struct thread_data *td, struct fio_file *f)
{
	int i;
	pthread_mutex_lock(&vol_obj_mutex);
	// TODO: improve search / insert, narrow mutex scope
	for (i = 3; i < FIO_VOLAIO_MAX_OPEN_FILES; i++) {
		if (vol_objects[i].name &&
				!strcmp(f->file_name, vol_objects[i].name)) {
			ccow_vol_close(vol_objects[i].ci);
			free(vol_objects[i].ci);
			vol_objects[i].ci = NULL;
			FILE_SET_ENG_DATA(f, NULL);
			free(vol_objects[i].name);
			vol_objects[i].name = NULL;
			f->fd = -1;
			pthread_mutex_unlock(&vol_obj_mutex);
			return 0;
		}
	}
	pthread_mutex_unlock(&vol_obj_mutex);
	return 1;
}

static void fio_volaio_cleanup(struct thread_data *td)
{
	struct volaio_data *pd = td->io_ops_data;

	if (pd) {
		free(pd->aio_events);
		free(pd);
	}
	td->io_ops_data = NULL;
}

static int
fio_volaio_setup_objects(struct thread_data *td)
{
	int err = 0;
	unsigned int i;
	static ccow_t cl = NULL;
	struct ccow_info ci;
	ccow_completion_t c;
	struct fio_file *f;
	struct volaio_params *params = td->eo;
	uint16_t nv = 1;
	uint8_t ht = 8; // HASH_TYPE_XXHASH_128;
	int fd;
	char *buf;
	struct iovec iov[1];
	uint32_t cs = params->chunk_size;
	uint32_t rc = params->repcount;
	uint64_t vs = FIO_VOLAIO_VOL_SIZE;
	uint32_t bs = FIO_VOLAIO_BLK_SIZE;
#define FIO_VOLAIO_VOL_SIZE_STR (char *)"X-volsize"
#define FIO_VOLAIO_BLK_SIZE_STR (char *)"X-blocksize"
#define FIO_VOLAIO_VOL_SIZE_STR_SIZE (strlen(FIO_VOLAIO_VOL_SIZE_STR) + 1)
#define FIO_VOLAIO_BLK_SIZE_STR_SIZE (strlen(FIO_VOLAIO_BLK_SIZE_STR) + 1)

	if (sscanf(params->bucket, "%2047[^/]/%2047[^/]/%2047[^\n]",
		    ci.cid, ci.tid, ci.bid) < 3) {
		return -1;
	}
	ci.cid_size = strlen(ci.cid) + 1;
	ci.tid_size = strlen(ci.tid) + 1;
	ci.bid_size = strlen(ci.bid) + 1;

	char ccow_json[PATH_MAX] = "";
	char *nedge_home = getenv("NEDGE_HOME");
	if (NULL == nedge_home)
		nedge_home = "/opt/nedge";
	snprintf(ccow_json, PATH_MAX, "%s/etc/ccow/ccow.json", nedge_home);

	pthread_mutex_lock(&fio_volaio_init_mutex);

	fd = open(ccow_json, O_RDONLY);
	if (fd < 0) {
		return fd;
	}
	buf = calloc(1, 16384);
	if (!buf) {
		return -1;
	}
	if (read(fd, buf, 16383) == -1) {
		free(buf);
		close(fd);
		return -1;
	}
	close(fd);

	err = ccow_tenant_init(buf, ci.cid, ci.cid_size, ci.tid, ci.tid_size, &cl);
	free(buf);
	if (err) {
		return err;
	}
	if (!cl) {
		return -1;
	}
	err = ccow_bucket_create(cl, ci.bid, ci.bid_size, NULL);
	if ((err != 0) && (err != -EEXIST)) {
		goto term_and_exit;
	}

	err = ccow_create_completion(cl, NULL, NULL, 1, &c);
	if (err) {
		goto release_and_exit;
	}

	err = ccow_attr_modify_default(c, CCOW_ATTR_REPLICATION_COUNT,
		(void *) &rc, NULL);
	if (err) {
		goto release_and_exit;
	}

	err = ccow_attr_modify_default(c, CCOW_ATTR_CHUNKMAP_CHUNK_SIZE,
		(void *) &cs, NULL);
	if (err) {
		goto release_and_exit;
	}
	err = ccow_attr_modify_custom(c, CCOW_KVTYPE_UINT64,
			FIO_VOLAIO_VOL_SIZE_STR, FIO_VOLAIO_VOL_SIZE_STR_SIZE,
			&vs, 0, NULL);
	if (err) {
		goto release_and_exit;
	}

	err = ccow_attr_modify_custom(c, CCOW_KVTYPE_UINT32,
			FIO_VOLAIO_BLK_SIZE_STR, FIO_VOLAIO_BLK_SIZE_STR_SIZE,
			&bs, 0, NULL);
	if (err) {
		goto release_and_exit;
	}

	err = ccow_attr_modify_default(c, CCOW_ATTR_NUMBER_OF_VERSIONS,
		(void *) &nv, NULL);
	if (err) {
		goto release_and_exit;
	}

	err = ccow_attr_modify_default(c, CCOW_ATTR_HASH_TYPE,
		(void *) &ht, NULL);
	if (err) {
		goto release_and_exit;
	}

	iov[0].iov_len = params->chunk_size;
	iov[0].iov_base = malloc(iov[0].iov_len);
	if (!iov[0].iov_base) {
		err = -1;
		goto release_and_exit;
	}

	/* put ccow object with name of fio generated file names */
	for_each_file(td, f, i) {
		ccow_put(ci.bid, ci.bid_size, f->file_name, strlen(f->file_name) + 1,
				c, &iov[0], 1, 0);
		log_info("Put ccow object %s/%s chunkSize=%d\n", params->bucket,
		    f->file_name, params->chunk_size);
	}
	err = ccow_wait(c, -1);
	free(iov[0].iov_base);
release_and_exit:
	ccow_release(c);
term_and_exit:
	ccow_tenant_term(cl);
	pthread_mutex_unlock(&fio_volaio_init_mutex);
	return err;
}

static int fio_volaio_init(struct thread_data *td)
{
	struct volaio_data *pd = calloc(1,sizeof(*pd));

	if (!pd)
		return 1;
	pd->aio_events = calloc(1, td->o.iodepth * sizeof(struct io_u *));
	td->io_ops_data = pd;

	return fio_volaio_setup_objects(td);
}

static struct fio_option options[] = {
	{
		.name	= "bucket",
		.lname	= "CCOW bucket",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct volaio_params, bucket),
		.cb	= NULL,
		.help	= "Place to store CCOW object",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_FILENAME,
	},
	{
		.name	= "repcount",
		.lname	= "CCOW object replication count",
		.type	= FIO_OPT_INT,
		.def	= "1",
		.off1	= offsetof(struct volaio_params, repcount),
		.cb	= NULL,
		.help	= "Specifies replication count of object to be created. Default is 1. (Range: 1-4)",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_FILENAME,
	},
	{
		.name	= "chunk_size",
		.lname	= "CCOW object chunk size",
		.type	= FIO_OPT_INT,
		.def	= "32768",
		.off1	= offsetof(struct volaio_params, chunk_size),
		.cb	= NULL,
		.help	= "Specifies chunk size of object to be created. Default is 32768",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_FILENAME,
	},
	{
		.name	= NULL,
	},
};

static struct ioengine_ops ioengine = {
	.name		= "ccowvolaio",
	.version	= FIO_IOOPS_VERSION,
	.init		= fio_volaio_init,
	.prep		= fio_volaio_prep,
	.queue		= fio_volaio_queue,
	.cancel		= fio_volaio_cancel,
	.getevents	= fio_volaio_getevents,
	.event		= fio_volaio_event,
	.cleanup	= fio_volaio_cleanup,
	.open_file	= fio_volaio_open,
	.close_file	= fio_volaio_close,
	.options    = options,
	.option_struct_size = sizeof(struct volaio_params),
	.flags		= FIO_DISKLESSIO,
};

static void fio_init fio_volaio_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_volaio_unregister(void)
{
	unregister_ioengine(&ioengine);
}
