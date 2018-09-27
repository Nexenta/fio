/*
 * ccowobj engine
 *
 * (c) 2017 Nexenta Systems, Inc.
 *
 * IO engine that uses ccow object interface.
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
#include "ccow.h"

pthread_mutex_t fio_ccowobj_init_mutex = PTHREAD_MUTEX_INITIALIZER;

struct ccowobj_opts {
	void *pad;
	char *cluster;
	char *tenant;
	int repcount;
	int delete;
};

static const char*
fio_ccowobj_parse_bkname(char *file_name)
{
	const char *bkname = file_name;
	if (*file_name == '\\')
		bkname = (char *)file_name + 4;
	return bkname;
}

static int
fio_ccowobj_queue(struct thread_data *td, struct io_u *io_u) {

	struct fio_file *f = io_u->file;
	ccow_completion_t c = NULL;
	ccow_t cl = (ccow_t)td->io_ops_data;
	struct ccowobj_opts *opts = td->eo;
	struct iovec iov[1];
	char fname[64];
	int err;
	uint8_t repcount = opts->repcount;
	uint8_t ht = 8; // HASH_TYPE_XXHASH_128;
	uint16_t nv = 1;
	uint32_t cs = (4 * 1024 * 1024);
	const char *bkname = fio_ccowobj_parse_bkname(f->file_name);

	err = ccow_create_completion(cl, NULL, NULL, 1, &c);
	if (err) {
		return err;
	}

	err = ccow_attr_modify_default(c, CCOW_ATTR_CHUNKMAP_CHUNK_SIZE,
		(void *) &cs, NULL);
	if (err) {
		goto release_and_exit;
	}

	if (io_u->xfer_buflen > cs)
		return -EBADF;

	err = ccow_attr_modify_default(c, CCOW_ATTR_REPLICATION_COUNT,
		(void *) &repcount, NULL);
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

	iov[0].iov_len = io_u->xfer_buflen;
	iov[0].iov_base = io_u->xfer_buf;

	sprintf(fname, "%llx", io_u->offset);
	if (io_u->ddir == DDIR_WRITE) {
		err = ccow_put(bkname, strlen(bkname) + 1,
		    fname, strlen(fname) + 1, c, &iov[0], 1, 0);
	} else if (io_u->ddir == DDIR_READ) {
		err = ccow_get(bkname, strlen(bkname) + 1,
		    fname, strlen(fname) + 1, c, &iov[0], 1, 0, NULL);
	}
	if (err) {
		goto release_and_exit;
	}

	err = ccow_wait(c, -1);
	if (!err)
		c = NULL;

	if (opts->delete) {
		err = ccow_create_completion(cl, NULL, NULL, 1, &c);
		if (err) {
			goto release_and_exit;
		}
		err = ccow_delete(bkname, strlen(bkname) + 1, fname, strlen(fname) + 1, c);
		if (err) {
			goto release_and_exit;
		}
		err = ccow_wait(c, -1);
		if (!err)
			c = NULL;
	}

	return FIO_Q_COMPLETED;

release_and_exit:
	if (c)
		ccow_release(c);
	if (err) {
		io_u->error = err;
		td_verror(td, io_u->error, "ccowop");
	}
	return err;
}

static int
fio_ccowobj_init(struct thread_data *td)
{
	int i;
	struct fio_file *f;
	ccow_t cl = NULL;
	int err = 0, fd;
	char *buf;
	struct ccowobj_opts *opts = td->eo;

	char ccow_json[PATH_MAX] = "";
	char *nedge_home = getenv("NEDGE_HOME");
	if (NULL == nedge_home)
		nedge_home = "/opt/nedge";
	snprintf(ccow_json, PATH_MAX, "%s/etc/ccow/ccow.json", nedge_home);

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

	pthread_mutex_lock(&fio_ccowobj_init_mutex);
	err = ccow_tenant_init(buf, opts->cluster, strlen(opts->cluster) + 1,
	    opts->tenant, strlen(opts->tenant) + 1, &cl);
	free(buf);
	if (err || !cl) {
		pthread_mutex_unlock(&fio_ccowobj_init_mutex);
		return err;
	}

	for_each_file(td, f, i) {
		const char *bkname = fio_ccowobj_parse_bkname(f->file_name);
		err = ccow_bucket_create(cl, bkname, strlen(bkname) + 1, NULL);
		if ((err != 0) && (err != -EEXIST)) {
			goto term_and_exit;
		}
	}

	td->io_ops_data = cl;
	pthread_mutex_unlock(&fio_ccowobj_init_mutex);
	return 0;

term_and_exit:
	td->io_ops_data = NULL;
	ccow_tenant_term(cl);
	pthread_mutex_unlock(&fio_ccowobj_init_mutex);
	return err;
}

static void
fio_ccowobj_cleanup(struct thread_data *td)
{
}

static int
fio_ccowobj_open(struct thread_data *td, struct fio_file *f)
{
	f->fd = -1;
	td->o.open_files++;
	return 0;

}

static int
fio_ccowobj_close(struct thread_data *td, struct fio_file *f)
{
	ccow_t cl = (ccow_t)td->io_ops_data;
	f->fd = -1;
	if (cl)
		ccow_tenant_term(cl);
	return 0;
}

static int
fio_ccowobj_invalidate(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

struct fio_option fio_ccowobj_options[] = {
	{
		.name	= "cluster",
		.lname	= "CCOW cluster",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct ccowobj_opts, cluster),
		.cb	= NULL,
		.help	= "Place to store CCOW cluster",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_FILENAME,
	},
	{
		.name	= "tenant",
		.lname	= "CCOW cluster",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct ccowobj_opts, tenant),
		.cb	= NULL,
		.help	= "Place to store CCOW tenant",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_FILENAME,
	},
	{
		.name	= "delete",
		.lname	= "CCOW object delete after create",
		.type	= FIO_OPT_INT,
		.def	= "0",
		.off1	= offsetof(struct ccowobj_opts, delete),
		.cb	= NULL,
		.help	= "Specifies replication if a new object has to be deleted. (Range: 0-1)",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_FILENAME,
	},
	{
		.name	= "repcount",
		.lname	= "CCOW object replication count",
		.type	= FIO_OPT_INT,
		.def	= "1",
		.off1	= offsetof(struct ccowobj_opts, repcount),
		.cb	= NULL,
		.help	= "Specifies replication count of object to be created. Default is 1. (Range: 1-4)",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_FILENAME,
	},
	{
		.name = NULL,
	},
};

struct ioengine_ops ccowobj_ioengine = {
	.name       = "ccowobj",
	.version    = FIO_IOOPS_VERSION,
	.init       = fio_ccowobj_init,
	.queue      = fio_ccowobj_queue,
	.cleanup    = fio_ccowobj_cleanup,
	.open_file  = fio_ccowobj_open,
	.close_file = fio_ccowobj_close,
	.invalidate = fio_ccowobj_invalidate,
	.options    = fio_ccowobj_options,
	.option_struct_size = sizeof(struct ccowobj_opts),

	.flags      = FIO_SYNCIO | FIO_DISKLESSIO | FIO_NOEXTEND |
		FIO_NODISKUTIL,
};

static void fio_init
fio_ccowobj_register(void)
{
	register_ioengine(&ccowobj_ioengine);
}

static void fio_exit
fio_ccowobj_unregister(void)
{
	unregister_ioengine(&ccowobj_ioengine);
}
