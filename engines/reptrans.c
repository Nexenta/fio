#include <stdio.h>
#include <errno.h>
#include "../fio.h"
#include "../optgroup.h"
#include "reptrans.h"
#include "rtbuf.h"

static int initialized = 0;
struct repdev *rpdev[100];
char *transport[1] = {0};

struct enum_dev_arg {
	int n_dev;
	struct repdev **dev;
};

struct reptrans_opts {
	void *pad;
	char *driver;
	int autofill;
	uint64_t autofill_offset;
	int ttag;
};

int g_autofill = 0;

pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;

struct enum_dev_arg devices = {0, rpdev};

static void
enum_cb(struct repdev *dev, void *arg, int status) {

	struct enum_dev_arg *da = (struct enum_dev_arg *) arg;

	if (status == 0)
		da->dev[da->n_dev++] = dev;
}

static int
fio_reptrans_queue(struct thread_data *td, struct io_u *io_u) {

	struct fio_file *f = io_u->file;
	struct reptrans_opts *opt = td->eo;
	uint64_t offset = io_u->offset;
	crypto_state_t S;
	int err;

_retry_autofill:;

	uint512_t nhid = {{{0, 0}, {0, 0}},
					  {{0, 0}, {0, 0}}};

	if ((err = crypto_init_with_type(&S, HASH_TYPE_XXHASH_256)) != 0)
		td_verror(td, err, "crypto_init_with_type");
	if ((err = crypto_update(&S, (uint8_t *) &offset,
			sizeof(unsigned long))) != 0)
		td_verror(td, err, "crypto_update");
	if ((err = crypto_final(&S, (uint8_t *) &nhid)) != 0)
		td_verror(td, err, "crypto_final");

	assert((offset % td->o.min_bs[DDIR_WRITE]) == 0);

	if (io_u->ddir == DDIR_WRITE) {

		rtbuf_t *rb = rtbuf_init_alloc_one(io_u->xfer_buflen);
		memcpy(rb->bufs[0].base, io_u->xfer_buf, io_u->xfer_buflen);
		rb->bufs[0].len = io_u->xfer_buflen;

		err = reptrans_put_blob_with_attr(devices.dev[f->fd], opt->ttag,
		    HASH_TYPE_XXHASH_256, rb, &nhid, 0, -1);
		if (err) {
			io_u->error = err;
			td_verror(td, io_u->error, "putblob");
		}
		rtbuf_destroy(rb);

	} else if (io_u->ddir == DDIR_READ) {
		rtbuf_t *rb = NULL;
		err = reptrans_get_blob(devices.dev[f->fd], opt->ttag,
				HASH_TYPE_XXHASH_256, &nhid, &rb);

		/* autofill missing blob and re-read */

		if (err == -2 && g_autofill == 1) {
			if (opt->autofill_offset) {
				offset = opt->autofill_offset;
				goto _retry_autofill;
			}
			char *buffer = malloc(io_u->xfer_buflen);

			fill_io_buffer(td, buffer, io_u->xfer_buflen,
					io_u->xfer_buflen);

			rtbuf_t *inflate = rtbuf_init_alloc_one(io_u->xfer_buflen);
			memcpy(inflate->bufs[0].base, buffer, io_u->xfer_buflen);
			inflate->bufs[0].len = io_u->xfer_buflen;

			err = reptrans_put_blob_with_attr(devices.dev[f->fd],
			    opt->ttag, HASH_TYPE_XXHASH_256, inflate, &nhid,
			    0, -1);

			if (err != 0)
				td_verror(td, io_u->error, "inflated putblob");

			memcpy(io_u->xfer_buf, buffer, io_u->xfer_buflen);
			free(buffer);
			rtbuf_destroy(inflate);
			td->io_issues[DDIR_WRITE]++;
			td->io_issue_bytes[DDIR_WRITE] += io_u->xfer_buflen;
			opt->autofill_offset = offset;
			return FIO_Q_COMPLETED;
		}

		if (err != 0) {
			io_u->error = err;
			td_verror(td, io_u->error, "getblob");
		}

		if (rb) {
			memcpy(io_u->xfer_buf, rb->bufs[0].base, rb->bufs[0].len);
			rtbuf_destroy(rb);
		}

	}

	return FIO_Q_COMPLETED;
}

static int
fio_reptrans_init(struct thread_data *td) {

	pthread_mutex_lock(&init_mutex);
	struct reptrans_opts *opt = td->eo;
	transport[0] = opt->driver;
	if (initialized) {
		pthread_mutex_unlock(&init_mutex);
		return 0;
	}

	if (reptrans_init(0, NULL, NULL,
			RT_FLAG_ALLOW_OVERWRITE | RT_FLAG_STANDALONE | RT_FLAG_CREATE, 1,
			(char **) transport, NULL) <= 0) {
		td_verror(td, ENOSYS, "init failed");;
		exit(1);
	}

	if (reptrans_enum(NULL, &devices, enum_cb, 0) != 0) {
		td_verror(td, ENODEV, "enum");
		exit(1);
	}

	initialized = 1;

	if (td->eo != NULL && opt->autofill == 1) {
		g_autofill = 1;
		opt->autofill_offset = 0;
		printf("autofill enabled\n");
	}

	if (td->eo != NULL) {
		printf("using ttag %s\n", type_tag_name[opt->ttag]);
	}

	pthread_mutex_unlock(&init_mutex);

	return 0;
}

static void
fio_reptrans_cleanup(struct thread_data *td) {
	pthread_mutex_lock(&init_mutex);
	if (initialized) {
		reptrans_destroy();
		reptrans_close_all_rt();
		initialized = 0;
	}
	pthread_mutex_unlock(&init_mutex);
}


static int
fio_reptrans_open(struct thread_data *td, struct fio_file *f) {

	int i;

	const char *fname = f->file_name;
	if (*f->file_name == '\\')
		fname = (char *) f->file_name + 4;

	printf("opening %s\n", fname);

	for (i = 0; i < devices.n_dev; i++) {
		if (strcmp(fname, devices.dev[i]->name) == 0)
			break;
	}

	if (i == devices.n_dev) {
		fprintf(stderr, "Devname %s not found\n", fname);
		return ENODEV;
	}

	f->fd = i;
	td->o.open_files++;
	return 0;

}

/*
 * Hook for closing a file. See fio_reptrans_open().
 */
static int
fio_reptrans_close(struct thread_data *td, struct fio_file *f) {
	f->fd = -1;
	return 0;
}

static int
fio_reptrans_invalidate(struct thread_data *td, struct fio_file *f) {
	return 0;
}


struct fio_option fio_reptrans_options[] = {
		{
				.name = "autofill",
				.lname = "autofill missing blobs",
				.type = FIO_OPT_BOOL,
				.def = 0,
				.help = "write missing blobs to prevent preseed of devices",
				.off1 = offsetof(struct reptrans_opts, autofill),
				.category = FIO_OPT_C_ENGINE,
				.group = FIO_OPT_G_FILENAME,
		},
		{
				.name = "driver",
				.lname = "driver to use",
				.type = FIO_OPT_STR_STORE,
				.def = "rtrd",
				.cb = NULL,
				.help = "driver to use options are rtrd rtlfs",
				.off1 = offsetof(struct reptrans_opts, driver),
				.category = FIO_OPT_C_ENGINE,
				.group = FIO_OPT_G_FILENAME,
		},
		{
				.name = "ttag",
				.lname = "Reptrans TypeTag to use",
				.type = FIO_OPT_INT,
				.def = "4",
				.cb = NULL,
				.help = "read/write using specified TypeTag number",
				.off1 = offsetof(struct reptrans_opts, ttag),
				.category = FIO_OPT_C_ENGINE,
				.group = FIO_OPT_G_FILENAME,
		},
		{
				.name = NULL,
		},
};

struct ioengine_ops ioengine = {
		.name       = "reptrans",
		.version    = FIO_IOOPS_VERSION,
		.init       = fio_reptrans_init,
		.queue      = fio_reptrans_queue,
		.cleanup    = fio_reptrans_cleanup,
		.open_file  = fio_reptrans_open,
		.close_file = fio_reptrans_close,
		.invalidate = fio_reptrans_invalidate,
		.options    = fio_reptrans_options,
		.option_struct_size = sizeof(struct reptrans_opts),

		.flags      = FIO_SYNCIO | FIO_DISKLESSIO | FIO_NOEXTEND |
					  FIO_NODISKUTIL,
};

static void fio_init
fio_reptrans_register(void) {
	register_ioengine(&ioengine);
}

static void fio_exit
fio_reptrans_unregister(void) {
	unregister_ioengine(&ioengine);
}
