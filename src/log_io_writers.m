/*
 * Copyright (C) 2012 Mail.RU
 * Copyright (C) 2012 Yuriy Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#import <util.h>
#import <fiber.h>
#import <object.h>
#import <log_io.h>
#import <net_io.h>
#import <palloc.h>
#import <pickle.h>
#import <tbuf.h>
#import <say.h>

#include <third_party/crc32.h>

#include <stdio.h>
#include <sysexits.h>
#include <sys/socket.h>

void
wal_disk_writer_input_dispatch(va_list ap __attribute__((unused)))
{
	for (;;) {
		struct conn *c = ((struct ev_watcher *)yield())->data;
		tbuf_ensure(c->rbuf, 128 * 1024);

		ssize_t r = tbuf_recv(c->rbuf, c->fd);
		if (unlikely(r <= 0)) {
			if (r < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
					continue;
				say_syserror("%s: recv", __func__);
				panic("WAL writer connection read error");
			} else
				panic("WAL writer connection EOF");
		}

		while (tbuf_len(c->rbuf) > sizeof(u32) &&
		       tbuf_len(c->rbuf) >= *(u32 *)c->rbuf->ptr)
		{
			struct wal_reply *r = read_bytes(c->rbuf, sizeof(*r));
			if (unlikely(r->sender->fid != r->fid)) {
				say_warn("orphan WAL reply");
				continue;
			}
			resume(r->sender, r);
		}

		if (palloc_allocated(c->rbuf->pool) > 4 * 1024 * 1024)
			palloc_gc(c->pool);
	}
}


@implementation XLogWriter

- (i64) lsn { return lsn; }
- (i64) scn { return scn; }
- (struct child *) wal_writer { return wal_writer; };

- (void) set_lsn:(i64)lsn_ { lsn = lsn_; }
- (void) set_scn:(i64)scn_ { scn = scn_; }

- (void)
configure_wal_writer
{
	say_info("Configuring WAL writer LSN:%"PRIi64" SCN:%"PRIi64, lsn, scn);

	struct netmsg *n = netmsg_tail(&wal_writer->c->out_messages);
	net_add_iov_dup(&n, &lsn, sizeof(lsn));
	net_add_iov_dup(&n, &scn, sizeof(scn));
	net_add_iov_dup(&n, &run_crc_log, sizeof(run_crc_log));
	ev_io_start(&wal_writer->c->out);
}

- (int)
submit:(id<Txn>)txn
{
	struct wal_pack pack;
	wal_pack_prepare(self, &pack);
	[txn append:&pack];
	return [self wal_pack_submit];
}


- (int)
submit:(const void *)data len:(u32)data_len tag:(u16)tag
{
	struct row_v12 row = { .scn = 0,
			       .tag = tag };

	struct wal_pack pack;
	if (!wal_pack_prepare(self, &pack))
		return 0;

	wal_pack_append_row(&pack, &row);
	wal_pack_append_data(&pack, &row, data, data_len);
	return [self wal_pack_submit];
}

int
wal_pack_prepare(XLogWriter *w, struct wal_pack *pack)
{
	if (!w->local_writes) {
		say_warn("local writes disabled");
		return 0;
	}

	pack->netmsg = netmsg_tail(&w->wal_writer->c->out_messages);
	pack->packet_len = sizeof(*pack) - offsetof(struct wal_pack, packet_len);
	pack->fid = fiber->fid;
	pack->sender = fiber;
	pack->row_count = 0;
	net_add_iov(&pack->netmsg, &pack->packet_len, pack->packet_len);
	return 1;
}

u32
wal_pack_append_row(struct wal_pack *pack, struct row_v12 *row)
{
	assert(pack->row_count <= WAL_PACK_MAX);
	assert(row->tag & ~TAG_MASK);

	pack->packet_len += sizeof(*row) + row->len;
	pack->row_count++;
	net_add_iov(&pack->netmsg, row, sizeof(*row));
	if (row->len > 0)
		net_add_iov(&pack->netmsg, row->data, row->len);
	return WAL_PACK_MAX - pack->row_count;
}

void
wal_pack_append_data(struct wal_pack *pack, struct row_v12 *row,
		     const void *data, size_t len)
{
	pack->packet_len += len;
	row->len += len;
	net_add_iov(&pack->netmsg, data, len);
}

- (int)
wal_pack_submit
{
	ev_io_start(&wal_writer->c->out);
	struct wal_reply *reply = yield();
	assert(reply->lsn != -1);
	if (reply->lsn == 0)
		say_warn("wal writer returned error status");

	if (cfg.sync_scn_with_lsn && reply->lsn != reply->scn)
		panic("out ouf sync SCN:%"PRIi64 " != LSN:%"PRIi64,
		      reply->scn, reply->lsn);

	/* update recovery state */
	lsn = reply->lsn;
	scn = reply->scn;
	run_crc_log = reply->run_crc;
	crc_hist[++crc_hist_i % nelem(crc_hist)] = (struct crc_hist){ scn, run_crc_log };

	say_debug("%s: => rows:%i", __func__, reply->row_count);
	return reply->row_count;
}


- (i64)
append_row:(const void *)data len:(u32)data_len scn:(i64)scn_ tag:(u16)tag cookie:(u64)cookie
{
        if ([current_wal rows] == 1) {
		assert(current_wal->inprogress == true);
		/* rename wal after first successfull write to name without inprogress suffix*/
		if ([current_wal inprogress_rename] != 0) {
			say_error("can't rename inprogress wal");
			return -1;
		}
	}

	return [current_wal append_row:data len:data_len scn:scn_ tag:tag cookie:cookie];
}

- (int)
prepare_write:(i64)scn_
{
	if (current_wal == nil)
		current_wal = [wal_dir open_for_write:lsn + 1 scn:scn_];

        if (current_wal == nil) {
                say_error("can't open wal");
                return -1;
        }

	if (wal_to_close != nil) {
		[wal_to_close close];
		wal_to_close = nil;
	}
	return 0;
}

- (i64)
confirm_write
{
	static ev_tstamp last_flush;

	if (current_wal != nil) {
		lsn = [current_wal confirm_write];

		ev_tstamp fsync_delay = current_wal->dir->fsync_delay;
		if (fsync_delay >= 0 && ev_now() - last_flush >= fsync_delay) {
			if ([current_wal flush] < 0) {
				say_syserror("can't flush wal");
			} else {
				ev_now_update();
				last_flush = ev_now();
			}
		}

		if (current_wal->dir->rows_per_file <= [current_wal rows] ||
		    (lsn + 1) % current_wal->dir->rows_per_file == 0)
		{
			wal_to_close = current_wal;
			current_wal = nil;
		}
	}

	return lsn;
}


int
wal_disk_writer(int fd, void *state)
{
	XLogWriter *writer = state;
	struct tbuf *wbuf, rbuf = TBUF(NULL, 0, fiber->pool);
	int result = EXIT_FAILURE;
	i64 start_lsn, next_scn = 0;
	u32 crc, requests_processed;
	bool io_failure = false, have_unwritten_rows = false;
	ssize_t r;
	struct {
		struct fiber *sender;
		u32 fid;
		u32 row_count;
		u32 run_crc; /* run_crc is computed */
		i64 scn;
	} *request = xmalloc(sizeof(*request) * 1024);
	bool broken[1024];

	ev_tstamp start_time = ev_now();
	palloc_register_gc_root(fiber->pool, &rbuf, tbuf_gc);

	struct wal_conf { i64 lsn; i64 scn; u32 run_crc; } __attribute__((packed)) wal_conf;
	if ((r = recv(fd, &wal_conf, sizeof(wal_conf), 0)) != sizeof(wal_conf)) {
		if (r == 0) {
			result = EX_OK;
			goto exit;
		}
		say_syserror("recv: failed");
		panic("unable to start WAL writer");
	}
	[writer set_lsn:wal_conf.lsn];
	next_scn = wal_conf.scn;
	crc = wal_conf.run_crc;
	say_debug("%s: configured LSN:%"PRIi64 " SCN:%"PRIi64" run_crc_log:0x%x",
		  __func__, wal_conf.lsn, wal_conf.scn, wal_conf.run_crc);

	/* since wal_writer have bidirectional communiction to master
	   and checks for errors on send/recv,
	   there is no need in util.m:keepalive() */
	signal(SIGPIPE, SIG_IGN);

	for (;;) {
		if (!have_unwritten_rows) {
			tbuf_ensure(&rbuf, 16 * 1024);
			r = recv(fd, rbuf.end, tbuf_free(&rbuf), 0);
			if (r < 0 && (errno == EINTR))
				continue;
			else if (r < 0) {
				say_syserror("recv");
				result = EX_OSERR;
				goto exit;
			} else if (r == 0) {
				result = EX_OK;
				goto exit;
			}
			tbuf_append(&rbuf, NULL, r);
		}


		if (cfg.coredump > 0 && ev_now() - start_time > cfg.coredump * 60) {
			maximize_core_rlimit();
			cfg.coredump = 0;
		}

		start_lsn = [writer lsn];
		requests_processed = 0;
		have_unwritten_rows = false;
		io_failure = [writer prepare_write:next_scn] == -1;

		/* we're not running inside ev_loop, so update ev_now manually just before write */
		ev_now_update();

		while (tbuf_len(&rbuf) > sizeof(u32) && tbuf_len(&rbuf) >= *(u32 *)rbuf.ptr) {
			u32 row_count = ((u32 *)rbuf.ptr)[1];
			if (!io_failure && row_count > [writer->current_wal wet_rows_offset_available]) {
				assert(requests_processed != 0);
				have_unwritten_rows = true;
				break;
			}

			tbuf_ltrim(&rbuf, sizeof(u32)); /* drop packet_len */
			broken[requests_processed] = 0;
			request[requests_processed].row_count = read_u32(&rbuf);
			request[requests_processed].sender = read_ptr(&rbuf);
			request[requests_processed].fid = read_u32(&rbuf);

			for (int i = 0; i < request[requests_processed].row_count; i++) {
				struct row_v12 *h = read_bytes(&rbuf, sizeof(*h));
				void *data = read_bytes(&rbuf, h->len);

				if (io_failure)
					continue;

				say_debug("%s: SCN:%"PRIi64" tag:%s data_len:%u", __func__,
					  h->scn, xlog_tag_to_a(h->tag), h->len);

				/* FIXME: use packed repr! */
				i64 ret = [writer append_row:data
							 len:h->len
							 scn:h->scn
							 tag:h->tag
						      cookie:h->cookie];
				if (ret <= 0) {
					say_error("append_row failed");
					io_failure = true;
					continue;
				}

				int tag = h->tag & TAG_MASK;
				int tag_type = h->tag & ~TAG_MASK;
				if (tag_type == TAG_WAL && (tag == wal_tag || tag > user_tag))
					crc = crc32c(crc, data, h->len);

				/* next_scn is used for writing XLog header, which is turn used to
				   find correct file for replication replay.
				   so, next_scn should be updated only when data modification occurs */
				if (tag_type == TAG_WAL) {
					if (h->scn > 100 && h->scn - next_scn != 1) {
						say_warn("GAP %i rows:%i", (int)(h->scn - next_scn),
							 request[requests_processed].row_count);
						broken[requests_processed] = true;
					}
					next_scn = h->scn;
				}

				request[requests_processed].run_crc = crc;
				request[requests_processed].scn = next_scn;
			}
			requests_processed++;
		}
		if (requests_processed == 0)
			continue;

		assert(start_lsn > 0);
		u32 rows_confirmed = [writer confirm_write] - start_lsn;

		wbuf = tbuf_alloc(fiber->pool);
		for (int i = 0; i < requests_processed; i++) {
			struct wal_reply reply = { .packet_len = sizeof(reply),
						   .row_count = 0,
						   .sender = request[i].sender,
						   .fid = request[i].fid,
						   .lsn = 0,
						   .scn = 0,
						   .run_crc = request[i].run_crc };

			if (rows_confirmed > 0) {
				reply.row_count = MIN(rows_confirmed, request[i].row_count);
				rows_confirmed -= reply.row_count;
				start_lsn += reply.row_count;
				reply.lsn = start_lsn;
				reply.scn = request[i].scn ?: reply.lsn;

				if (broken[i]) {
					reply.lsn = -1;
					reply.scn = -1;
				}
			}

			tbuf_append(wbuf, &reply, sizeof(reply));

			say_debug("%s: wrote rows:%i  maxLSN:%"PRIi64,
				  __func__, reply.row_count, reply.lsn);
		}


		do {
			ssize_t r = write(fd, wbuf->ptr, tbuf_len(wbuf));
			if (r < 0) {
				if (errno == EINTR)
					continue;
				/* parent is dead, exit quetly */
				result = EX_OK;
				goto exit;
			}
			tbuf_ltrim(wbuf, r);
		} while (tbuf_len(wbuf) > 0);

		fiber_gc();
	}
exit:
	[writer->current_wal close];
	writer->current_wal = nil;
	return result;
}

int
snapshot_write_row(XLog *l, u16 tag, struct tbuf *row)
{
	static int bytes;
	ev_tstamp elapsed;
	static ev_tstamp last = 0;
	const int io_rate_limit = l->dir->writer->snap_io_rate_limit;

	if ([l append_row:row->ptr len:tbuf_len(row) scn:0 tag:(tag | TAG_SNAP)] < 0) {
		say_error("unable write row");
		return -1;
	}

	if (io_rate_limit > 0) {
		if (last == 0) {
			ev_now_update();
			last = ev_now();
		}

		bytes += tbuf_len(row) + sizeof(struct row_v12);

		while (bytes >= io_rate_limit) {
			if ([l flush] < 0) {
				say_error("unable to flush");
				return -1;
			}

			ev_now_update();
			elapsed = ev_now() - last;
			if (elapsed < 1)
				usleep(((1 - elapsed) * 1000000));

			ev_now_update();
			last = ev_now();
			bytes -= io_rate_limit;
		}
	}
	return 0;
}


- (u32)
snapshot_estimate
{
	return 0;
}

- (int)
snapshot_write_header_rows:(XLog *)snap
{
	(void)snap;
	return 0;
}

- (int)
snapshot_write_rows:(XLog *)snap
{
	(void)snap;
	return 0;
}

- (int)
snapshot_write
{
        XLog *snap;
	const char *final_filename, *filename;

	say_debug("%s: lsn:%"PRIi64" scn:%"PRIi64, __func__, lsn, scn);
	snap = [snap_dir open_for_write:lsn scn:scn];
	if (snap == nil) {
		say_syserror("can't open snap for writing");
		return -1;
	}
	snap->inprogress = false; /* we do .inprogress rename ourself */
	snap->no_wet = true; /* disable wet row tracking */;

	/*
	 * While saving a snapshot, snapshot name is set to
	 * <lsn>.snap.inprogress. When done, the snapshot is
	 * renamed to <lsn>.snap.
	 */

	final_filename = strdup([snap final_filename]);
	filename = strdup(snap->filename);

	say_info("saving snapshot `%s'", final_filename);

	struct tbuf *snap_ini = tbuf_alloc(fiber->pool);
	u32 rows = [self snapshot_estimate];
	tbuf_append(snap_ini, &rows, sizeof(rows));
	tbuf_append(snap_ini, &run_crc_log, sizeof(run_crc_log));
	u32 run_crc_mod = 0;
	tbuf_append(snap_ini, &run_crc_mod, sizeof(run_crc_mod));

	if ([snap append_row:snap_ini->ptr len:tbuf_len(snap_ini) scn:scn tag:(snap_initial_tag | TAG_SNAP)] < 0) {
		say_error("unable write initial row");
		return -1;
	}

	if ([self snapshot_write_header_rows:snap] < 0) /* FIXME: this is ugly */
		return -1;

	if ([self snapshot_write_rows:snap] < 0)
		return -1;

	const char end[] = "END";
	if ([snap append_row:end len:strlen(end) scn:scn tag:(snap_final_tag | TAG_SNAP)] < 0) {
		say_error("unable write final row");
		return -1;
	}
	if ([snap flush] == -1) {
		say_syserror("snap flush failed");
		return -1;
	}

	[snap fadvise_dont_need];

	if ([snap close] == -1) {
		say_syserror("snap close failed");
		return -1;
	}
	snap = nil;

	if (link(filename, final_filename) == -1) {
		say_syserror("can't create hard link to snapshot");
		return -1;
	}

	if (unlink(filename) == -1) {
		say_syserror("can't unlink 'inprogress' snapshot");
		return -1;
	}

	say_info("done");
	return 0;
}

- (int)
snapshot:(bool)sync
{
	pid_t p;

	switch ((p = tnt_fork())) {
	case -1:
		say_syserror("fork");
		return -1;

	case 0: /* child, the dumper */
		fiber->name = "dumper";
		set_proc_title("dumper (%" PRIu32 ")", getppid());
		fiber_destroy_all();
		palloc_unmap_unused();
		close_all_xcpt(2, stderrfd, sayfd);

		int r = [self snapshot_write];

#ifdef COVERAGE
		__gcov_flush();
#endif
		_exit(r != 0 ? errno : 0);

	default: /* parent, may wait for child */
		return sync ? wait_for_child(p) : 0;
	}
}

- (int)
snapshot_initial
{
	lsn = scn = 1;
	return [self snapshot_write];
}

@end

register_source();