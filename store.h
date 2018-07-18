/*
 * Copyright (C) 2010, 2011, 2012, 2013 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013 Yuriy Vostrikov
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
#ifndef OCTOPUS_MEMCACHED_H
#define OCTOPUS_MEMCACHED_H

#include <util.h>
#include <index.h>

#import <log_io.h>
#import <fiber.h>

@interface Memcached : Object<Executor>
{
	struct Fiber* expire_fiber;

@public
	Shard<Shard>* shard;
	CStringHash*  mc_index;
}
@end

enum object_type
{
	MC_OBJ = 1
};

struct mc_obj
{
	u32 exptime;
	u32 flags;
	u64 cas;
	u16 key_len; /* including \0 */
	u16 suffix_len;
	u32 value_len;
	char data[0]; /* key + '\0' + suffix + '\r''\n' +  data + '\n' */
} __attribute__((packed));

struct mc_stats
{
	u64 total_items;
	u32 curr_connections;
	u32 total_connections;
	u64 cmd_get;
	u64 cmd_set;
	u64 get_hits;
	u64 get_misses;
	u64 evictions;
	u64 bytes_read;
	u64 bytes_written;
};

extern struct mc_stats g_mc_stats;

struct mc_obj* mc_obj (struct tnt_object* _obj);

int mc_len (const struct mc_obj* _m);

const char* mc_key (const struct mc_obj* _m);

const char* mc_value (const struct mc_obj* _m);

bool expired (struct tnt_object* _obj);

bool missing (struct tnt_object* _obj);

int store (Memcached* _memc, const char* _key, u32 _exptime, u32 _flags, u32 _value_len, const char* _value);

int delete (Memcached* _memc, const char* _keys[], int _n);

void print_stats (struct netmsg_head* _wbuf);

void flush_all (va_list _ap);

int memcached_dispatch (Memcached* _memc, int _fd, struct tbuf* _rbuf, struct netmsg_head* _wbuf);

#endif
