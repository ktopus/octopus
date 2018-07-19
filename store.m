/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014 Yuriy Vostrikov
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
#include <stat.h>
#include <sysexits.h>
#include <salloc.h>

#include <iproto.h>
#include <util.h>
#include <fiber.h>
#include <log_io.h>
#include <index.h>
#include <say.h>
#include <pickle.h>

#include <octopus.h>

#include "memcached_version.h"
#include "store.h"

enum tag
{
	ADD_OR_REPLACE = user_tag,
	ERASE
};

/**
 * @brief Код объекта, заворачиваемого в структуру tnt_object
 */
enum object_type
{
	MC_OBJ = 1
};

/**
 * @brief Ключ/значение для хранения в memcached с доп. параметрами
 */
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

/**
 * @brief Структура и переменная для хранения статистики
 */
static struct mc_stats
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
} g_mc_stats = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

/**
 * @brief Счётчик объектов
 */
static u64 g_cas = 0;

/**
 * @brief Контекст управления памятью
 */
static struct netmsg_pool_ctx g_ctx;

/**
 * @brief Признак использования Garbage Collector при управлении памятью
 */
#define USE_GC 1

static inline struct mc_obj*
mc_obj (struct tnt_object* _obj)
{
	if (_obj->type != MC_OBJ)
		abort ();

	return (struct mc_obj*)_obj->data;
}

static inline int
mc_len (const struct mc_obj* _m)
{
	return sizeof (*_m) + _m->key_len + _m->suffix_len + _m->value_len;
}

static inline const char*
mc_key (const struct mc_obj* _m)
{
	return _m->data;
}

static inline const char*
mc_suffix (const struct mc_obj* _m)
{
	return _m->data + _m->key_len;
}

static inline const char*
mc_value (const struct mc_obj* _m)
{
	return _m->data + _m->key_len + _m->suffix_len;
}

static inline bool
mc_expired (struct tnt_object* _o)
{
	if (cfg.memcached_no_expire)
		return false;

	struct mc_obj* m = mc_obj (_o);

	return (m->exptime == 0) ? false : m->exptime < ev_now ();
}

static inline bool
mc_missing (struct tnt_object* _o)
{
	return (_o == NULL) || object_ghost (_o) || mc_expired (_o);
}

/**
 * @brief Упаковка параметров во вновь созданную структуру tnt_object{mc_obj}
 *
 * Объекты с таким распределением должны освобождаться только с использованием
 *
 */
static struct tnt_object*
mc_alloc (const char* _key, u32 _exptime, u32 _flags, u32 _vlen, const char* _value, u64 _cas)
{
	char suffix[43] = {0};
	snprintf (suffix, sizeof (suffix) - 1, " %"PRIu32" %"PRIu32"\r\n", _flags, _vlen);

	int klen = strlen (_key) + 1;
	int slen = strlen (suffix);

	struct tnt_object *o = object_alloc (MC_OBJ, USE_GC, sizeof (struct mc_obj) + klen + slen + _vlen);

	struct mc_obj* m = mc_obj (o);
	m->exptime    = _exptime;
	m->flags      = _flags;
	m->cas        = (_cas > 0) ? _cas : ++g_cas;
	m->key_len    = klen;
	m->suffix_len = slen;
	m->value_len  = _vlen;

	memcpy (m->data, _key, klen);
	memcpy (m->data + klen, suffix, slen);
	memcpy (m->data + klen + slen, _value, _vlen);

	return o;
}

/**
 * @brief Проверка, является ли заданная строка беззнаковым целым числом
 */
static bool
is_numeric (const char* _field, u32 _flen)
{
	for (int i = 0; i < _flen; ++i)
	{
		if ((_field[i] < '0') || ('9' < _field[i]))
			return false;
	}

	return true;
}

/**
 * @brief Получить указатель на первый ключ в списке
 *
 * Каждый ключ завершается символом '\0', после возврата указатель @a _k указывает
 * на память за символом '\0' возвращённого ключа. Данная функция модифицирует как
 * параметр @a _k, так и память, на которую он указывает
 */
static char*
next_key (char** _k)
{
	char* r = *_k;
	char* p;
	char* s;

	if (!r)
		return NULL;

	//
	// Ищем первый пробельный символ
	//
	for (p = r; (*p != ' ') && (*p != '\r') && (*p != '\n'); ++p)
		;

	//
	// Начало анализируемой строки
	//
	s = p;

	//
	// Проматываем все пробелы
	//
	while (*p == ' ')
		++p;

	//
	// Если это не конец строки
	//
	if ((*p != '\r') && (*p != '\n'))
		*_k = p;
	else
		*_k = NULL;

	*s = 0;

	return r;
}

/**
 * @brief Удаление данных из кэша без записи в журнал и без блокировок
 *
 * Используется при восстановлении данных из журнала и/или снапшота
 */
static void
onlyErase (Memcached* _memc, const char* _key)
{
	struct tnt_object* o = [_memc->mc_index find:_key];
	if (o)
	{
		[_memc->mc_index remove:o];
		object_decr_ref (o);
	}
}

/**
 * @brief Сохранение данных в кэше без записи в журнал и без блокировок
 *
 * Используется при восстановлении данных из журнала и/или снапшота
 */
static void
onlyAddOrReplace (Memcached* _memc, const char* _key, u32 _exptime, u32 _flags, u32 _vlen, const char* _value, u64 _cas)
{
	struct tnt_object* o = mc_alloc (_key, _exptime, _flags, _vlen, _value, _cas);
	object_incr_ref (o);

	//
	// Модифицируем глобальный счётчик объектов
	//
	struct mc_obj* m = mc_obj (o);
	if (m->cas > g_cas)
		g_cas = m->cas + 1;

	//
	// Удаляем из кэша объект с таким же ключём
	//
	onlyErase (_memc, _key);

	//
	// Добавляем объект в кэш
	//
	[_memc->mc_index replace:o];
}

/**
 * @brief Восстановление данных из журнала и/или снапшота
 */
static void
onlyEraseCompat (Memcached* _memc, struct tbuf* _op)
{
	int klen = read_varint32 (_op);

	char key[klen + 1];
	memcpy (key, _op->ptr, klen);
	key[klen] = 0;

	onlyErase (_memc, key);
}

/**
 * @brief Восстановление данных из журнала и/или снапшота
 */
static void
onlyAddOrReplaceCompat (Memcached* _memc, struct tbuf* _op)
{
	int   klen = read_varint32 (_op);
	char* key  = read_bytes (_op, klen);

	int meta_len = read_varint32 (_op);
	assert (meta_len == 16);

	u32 exptime = read_u32 (_op);
	u32 flags   = read_u32 (_op);
	u64 cas     = read_u64 (_op);

	int slen = read_varint32 (_op);
	read_bytes (_op, slen);

	int   vlen  = read_varint32 (_op);
	char* value = read_bytes (_op, vlen);

	onlyAddOrReplace (_memc, key, exptime, flags, vlen, value, cas);
}

/**
 * @brief Добавить или обновить объект в кэше с записью информации в журнал
 */
static int
addOrReplace (Memcached* _memc, const char* _key, u32 _exptime, u32 _flags, u32 _vlen, const char* _value)
{
	if ([_memc->shard is_replica])
		return 0;

	//
	// Создаём объект для сохранения в кэше
	//
	struct tnt_object* o = mc_alloc (_key, _exptime, _flags, _vlen, _value, 0);
	object_incr_ref (o);

	@try
	{
		//
		// Ищём в кэше объект с тем же ключём и если нашли, то блокируем
		// объект в кэше, чтобы он не мог быть удалён или изменён в
		// параллельных соединениях
		//
		struct tnt_object* oo = NULL;
		if ((oo = [_memc->mc_index find_obj:o]))
			object_lock (oo);

		//
		// Пишем данные о добавляемом объекте в лог
		//
		{
			struct mc_obj* m = mc_obj (o);
			if ([_memc->shard submit:m len:mc_len (m) tag:ADD_OR_REPLACE|TAG_WAL] != 1)
			{
				//
				// Если объект в кэше был заблокирован, то разблокируем его
				//
				if (oo)
					object_unlock (oo);

				//
				// Выбрасываем исключение о невозможности записи в журнал
				//
				// FIXME: код ошибки выбран произвольно, так как подходящего
				//        не объявлено
				//
				iproto_raise (ERR_CODE_MEMORY_ISSUE, "can't write WAL row");
			}
		}

		//
		// Добавляем объект в кэш (блокировать его нет необходимости, так как
		// далее по коду он не используется и другие соединения могут его
		// модифицировать как угодно)
		//
		[_memc->mc_index replace:o];

		//
		// Если в кэше был объект с тем же ключом, то разблокируем его и
		// удаляем (в принципе разблокировка не нужна, так как объект уже
		// в кэше не находится и он в любом случае будет удалён. Но всё
		// равно для чистоты кода разблокируем)
		//
		if (oo)
		{
			object_unlock (oo);
			object_decr_ref (oo);
		}

		//
		// Возвращаем количество добавленных/обновлённых объектов
		//
		return 1;
	}
	@catch (Error* e)
	{
		say_warn ("%s, got exception: %s", __PRETTY_FUNCTION__, e->reason);
		[e release];
	}
	@catch (id e)
	{}

	//
	// В случае ошибки удаляем созданный объект и возвращаем нулевое
	// количество добавленных/изменённых объектов
	//
	object_decr_ref (o);
	return 0;
}

/**
 * @brief Удалить объект из кэша с записью информации в журнал
 */
static int
erase (Memcached* _memc, const char* _keys[], int _n)
{
	//
	// Из реплики ничего не удаляем
	//
	if ([_memc->shard is_replica])
		return 0;

	//
	// Распределяем массив указателей для всех потенциально удаляемых объектов.
	// Массив распределяем в памяти, которая будет удалена после обработки
	// соединения
	//
	struct tnt_object** objs = palloc (fiber->pool, sizeof (struct tnt_object*)*_n);

	//
	// Собираем в массив все объекты для удаления
	//
	int k = 0;
	for (int i = 0; i < _n; ++i)
	{
		//
		// Если объект с заданным ключом найден в кэше
		//
		if ((objs[k] = [_memc->mc_index find:*(_keys++)]))
		{
			@try
			{
				//
				// Блокируем объект, чтобы его не могли изменить в параллельных
				// соединениях. Здесь может быть выброшено исключение, которое
				// обойдёт приращение счётчика объектов для удаления и соответственно
				// данный объект не будет удалён
				//
				object_lock (objs[k]);

				//
				// Если объект успешно заблокирован, то увеличиваем счётчик
				// объектов для удаления
				//
				++k;
			}
			@catch (Error* e)
			{
				say_warn ("%s, got exception: %s", __PRETTY_FUNCTION__, e->reason);
				[e release];
			}
			@catch (id e)
			{}
		}
	}

	//
	// Если есть объекты для удаления
	//
	if (k > 0)
	{
		//
		// Пишем в буфер все объекты для записи информации в журнал. Буфер
		// распределяем в памяти, которая будет удалена после обработки
		// соединения
		//
		struct tbuf* b = tbuf_alloc (fiber->pool);
		for (int i = 0; i < k; ++i)
		{
			struct mc_obj* m = mc_obj (objs[i]);

			tbuf_append (b, m->data, m->key_len);
		}

		//
		// Если данные об удаляемых объектов удалось записать в журнал
		//
		if ([_memc->shard submit:b->ptr len:tbuf_len (b) tag:ERASE|TAG_WAL] == 1)
		{
			for (int i = 0; i < k; ++i)
			{
				//
				// Удаляем объекты из кэша
				//
				[_memc->mc_index remove:objs[i]];

				//
				// ... и из памяти
				//
				object_unlock (objs[i]);
				object_decr_ref (objs[i]);
			}

			//
			// Число реально удалённых объектов
			//
			return k;
		}
	}

	return 0;
}

/**
 * @brief Добавить или обновить объект в кэше с проверкой размера данных и выводом сообщения
 */
static void
addOrReplaceKey (Memcached* _memc, const char* _key, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	++g_mc_stats.cmd_set;

	if (_params->bytes > (1 << 20))
	{
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "SERVER_ERROR object too large for cache\r\n");
	}
	else
	{
		if (addOrReplace (_memc, _key, _params->exptime, _params->flags, _params->bytes, _params->data) > 0)
		{
			g_mc_stats.total_items++;

			ADD_IOV_LITERAL (_params->noreply, _wbuf, "STORED\r\n");
		}
		else
		{
			ADD_IOV_LITERAL (_params->noreply, _wbuf, "SERVER_ERROR\r\n");
		}
	}
}

static void
flush_all (va_list _ap)
{
	Memcached* memc = va_arg (_ap, Memcached*);

	i32 delay = va_arg (_ap, u32);
	if (delay > ev_now ())
		fiber_sleep (delay - ev_now ());

	u32 slots = [memc->mc_index slots];
	for (u32 i = 0; i < slots; ++i)
	{
		struct tnt_object* obj = [memc->mc_index get:i];
		if (obj != NULL)
			mc_obj (obj)->exptime = 1;
	}
}

static void
memcached_expire (va_list _ap __attribute__((unused)))
{
	if (cfg.memcached_no_expire)
		return;

	Shard<Shard>* shard = [recovery shard:0];
	Memcached*    memc  = [shard executor];

	char** keys = malloc (cfg.memcached_expire_per_loop*sizeof(void*));

	u32 i = 0;

	say_info ("%s, memcached expire fiber started", __PRETTY_FUNCTION__);

	for (;;)
	{
		double delay = (double)cfg.memcached_expire_per_loop*cfg.memcached_expire_full_sweep/([memc->mc_index slots] + 1);
		if (delay > 1.0)
			delay = 1.0;
		fiber_sleep (delay);

		say_debug ("%s, expire loop", __PRETTY_FUNCTION__);
		if ([shard is_replica])
			continue;

		if (i >= [memc->mc_index slots])
			i = 0;

		int k = 0;
		for (int j = 0; j < cfg.memcached_expire_per_loop; j++, i++)
		{
			struct tnt_object* o = [memc->mc_index get:i];
			if ((o == NULL) || object_ghost (o))
				continue;

			if (!mc_expired (o))
				continue;

			struct mc_obj* m = mc_obj (o);

			keys[k] = palloc (fiber->pool, m->key_len);
			strcpy (keys[k], m->data);
			++k;
		}

		erase (memc, (const char**)keys, k);
		say_debug ("%s, expired %i keys", __PRETTY_FUNCTION__, k);

		fiber_gc ();
	}

	free (keys);
}

static void
mc_print_row (struct tbuf* _out, u16 _tag, struct tbuf* _op)
{
	switch(_tag & TAG_MASK)
	{
		case ADD_OR_REPLACE:
		{
			struct mc_obj* m = _op->ptr;
			const char*    k = m->data;
			tbuf_printf (_out, "ADD_OR_REPLACE %.*s %.*s", m->key_len, k, m->value_len, mc_value (m));
			break;
		}

		case ERASE:
			tbuf_printf (_out, "ERASE");
			while (tbuf_len (_op) > 0)
			{
				const char* k = _op->ptr;
				tbuf_printf (_out, " %s", k);
				tbuf_ltrim (_op, strlen (k) + 1);
			}
			break;

		case snap_final:
			break;

		default:
			tbuf_printf (_out, "++UNKNOWN++");
			return;
	}
}

static void
memcached_handler (va_list _ap)
{
	++g_mc_stats.total_connections;
	++g_mc_stats.curr_connections;

	int        fd   = va_arg (_ap, int);
	Memcached* memc = va_arg (_ap, Memcached*);

	struct netmsg_head wbuf;
	netmsg_head_init (&wbuf, &g_ctx);

	struct tbuf rbuf = TBUF (NULL, 0, fiber->pool);
	palloc_register_gc_root (fiber->pool, &rbuf, tbuf_gc);

	@try
	{
		for (;;)
		{
			int p;
			int r;
			int batch_count = 0;

			if (fiber_recv (fd, &rbuf) <= 0)
				return;

		dispatch:
			p = memcached_dispatch (memc, fd, &rbuf, &wbuf);
			if (p < 0)
			{
				say_debug ("%s, negative dispatch, closing connection", __PRETTY_FUNCTION__);
				return;
			}

			if ((p == 0) && (batch_count == 0)) /* we havn't successfully parsed any requests */
				continue;

			if (p == 1)
			{
				++batch_count;

				/* some unparsed commands remain and batch count less than 20 */
				if ((tbuf_len (&rbuf) > 0) && (batch_count < 20))
					goto dispatch;
			}

			g_mc_stats.bytes_written += wbuf.bytes;
			r = fiber_writev (fd, &wbuf);
			if (r < 0)
			{
				say_debug ("%s, flush_output failed, closing connection", __PRETTY_FUNCTION__);
				return;
			}

			fiber_gc ();

			if ((p == 1) && (tbuf_len (&rbuf) > 0))
			{
				batch_count = 0;
				goto dispatch;
			}
		}
	}
	@catch (Error* e)
	{
		say_debug ("%s, got error %s", __PRETTY_FUNCTION__, e->reason);
		[e release];
	}
	@finally
	{
		palloc_unregister_gc_root (fiber->pool, &rbuf);
		close (fd);

		--g_mc_stats.curr_connections;
	}
}

static void
memcached_accept (int _fd, void* _data)
{
	fiber_create ("memcached/handler", memcached_handler, _fd, _data);
}

static void
init_second_stage (va_list _ap __attribute__((unused)))
{
	assert (recovery != NULL);
	[recovery simple:NULL];

	Memcached* memc = [[recovery shard:0] executor];
	assert (memc != NULL);

	netmsg_pool_ctx_init (&g_ctx, "stats_pool", 1024*1024);

	if (fiber_create ("memcached/acceptor", tcp_server, cfg.primary_addr, memcached_accept, NULL, memc) == NULL)
	{
		say_error ("%s, can't start tcp_server on `%s'", __PRETTY_FUNCTION__, cfg.primary_addr);
		exit (EX_OSERR);
	}

	say_info ("%s, memcached initialized", __PRETTY_FUNCTION__);
}

static void
memcached_init ()
{
	struct feeder_param feeder;
	enum feeder_cfg_e fid_err = feeder_param_fill_from_cfg (&feeder, NULL);
	if (fid_err)
		panic ("wrong feeder conf");

	recovery = [[Recovery alloc] init];
	recovery->default_exec_class = [Memcached class];

	[recovery shard_create_dummy:NULL];
	if (init_storage)
		return;

	//
	// fiber is required to successfully pull from remote
	//
	fiber_create ("memcached_init", init_second_stage);
}

static int
memcached_cat (const char* _filename)
{
	read_log (_filename, mc_print_row);

	return 0; /* ignore return status of read_log */
}

static struct index_node*
dtor (struct tnt_object* _o, struct index_node* _n, void* _arg __attribute__((unused)))
{
	_n->obj     = _o;
	_n->key.ptr = mc_obj (_o)->data;

	return _n;
}

@implementation Memcached
- (id)
init
{
	[super init];
	mc_index = [[CStringHash alloc] init:NULL dtor: NULL];
	mc_index->dtor = dtor;

	return self;
}

- (void)
set_shard:(Shard<Shard>*)_shard
{
	shard = _shard;
}

- (void)
apply:(struct tbuf*)_op tag:(u16)_tag
{
	/*
	 * row format is dead simple:
	 *     ADD_OR_REPLACE -> op is mc_obj itself.
	 *     ERASE          -> op is key array
	 */
	switch (_tag & TAG_MASK)
	{
		case ADD_OR_REPLACE:
		{
			struct mc_obj* m = (struct mc_obj*)_op->ptr;
			say_debug ("%s, ADD_OR_REPLACE %s", __PRETTY_FUNCTION__, mc_key (m));

			onlyAddOrReplace (self, mc_key (m), m->exptime, m->flags, m->value_len, mc_value (m), m->cas);
			break;
		}

		case ERASE:
			while (tbuf_len (_op) > 0)
			{
				const char* key = (const char*)_op->ptr;
				say_debug ("%s, ERASE %s", __PRETTY_FUNCTION__, key);

				onlyErase (self, key);
				tbuf_ltrim (_op, strlen (key) + 1);
			}
			break;

		//
		// compat with box emulation
		//
		case wal_data:
		{
			u16 code = read_u16 (_op);
			read_u32 (_op); /* obj_space */

			switch(code)
			{
				case 13:
				{
					say_debug ("%s, ADD_OR_REPLACE(compat)", __PRETTY_FUNCTION__);

					read_u32 (_op); /* flags */
					read_u32 (_op); /* cardinality */

					onlyAddOrReplaceCompat (self, _op);
					break;
				}

				case 20:
				{
					say_debug ("%s, ERASE(compat)", __PRETTY_FUNCTION__);

					read_u32 (_op); /* key cardinality */

					onlyEraseCompat (self, _op);
					break;
				}

				default:
					abort ();
					break;
			}

			assert (tbuf_len (_op) == 0);
			break;
		}

		case snap_data:
			say_debug ("%s, SNAP(compat)", __PRETTY_FUNCTION__);

			read_u32 (_op); /* obj_space */
			read_u32 (_op); /* cardinality */
			read_u32 (_op); /* data_size */

			onlyAddOrReplaceCompat (self, _op);
			break;

		default:
			break;
	}
}

- (void)
wal_final_row
{}

- (void)
status_changed
{
	if (cfg.memcached_no_expire)
		return;

	if ([shard is_replica] && expire_fiber != NULL)
		panic ("can't downgrade from primary");

	if (![shard is_replica])
	{
		if (expire_fiber == NULL)
			expire_fiber = fiber_create ("memecached_expire", memcached_expire);
	}
}

- (int)
snapshot_write_rows: (XLog*)_log
{
	u32                i = 0;
	struct tnt_object* o = NULL;

	title ("dumper of pid %" PRIu32 ": dumping actions", getppid ());

	[mc_index iterator_init];
	while ((o = [mc_index iterator_next]))
	{
		struct mc_obj* m = mc_obj (o);
		if ([_log append_row:m len:mc_len (m) shard:nil tag:ADD_OR_REPLACE|TAG_SNAP] == NULL)
			return -1;

		if ((++i)%100000 == 0)
		{
			say_info ("%s, %.1fM rows written", __PRETTY_FUNCTION__, i/1000000.0);
			title ("dumper of pid %" PRIu32 ": dumping actions (%.1fM  rows )", getppid (), i / 1000000.);
		}

		if (i%10000 == 0)
			[_log confirm_write];
	}

	say_info ("%s, snapshot finished", __PRETTY_FUNCTION__);
	return 0;
}

- (u32)
snapshot_estimate
{
	return [mc_index size];
}

- (void)
print:(const struct row_v12*)_row into:(struct tbuf*)_buf
{
	print_row (_buf, _row, mc_print_row);
}
@end

u64
natoq (const char* _start, const char* _end)
{
	u64 num = 0;
	while (_start < _end)
		num = num*10 + (*_start++ - '0');
	return num;
}

void
init (struct mc_params* _params)
{
	assert (_params != NULL);

	_params->keys     = NULL;
	_params->noreply  = false;
	_params->value    = 0;
	_params->flags    = 0;
	_params->exptime  = 0;
	_params->bytes    = 0;
	_params->delay    = 0;
	_params->data     = NULL;
}

void
protoError (struct mc_params* _params, struct netmsg_head* _wbuf)
{
	say_warn ("%s, memcached proto error", __PRETTY_FUNCTION__);
	ADD_IOV_LITERAL (_params->noreply, _wbuf, "ERROR\r\n");
	g_mc_stats.bytes_written += 7;
}

void
statsAddRead (u64 _bytes)
{
	g_mc_stats.bytes_read += _bytes;
}

void
set (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	addOrReplaceKey (_memc, key, _params, _wbuf);
}

void
add (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (!mc_missing (o))
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_STORED\r\n");
	else
		addOrReplaceKey (_memc, key, _params, _wbuf);
}

void
replace (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (mc_missing (o))
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_STORED\r\n");
	else
		addOrReplaceKey (_memc, key, _params, _wbuf);
}

void
cas (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (mc_missing (o))
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_FOUND\r\n");
	else if (mc_obj (o)->cas != _params->value)
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "EXISTS\r\n");
	else
		addOrReplaceKey (_memc, key, _params, _wbuf);
}

void
append (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, bool _back)
{
	char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (mc_missing (o))
	{
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_STORED\r\n");
	}
	else
	{
		struct mc_obj* m = mc_obj (o);
		struct tbuf*   b = tbuf_alloc (fiber->pool);

		if (_back)
		{
			tbuf_append (b, mc_value (m), m->value_len);
			tbuf_append (b, _params->data, _params->bytes);
		}
		else
		{
			tbuf_append (b, _params->data, _params->bytes);
			tbuf_append (b, mc_value (m), m->value_len);
		}

		_params->bytes += m->value_len;
		_params->data   = (char*)b->ptr;

		addOrReplaceKey (_memc, key, _params, _wbuf);
	}
}

void
inc (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, int _sign)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (mc_missing (o))
	{
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_FOUND\r\n");
	}
	else
	{
		struct mc_obj* m = mc_obj (o);

		if (is_numeric (mc_value (m), m->value_len))
		{
			++g_mc_stats.cmd_set;

			u64 value = natoq (mc_value (m), mc_value (m) + m->value_len);

			if (_sign > 0)
			{
				value += _params->value;
			}
			else
			{
				if (_params->value > value)
					value = 0;
				else
					value -= _params->value;
			}

			struct tbuf* b = tbuf_alloc (fiber->pool);
			tbuf_printf (b, "%"PRIu64, value);

			if (addOrReplace (_memc, key, m->exptime, m->flags, tbuf_len(b), b->ptr))
			{
				++g_mc_stats.total_items;

				if (!_params->noreply)
				{
					net_add_iov (_wbuf, b->ptr, tbuf_len (b));
					ADD_IOV_LITERAL (_params->noreply, _wbuf, "\r\n");
				}
			}
			else
			{
				ADD_IOV_LITERAL (_params->noreply, _wbuf, "SERVER_ERROR\r\n");
			}
		}
		else
		{
			ADD_IOV_LITERAL (_params->noreply, _wbuf, "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n");
		}
	}
}

void
eraseKey (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (mc_missing(o))
	{
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_FOUND\r\n");
	}
	else
	{
		if (erase (_memc, &key, 1) > 0)
			ADD_IOV_LITERAL (_params->noreply, _wbuf, "DELETED\r\n");
		else
			ADD_IOV_LITERAL (_params->noreply, _wbuf, "SERVER_ERROR\r\n");
	}
}

void
get (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, bool _show_cas)
{
	++g_mc_stats.cmd_get;

	const char* key;
	while ((key = next_key (&_params->keys)))
	{
		struct tnt_object* o = [_memc->mc_index find:key];

		if (mc_missing (o))
		{
			++g_mc_stats.get_misses;
			continue;
		}

		++g_mc_stats.get_hits;

		struct mc_obj* m = mc_obj (o);
		const char* suffix = mc_suffix (m);
		const char* value  = mc_value (m);

		if (_show_cas)
		{
			struct tbuf* b = tbuf_alloc (fiber->pool);
			tbuf_printf (b, "VALUE %s %"PRIu32" %"PRIu32" %"PRIu64"\r\n", key, m->flags, m->value_len, m->cas);
			net_add_iov (_wbuf, b->ptr, tbuf_len (b));
			g_mc_stats.bytes_written += tbuf_len (b);
		}
		else
		{
			ADD_IOV_LITERAL (_params->noreply, _wbuf, "VALUE ");
			net_add_iov (_wbuf, key, m->key_len - 1);
			net_add_iov (_wbuf, suffix, m->suffix_len);
		}

		net_add_obj_iov (_wbuf, o, value, m->value_len);
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "\r\n");

		g_mc_stats.bytes_written += m->value_len + 2;
	}

	ADD_IOV_LITERAL (_params->noreply, _wbuf, "END\r\n");

	g_mc_stats.bytes_written += 5;
}

void
flushAll (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	fiber_create ("flush_all", flush_all, _memc, _params->delay);
	ADD_IOV_LITERAL (_params->noreply, _wbuf, "OK\r\n");
}

void
printStats (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	(void)_memc;
	(void)_params;

	u64 bytes_used, items;
	struct tbuf* out = tbuf_alloc (_wbuf->ctx->pool);

	slab_total_stat (&bytes_used, &items);

	Memcached* memc = [[recovery shard:0] executor];
	assert (memc != NULL);

	tbuf_printf (out, "STAT pid %"PRIu32"\r\n", (u32)getpid ());
	tbuf_printf (out, "STAT uptime %"PRIu32"\r\n", (u32)tnt_uptime ());
	tbuf_printf (out, "STAT time %"PRIu32"\r\n", (u32)ev_now ());
	tbuf_printf (out, "STAT version 1.2.5 (octopus/(silver)box)\r\n");
	tbuf_printf (out, "STAT pointer_size %zu\r\n", sizeof(void*)*8);
	tbuf_printf (out, "STAT curr_items %"PRIu64"\r\n", items);
	tbuf_printf (out, "STAT total_items %"PRIu64"\r\n", g_mc_stats.total_items);
	tbuf_printf (out, "STAT bytes %"PRIu64"\r\n", bytes_used);
	tbuf_printf (out, "STAT curr_connections %"PRIu32"\r\n", g_mc_stats.curr_connections);
	tbuf_printf (out, "STAT total_connections %"PRIu32"\r\n", g_mc_stats.total_connections);
	tbuf_printf (out, "STAT connection_structures %"PRIu32"\r\n", g_mc_stats.curr_connections); /* lie a bit */
	tbuf_printf (out, "STAT cmd_get %"PRIu64"\r\n", g_mc_stats.cmd_get);
	tbuf_printf (out, "STAT cmd_set %"PRIu64"\r\n", g_mc_stats.cmd_set);
	tbuf_printf (out, "STAT get_hits %"PRIu64"\r\n", g_mc_stats.get_hits);
	tbuf_printf (out, "STAT get_misses %"PRIu64"\r\n", g_mc_stats.get_misses);
	tbuf_printf (out, "STAT evictions %"PRIu64"\r\n", g_mc_stats.evictions);
	tbuf_printf (out, "STAT bytes_read %"PRIu64"\r\n", g_mc_stats.bytes_read);
	tbuf_printf (out, "STAT bytes_written %"PRIu64"\r\n", g_mc_stats.bytes_written);
	tbuf_printf (out, "STAT limit_maxbytes %"PRIu64"\r\n", (u64)(cfg.slab_alloc_arena * (1 << 30)));
	tbuf_printf (out, "STAT memcached_size %"PRIu32"\r\n", [memc->mc_index size]);
	tbuf_printf (out, "STAT threads 1\r\n");
	tbuf_printf (out, "END\r\n");

	net_add_iov (_wbuf, out->ptr, tbuf_len (out));
	netmsg_pool_ctx_gc (_wbuf->ctx);
}

static struct tnt_module memcached =
{
	.name    = "memcached",
	.version = memcached_version_string,
	.init    = memcached_init,
	.cat     = memcached_cat
};

register_module (memcached);
register_source ();
