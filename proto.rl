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
#import <util.h>
#import <fiber.h>
#import <net_io.h>
#import <say.h>
#import <tbuf.h>
#import <index.h>

#include <string.h>

#import "store.h"

/**
 * @brief Преобразование строки в беззнаковое 64-битовое целое
 */
static u64
natoq (const char* _start, const char* _end)
{
	u64 num = 0;
	while (_start < _end)
		num = num*10 + (*_start++ - '0');
	return num;
}

/**
 * @brief Проверка, является ли заданная строка беззнаковым целым числом
 */
static bool
is_numeric (const char* _field, u32 _vlen)
{
	for (int i = 0; i < _vlen; ++i)
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
		p++;

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
 * @brief Квотирование символов '\r', '\n' и непечатных символов
 *
 * Результат работы функции сохраняется во внутреннем буфере и возвращается
 * указатель на него
 */
static const char *
quote (const char* _p, int _len)
{
	static char buf[40*2 + 3 + 1]; /* 40*2 + '...' + \0 */

	char* b = buf;

	for (int i = 0; i < MIN (_len, 40); ++i)
	{
		if ((' ' <= _p[i]) && (_p[i] <= 'z'))
		{
			*b++ = _p[i];
		}

		else if (_p[i] == '\r')
		{
			*b++ = '\\';
			*b++ = 'r';
		}

		else if (_p[i] == '\n')
		{
			*b++ = '\\';
			*b++ = 'n';
		}
		else
		{
			*b++ = '?';
		}
	}

	if (_len > 40)
	{
		*b++ = '.';
		*b++ = '.';
		*b++ = '.';
	}

	*b = '\0';

	return buf;
}

/**
 * @brief Вывод сообщения в буфер
 *
 * Используем макроопределение ради вычисления размера выводимой строки на этапе
 * компиляции
 */
#define ADD_IOV_LITERAL(_noreply, _wbuf, _s) \
	({ \
		if (!(_noreply)) \
			net_add_iov ((_wbuf), (_s), sizeof (_s) - 1); \
	})


static void
set4key (Memcached* _memc, const char* _key, struct mc_params* _params, struct netmsg_head* _wbuf)
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
set (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	set4key (_memc, key, _params, _wbuf);
}

static void
add (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (!missing (o))
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_STORED\r\n");
	else
		set4key (_memc, key, _params, _wbuf);
}

static void
replace (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (missing (o))
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_STORED\r\n");
	else
		set4key (_memc, key, _params, _wbuf);
}

static void
cas (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (missing (o))
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_FOUND\r\n");
	else if (mc_obj (o)->cas != _params->value)
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "EXISTS\r\n");
	else
		set4key (_memc, key, _params, _wbuf);
}

static void
append (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, bool _back)
{
	char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (missing (o))
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

		set4key (_memc, key, _params, _wbuf);
	}
}

static void
inc (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, int _sign)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (missing (o))
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

static void
deleteKey (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	const char* key = next_key (&_params->keys);

	struct tnt_object* o = [_memc->mc_index find:key];
	if (missing(o))
	{
		ADD_IOV_LITERAL (_params->noreply, _wbuf, "NOT_FOUND\r\n");
	}
	else
	{
		if (delete (_memc, &key, 1) > 0)
			ADD_IOV_LITERAL (_params->noreply, _wbuf, "DELETED\r\n");
		else
			ADD_IOV_LITERAL (_params->noreply, _wbuf, "SERVER_ERROR\r\n");
	}
}

static void
get (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, bool _show_cas)
{
	++g_mc_stats.cmd_get;

	const char* key;
	while ((key = next_key (&_params->keys)))
	{
		struct tnt_object* o = [_memc->mc_index find:key];

		if (missing (o))
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

static void
flushAll (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf)
{
	fiber_create ("flush_all", flush_all, _memc, _params->delay);
	ADD_IOV_LITERAL (_params->noreply, _wbuf, "OK\r\n");
}

%%machine memcached;
%%write data;

int
memcached_dispatch (Memcached* _memc, int _fd, struct tbuf* _rbuf, struct netmsg_head* _wbuf)
{
	//
	// Переменная состояния для конечного автомата, сгенерированного Ragel
	//
	int cs;

	//
	// Начало анализируемого буфера
	//
	char* p = (char*)_rbuf->ptr;

	//
	// Конец анализируемого буфера
	//
	char* pe = (char*)_rbuf->end;

	//
	// Признак завершения обработки команды
	//
	bool done = false;

	//
	// Маркер начала значения
	//
	char* mark;

	//
	// Параметры выполняемой команды
	//
	struct mc_params params;
	init (&params);

	say_debug ("%s, %s'", __PRETTY_FUNCTION__, quote (p, (int)(pe - p)));

	%%{
		action kmark
		{
			//
			// Маркировка начала ключа
			//
			params.keys = p;

			//
			// Проматываем указатель так, чтобы он указывал на последний символ
			// перед символом, который не входит в ключ. Это необходимо, чтобы
			// корректно сработал анализатор при дальнейшем разборе входного
			// потока
			//
			for (; ((p + 1) < pe) && (p[1] != ' ') && (p[1] != '\r') && (p[1] != '\n'); ++p)
				;

			//
			// Если вышли за границы буфера, то возвращаем указатель в начальное положение
			//
			if ((p + 1) == pe)
				p = params.keys;
		}

		action ksmark
		{
			//
			// Маркировка начала списка ключей, разделённых пробелами
			//
			params.keys = p;

			//
			// Проматываем указатель так, чтобы он указывал на последний символ
			// перед символом, который не входит в список ключей (пробел входит).
			// Это необходимо, чтобы корректно сработал анализатор при дальнейшем
			// разборе входного потока
			//
			for (; ((p + 1) < pe) && (p[1] != '\r') && (p[1] != '\n'); ++p)
				;

			//
			// Если вышли за границы буфера, то возвращаем указатель в начальное положение
			//
			if ((p + 1) == pe)
				p = params.keys;
		}

		action exptime
		{
			params.exptime = natoq (mark, p);
			if ((params.exptime > 0) && (params.exptime <= 60*60*24*30))
				params.exptime = params.exptime + ev_now ();
		}

		action read
		{
			//
			// Размер уже разобранных данных (нужно для правильного восстановления
			// указателей парсера после догрузки данных)
			//
			size_t parsed = p - (char*)_rbuf->ptr;

			//
			// Пока не все указанные в команде данные прочитаны (+ \r\n)
			//
			while ((tbuf_len (_rbuf) - parsed) < (params.bytes + 2))
			{
				//
				// Загружаем данные в буфер (здесь память буфера может быть при необходимости
				// переаллоцирована)
				//
				int r = fiber_recv (_fd, _rbuf);
				if (r <= 0)
				{
					say_debug ("%s, read returned %i, closing connection", __PRETTY_FUNCTION__, r);
					return -1;
				}
			}

			//
			// Из-за того, что буфер может переаллоцироваться при получении новых
			// данных необходимо восстановить правильные указатели
			//
			p  = _rbuf->ptr + parsed;
			pe = _rbuf->end;

			//
			// Если данные не завершаются \r\n, то завершаем обработку команды с ошибкой
			//
			if (strncmp ((char *)(p + params.bytes), "\r\n", 2) != 0)
			{
				say_warn ("%s, memcached proto error", __PRETTY_FUNCTION__);
				ADD_IOV_LITERAL (params.noreply, _wbuf, "ERROR\r\n");
				g_mc_stats.bytes_written += 7;
				return -1;
			}

			//
			// Данные команды
			//
			params.data = p;

			//
			// Перематываем указатель за пределы прочитанных данных
			//
			p += params.bytes + 2;
		}

		action done
		{
			g_mc_stats.bytes_read += p - (char*)_rbuf->ptr;
			tbuf_ltrim (_rbuf, p - (char*)_rbuf->ptr);

			done = true;
		}

		printable = [^ \t\r\n];
		key       = printable >kmark;
		keys      = printable >ksmark;
		exptime   = digit+ >{ mark = p; } %exptime;
		flags     = digit+ >{ mark = p; } %{ params.flags = natoq (mark, p); };
		bytes     = digit+ >{ mark = p; } %{ params.bytes = natoq (mark, p); };
		value     = digit+ >{ mark = p; } %{ params.value = natoq (mark, p); };
		delay     = digit+ >{ mark = p; } %{ params.delay = natoq (mark, p); };
		eol       = "\r\n" @{ ++p; };
		spc       = " "+;
		noreply   = (spc "noreply"i %{ params.noreply = true; })?;
		store     = spc key spc flags spc exptime spc bytes noreply eol;

		set       = "set"i store @read @done @{ set (_memc, &params, _wbuf); };
		add       = "add"i store @read @done @{ add (_memc, &params, _wbuf); };
		replace   = "replace"i store @read @done @{ replace (_memc, &params, _wbuf); };
		append    = "append"i store @read @done @{ append (_memc, &params, _wbuf, true); };
		prepend   = "prepend"i store @read @done @{ append (_memc, &params, _wbuf, false); };
		cas       = "cas"i spc key spc flags spc exptime spc bytes spc value noreply spc? eol @read @done @{ cas (_memc, &params, _wbuf); };
		gets      = "gets"i spc keys spc? eol @done @{ get (_memc, &params, _wbuf, true); };
		get       = "get"i spc keys spc? eol @done @{ get (_memc, &params, _wbuf, false); };
		delete    = "delete"i spc key (spc exptime)? noreply spc? eol @done @{ deleteKey (_memc, &params, _wbuf); };
		incr      = "incr"i spc key spc value noreply spc? eol @done @{ inc (_memc, &params, _wbuf,  1); };
		decr      = "decr"i spc key spc value noreply spc? eol @done @{ inc (_memc, &params, _wbuf, -1); };
		stats     = "stats"i eol @done @{ print_stats (_wbuf); };
		flush_all = "flush_all"i (spc delay)? noreply spc? eol @done @{ flushAll (_memc, &params, _wbuf); };
		quit      = "quit"i eol @done @{ return 0; };

		main := set |
				cas |
				add |
				replace |
				append |
				prepend |
				get |
				gets |
				delete |
				incr |
				decr |
				stats |
				flush_all |
				quit;
	}%%

	%%write init;
	%%write exec;

	if (!done)
	{
		say_debug ("%s, parse failed at: `%s'", __PRETTY_FUNCTION__, quote(p, (int)(pe - p)));
		if ((pe - p) > (1 << 20))
		{
			say_warn ("%s, memcached proto error", __PRETTY_FUNCTION__);
			ADD_IOV_LITERAL (params.noreply, _wbuf, "ERROR\r\n");
			g_mc_stats.bytes_written += 7;
			return -1;
		}

		char* r;
		if ((r = (char*)memmem (p, pe - p, "\r\n", 2)) != NULL)
		{
			tbuf_ltrim (_rbuf, r + 2 - (char*)_rbuf->ptr);

			while ((tbuf_len (_rbuf) >= 2) && (memcmp (_rbuf->ptr, "\r\n", 2) == 0))
				tbuf_ltrim (_rbuf, 2);

			ADD_IOV_LITERAL (params.noreply, _wbuf, "CLIENT_ERROR bad command line format\r\n");
			return 1;
		}

		return 0;
	}

	return 1;
}

register_source ();
