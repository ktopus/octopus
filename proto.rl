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
#include <util.h>
#include <index.h>
#include <tbuf.h>
#include <net_io.h>
#include <say.h>
#include <fiber.h>

#include <string.h>

#include "store.h"

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
	// Начало анализируемого буфера (используется конечным автоматом Ragel)
	//
	char* p = (char*)_rbuf->ptr;

	//
	// Конец анализируемого буфера (используется конечным автоматом Ragel)
	//
	char* pe = (char*)_rbuf->end;

	//
	// Признак завершения обработки команды
	//
	bool done = false;

	//
	// Вспомогательный маркер начала значения
	//
	const char* mark;

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
				protoError (&params, _wbuf);
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
			statsAddRead (p - (char*)_rbuf->ptr);
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
		del       = "delete"i spc key (spc exptime)? noreply spc? eol @done @{ eraseKey (_memc, &params, _wbuf); };
		incr      = "incr"i spc key spc value noreply spc? eol @done @{ inc (_memc, &params, _wbuf,  1); };
		decr      = "decr"i spc key spc value noreply spc? eol @done @{ inc (_memc, &params, _wbuf, -1); };
		stats     = "stats"i eol @done @{ printStats (_memc, &params, _wbuf); };
		flush_all = "flush_all"i (spc delay)? noreply spc? eol @done @{ flushAll (_memc, &params, _wbuf); };
		quit      = "quit"i eol @done @{ return 0; };

		main := set       |
				add       |
				replace   |
				append    |
				prepend   |
				cas       |
				gets      |
				get       |
				del       |
				incr      |
				decr      |
				stats     |
				flush_all |
				quit;
	}%%

	%%write init;
	%%write exec;

	if (!done)
	{
		say_debug ("%s, parse failed at: `%s'", __PRETTY_FUNCTION__, quote (p, (int)(pe - p)));
		if ((pe - p) > (1 << 20))
		{
			protoError (&params, _wbuf);
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
