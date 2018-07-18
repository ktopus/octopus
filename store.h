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
#ifndef __OCTOPUS_MEMCACHED_H
#define __OCTOPUS_MEMCACHED_H

#include <util.h>
#include <index.h>

#import <net_io.h>
#import <log_io.h>
#import <fiber.h>

/**
 * @brief Параметры команды
 */
struct mc_params
{
	//
	// Маркер начала ключа или набора ключей
	//
	char* keys;

	bool  noreply;
	u64   value;
	u32   flags;
	u32   exptime;
	u32   bytes;
	i32   delay;
	char* data;
};

/**
 * @brief In Memory хранилище ключ/объект с возможностью восстановления состояния после перезапуска
 */
@interface Memcached : Object<Executor>
	{
		struct Fiber* expire_fiber;

	@public
		Shard<Shard>* shard;
		CStringHash*  mc_index;
	}
@end

/**
 * @brief Вывод сообщения в буфер
 *
 * Используем макроопределение ради вычисления размера выводимой строки на этапе
 * компиляции (работает только для константных строк)
 */
#define ADD_IOV_LITERAL(_noreply, _wbuf, _s) \
	({ \
		if (!(_noreply)) \
			net_add_iov ((_wbuf), (_s), sizeof (_s) - 1); \
	})

/**
 * @brief Преобразование строки в беззнаковое 64-битовое целое
 */
u64 natoq (const char* _start, const char* _end);

/**
 * @brief Конструктор набора параметров команды
 */
void init (struct mc_params* _params);

/**
 * @brief Вывести сообщение об ошибке протокола
 */
void protoError (struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Добавить к статистике количество прочитанных байт
 */
void statsAddRead (u64 _bytes);

/**
 * @brief Реализация команды SET протокола memcached
 */
void set (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Реализация команды ADD протокола memcached
 */
void add (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Реализация команды REPLACE протокола memcached
 */
void replace (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Реализация команды CAS протокола memcached
 */
void cas (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Реализация команд APPEND и PREPEND протокола memcached
 */
void append (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, bool _back);

/**
 * @brief Реализация команд INCR и DECR протокола memcached
 */
void inc (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, int _sign);

/**
 * @brief Реализация команды DELETE протокола memcached
 */
void eraseKey (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Реализация команд GETS и GET протокола memcached
 */
void get (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf, bool _show_cas);

/**
 * @brief Реализация команды FLUSH_ALL протокола memcached
 */
void flushAll (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Реализация команды STATS протокола memcached
 */
void printStats (Memcached* _memc, struct mc_params* _params, struct netmsg_head* _wbuf);

/**
 * @brief Парсер и диспетчер команд
 */
int memcached_dispatch (Memcached* _memc, int _fd, struct tbuf* _rbuf, struct netmsg_head* _wbuf);

#endif /* __OCTOPUS_MEMCACHED_H */
