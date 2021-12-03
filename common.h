/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016, 2017 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016, 2017 Yuriy Vostrikov
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
#ifndef __BOX_COMMON_H
#define __BOX_COMMON_H

#import <util.h>
#import <index.h>

/**
 * @brief Максимальное число индексов для таблицы
 *
 * При изменении данного числа необходимо изменить соответствующее
 * определение в box.lua
 */
#define MAX_IDX 10

/**
 * @brief Максимально возможное число таблиц
 */
#define OBJECT_SPACE_MAX 1024

/**
 * @brief Тэги операций
 */
enum tlv_tag
{
	/**
	 * @brief Одинарная операция
	 */
	BOX_OP = 127,

	/**
	 * @brief Последовательность операций, выполняемая в рамках одной транзакции
	 */
	BOX_MULTI_OP
};

//
// Устаревшие идентификаторы команд:
//     _(INSERT, 1)
//     _(DELETE, 2)
//     _(SET_FIELD, 3)
//     _(ARITH, 5)
//     _(SET_FIELD, 6)
//     _(ARITH, 7)
//     _(SELECT, 4)
//     _(DELETE, 8)
//     _(UPDATE_FIELDS, 9)
//     _(INSERT,10)
//     _(SELECT_LIMIT, 12)
//     _(SELECT_OLD, 14)
//     _(UPDATE_FIELDS_OLD, 16)
//     _(JUBOX_ALIVE, 11)
//
// НЕ ИСПОЛЬЗУЙТЕ эти идентификаторы!
//

/**
 * @brief Команды, поддерживаемые базой данных
 */
#define MESSAGES(_)             \
	_(NOP, 1)                   \
	_(INSERT, 13)               \
	_(SELECT_LIMIT, 15)         \
	_(SELECT, 17)               \
	_(UPDATE_FIELDS, 19)        \
	_(DELETE_1_3, 20)           \
	_(DELETE, 21)               \
	_(EXEC_LUA, 22)             \
	_(PAXOS_LEADER, 90)         \
	_(SELECT_KEYS, 99)          \
	_(SELECT_TUPLES, 100)       \
	_(SUBMIT_ERROR, 101)        \
	_(SELECT_TIME, 102)         \
	_(CREATE_OBJECT_SPACE, 240) \
	_(CREATE_INDEX, 241)        \
	_(DROP_OBJECT_SPACE, 242)   \
	_(DROP_INDEX, 243)          \
	_(TRUNCATE, 244)

enum messages ENUM_INITIALIZER(MESSAGES);

enum BOX_SPACE_STAT
{
	BSS_INSERT,
	BSS_UPDATE,
	BSS_DELETE,
	BSS_SELECT_IDX0,
	BSS_SELECT_TIME_IDX0 = BSS_SELECT_IDX0 + MAX_IDX,
	BSS_SELECT_KEYS_IDX0 = BSS_SELECT_TIME_IDX0 + MAX_IDX,
	BSS_SELECT_TUPLES_IDX0 = BSS_SELECT_KEYS_IDX0 + MAX_IDX,
	BSS_MAX = BSS_SELECT_TUPLES_IDX0 + MAX_IDX
};

/**
 * @brief Инициализация пустого буфера в заданной области памяти
 *
 * Буфер должен быть массивом
 */
#define TBUF_BUF(_buf) (struct tbuf){.ptr = (_buf), .end = (_buf), .free = sizeof (_buf), .pool = NULL}

/**
 * @brief Инициализация буфера с данными заданного размера в заданной области памяти
 *
 * Буфер должен быть массивом
 */
#define TBUF_BUFL(_buf, _l) (struct tbuf){.ptr = (_buf), .end = (_buf) + (_l), .free = sizeof (_buf) - (_l), .pool = NULL}

/**
 * @brief Таблица объектов
 */
struct object_space
{
	/**
	 * @brief Целочисленный идентификатор таблицы (является её именем)
	 */
	int n;

	/**
	 * @brief Таблица игнорируется при загрузке данных из снапшота
	 */
	bool ignored;

	/**
	 * @brief Сохранять ли данные таблицы в снапшоте
	 */
	bool snap;

	/**
	 * @brief FIXME: ???
	 */
	bool wal;

	/**
	 * @brief Количество полей в записях таблицы
	 */
	int cardinality;

	/**
	 * @brief FIXME: ???
	 */
	int statbase;

	/**
	 * @brief FIXME: ???
	 */
	size_t obj_bytes;

	/**
	 * @brief FIXME: ???
	 */
	size_t slab_bytes;

	/**
	 * @brief Индексы таблицы
	 *
	 * Индексы таблицы задаются как в данном массиве, так и связываются
	 * в список, привязанный к первому индексу, который является первичным
	 * индексом таблицы. Задание в массиве позволяет ускорить операции
	 * добавления/удаления индекса, а связывание в список - операции
	 * обхода индексов
	 */
	Index<BasicIndex>* index[MAX_IDX];
};

/**
 * @brief Цикл по индексам таблицы @a _osp начиная с первого
 */
#define foreach_index(_idx, _osp) \
	for (Index<BasicIndex>* _idx = (_osp)->index[0]; _idx; _idx = (Index<BasicIndex>*)_idx->next)

/**
 * @brief Цикл по индексам таблицы @a _osp начиная с первого, если параметр
 *        @a i равен 0 и со второго, если @a i > 0
 */
#define foreach_indexi(i, _idx, _osp) \
	for (Index<BasicIndex>* _idx = ((i == 0) ? (_osp)->index[0] : (_osp)->index[0]->next); _idx; _idx = (Index<BasicIndex>*)_idx->next)

/**
 * @brief Преобразование кода операции в её имя
 */
const char* box_op_name (u16 _op);

/**
 * @brief Заполнить внутреннюю базу данных пространства имён
 */
void object_space_fill_stat_names (struct object_space* _osp);

/**
 * @brief Очистить внутреннюю базу данных пространства имён
 */
void object_space_clear_stat_names (struct object_space* _osp);

/**
 * @name Врапперы для соответствующих функций сбора статистики
 */
/** {*/
void box_stat_collect (int _name, i64 _v);
void box_stat_collect_double (int _name, double _v);
void box_stat_sum_named (char const* _name, int _n, double _v);
void box_stat_aggregate_named (char const* _name, int _n, double _v);
/** @}*/

/**
 * @brief Инициализация подсистемы сбора статистики
 */
void box_stat_init (void);

/**
 * @brief Отформатировать заданную область памяти во внутренний буфер в виде
 *        строки, завершающейся нулём
 */
const char* dump (void* data, size_t _len);

#endif // __BOX_COMMON_H
