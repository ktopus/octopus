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
#ifndef __BOX_H
#define __BOX_H

#include <index.h>

#import <iproto.h>
#import <net_io.h>
#import <objc.h>
#import <log_io.h>

#import <mod/box/common.h>
#import <mod/box/tuple.h>

/**
 * @brief Запись в журнале
 */
struct box_snap_row
{
	/**
	 * @brief Идентификатор таблицы
	 */
	u32 object_space;

	/**
	 * @brief Размер записи
	 */
	u32 tuple_size;

	/**
	 * @brief Размер данных
	 */
	u32 data_size;

	/**
	 * @brief Начало блока данных
	 */
	u8 data[];
} __attribute((packed));

/**
 * @brief Получение записи журнала из буфера
 */
#define box_snap_row(_t) ((struct box_snap_row*)(_t)->ptr)

/**
 * @brief Загрузака данных из снапшота в память
 */
void snap_insert_row (struct object_space* _osp, size_t _cardinality, const void* _data, u32 _len);

/**
 * @brief Реализация модуля нереляционной базы данных
 */
@interface Box : DefaultExecutor<Executor>
{
@public
	/**
	 * @brief Список таблиц для данных
	 */
	struct object_space* object_space_registry[OBJECT_SPACE_MAX];

	/**
	 * @brief Версия модуля
	 */
	int version;
}

-(void) set_shard:(Shard<Shard>*)_shard;
-(void) apply:(struct tbuf*)_data tag:(u16)_tag;
-(void) snap_final_row;
-(void) wal_final_row;
-(void) status_changed;
-(void) print:(const struct row_v12*)_row into:(struct tbuf*)_buf;
-(int) snapshot_fold;
-(u32) snapshot_estimate;
-(int) snapshot_write_rows:(XLog*)_log;
@end

/**
 * @brief Модуль, для которого выполняется транзакция
 */
Box* shard_box ();

/**
 * @brief Идентификатор шарда для текущей транзакции
 */
int shard_box_id ();

/**
 * @brief Версия модуля
 */
int box_version ();

/**
 * @brief Идентификатор данного модуля
 */
int box_shard_id (Box* _box);

/**
 * @brief Таблица с заданным номером
 */
struct object_space* object_space (Box* _box, int n);

/**
 * @brief Аргументы операции вывода дубликатов
 */
struct print_dups_arg
{
	int space;
	int index;
	struct tbuf* positions;
};

/**
 * @brief Функция реагирования на найденный дубликат
 *
 * @param[in] _varg аргументы сортировки
 * @param[in] _a предыдущий дубликат
 * @param[in] _b найденный дубликат
 * @param[in] _pos позиция дубликата в массиве узлов
 */
void box_idx_print_dups (void* _varg, struct index_node* _a, struct index_node* _b, uint32_t _pos);

#endif // __BOX_H
