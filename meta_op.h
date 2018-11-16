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
#ifndef __BOX_META_OP_H
#define __BOX_META_OP_H

#import <index.h>

#import <mod/box/common.h>
#import <mod/box/box.h>

/**
 * @brief Мета транзакция
 *
 * Мета транзакции выполняют изменения структуры данных
 */
struct box_meta_txn
{
	/**
	 * @brief Выполняемая операция
	 */
	u16 op;

	/**
	 * @brief Флаги выполняемой операции
	 */
	u32 flags;

	/**
	 * @brief Модуль, в котором выполняется транзакция
	 */
	Box* box;

	/**
	 * @brief Таблица, для которой выполняется транзакция
	 */
	struct object_space* object_space;

	/**
	 * @brief Индекс, для которого выполняется транзакция
	 */
	Index<BasicIndex>* index;
};

/**
 * @brief Удалить все данные из заданной таблицы заданного шарда
 *
 * Данная функция является частью OCTOPUS/BOX LUA API
 *
 * @return -1 - индекс шарда вне допустимого диапазона;
 *         -2 - шард с заданным индексом отсутствует;
 *         -3 - шард является репликой;
 *         -4 - с шардом не связан модуль;
 *         -5 - в шарде отсутствует таблица с заданным индексом;
 *         -6 - не удалось сохранить запись в журнал;
 *         0...n - число удалённых из таблицы записей
 */
int box_meta_truncate (int _shard_id, int _n);

/**
 * @brief Функция-диспетчер для подготовки изменений структуры БД
 *
 * Модифицировать здесь что либо нельзя, так как потом невозможно будет
 * откатить изменения. Поэтому в prepare делаем только анализ возможности
 * выполнения изменения и запоминаем необходимые параметры (возможно с
 * предварительным созданием заготовки объекта)
 */
void box_meta_prepare (struct box_meta_txn* _tx, struct tbuf* _data);

/**
 * @brief Подтвердить изменения в структуре
 */
void box_meta_commit (struct box_meta_txn* _tx);

/**
 * @brief Откатить изменения в структуре
 */
void box_meta_rollback (struct box_meta_txn* _tx);

/**
 * @brief Обработчик команд изменения мета-информации
 */
void box_meta_cb (struct netmsg_head* _wbuf, struct iproto* _request);

#endif // _BOX_META_OP_H
