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
#ifndef __BOX_OP_H
#define __BOX_OP_H

#import <third_party/queue.h>

#import <iproto.h>

#import <mod/box/common.h>
#import <mod/box/tuple.h>
#import <mod/box/box.h>

/**
 * @name Дополнительные флаги операций
 */
/** @{*/
/**
 * @brief Запрошен возврат записи, которая является результатом операции
 */
#define BOX_RETURN_TUPLE 1

/**
 * @brief Запрошено добавления записи
 *
 * В случае существования предыдущей версии записи детектируется ошибка.
 */
#define BOX_ADD 2

/**
 * @brief Запрошена замена записи
 *
 * В случае отсутствия предыдущей версии записи детектируется ошибка.
 */
#define BOX_REPLACE 4
/** @}*/

/**
 * @brief Режимы работы транзакций
 */
enum txn_mode
{
	/**
	 * @brief Транзакция только читает данные
	 */
	RO,

	/**
	 * @brief Транзакция читает и модифицирует данные
	 */
	RW
} mode;

/**
 * @brief Состояние транзакции
 */
enum txn_state
{
	/**
	 * @brief Неопределённое состояние транзакции
	 *
	 * Новая транзакция находится в этом состоянии, пока не будет
	 * подтверждена или отменена.
	 */
	UNDECIDED,

	/**
	 * @brief Транзакция подтверждена
	 */
	COMMIT,

	/**
	 * @brief Транзакция отменена
	 */
	ROLLBACK
};

/**
 * @brief Транзакция
 */
struct box_txn
{
	/**
	 * @brief Модуль, в котором выполняется транзакция
	 */
	Box* box;

	/**
	 * @brief Текущий режим транзакции
	 */
	enum txn_mode mode;

	/**
	 * @brief Текущее состояние транзакции
	 */
	enum txn_state state;

	/**
	 * @brief Число затронутых транзакцией записей
	 */
	u32 obj_affected;

	/**
	 * @brief Количество записей в журнале транзакций
	 */
	u32 submit;

	/**
	 * @brief Идентификатор транзакции
	 */
	int id;

	/**
	 * @brief Сопрограмма, в рамках которой выполняется транзакция
	 */
	struct Fiber* fiber;

	/**
	 * @brief Имя транзакции
	 */
	const char* name;

	/**
	 * @brief Длина имени
	 */
	size_t namelen;

	/**
	 * @brief Время запуска транзакции
	 */
	ev_tstamp start;

	/**
	 * @brief Список операций, выполненных в рамках данной транзакции
	 */
	TAILQ_HEAD(box_op_tailq, box_op) ops;
};

/**
 * @brief Операция над данными
 */
struct box_op
{
	/**
	 * @brief Код операции
	 */
	u16 op;

	/**
	 * @brief Флаги операции
	 */
	u32 flags;

	/**
	 * @brief Таблица, к записи которой применена операция
	 */
	struct object_space *object_space;

	/**
	 * @brief Версия объекта до выполнения операции
	 */
	struct tnt_object* old_obj;

	/**
	 * @brief Версия объекта после выполнения операции
	 */
	struct tnt_object* obj;

	/**
	 * @brief Результат операции
	 *
	 * Может быть указателем либо на old_obj либо на objю
	 */
	struct tnt_object* ret_obj;

	/**
	 * @brief Список версий объекта в каждом из индексов, которые были
	 *        изменены операцией
	 *
	 * Связь box_phi_cell выполняется по полю box_phi_cell::bop_link.
	 * Всеми box_phi_cell в этом списке владеет сама операция. При этом
	 * box_phi_cell одновременно добавлены в списки версий объекта
	 * box_phi, которые замещают объект в каждом изменённом индексе.
	 * Списки версий box_phi_cell'ами не владеют.
	 */
	struct phi_cells cells;

	/**
	 * @brief Количество объектов операции
	 *
	 * Если это операция добавления записи, то 1, если замены, то 2, если
	 * удаления, то 1.
	 */
	u32 obj_affected;

	/**
	 * @brief Структура для связывания операций в список операций, выполненных
	 *        одной транзакцией
	 */
	TAILQ_ENTRY(box_op) link;

	/**
	 * @brief Указатель на транзакцию, в рамках которой выполнена операция
	 */
	struct box_txn* txn;

	/**
	 * @brief Размер данных операции
	 */
	int data_len;

	/**
	 * @brief Начало блока данных операции
	 */
	char data[];
};

/**
 * @brief Создание транзакции
 */
struct box_txn* box_txn_alloc (int _shard_id, enum txn_mode _mode, const char* _name);

/**
 * @brief Выполнение операции модификации данных
 */
struct box_op* box_prepare (struct box_txn* _txn, int _op, const void* _data, u32 _len);

/**
 * @brief Запись транзакции в журнал
 */
int box_submit (struct box_txn* _txn) __attribute__ ((warn_unused_result));

/**
 * @brief Зафиксировать транзакцию
 */
void box_commit (struct box_txn* _txn);

/**
 * @brief Откатить транзакцию
 */
void box_rollback (struct box_txn* _txn);

/**
 * @brief Выполнение операции добавления/замены записи
 */
void prepare_replace (struct box_op* _bop, size_t _cardinality, const void* _data, u32 _len);

#if CFG_lua_path || CFG_caml_path
/**
 * @brief Обработка вызова процедур Ocaml и/или LUA
 */
void box_proc_cb (struct netmsg_head* _wbuf, struct iproto* _request);
#endif

/**
 * @brief Обработчик команд модификации данных базы
 */
void box_cb (struct netmsg_head* _wbuf, struct iproto* _request);

/**
 * @brief Обработчик выборок данных из базы
 */
void box_select_cb (struct netmsg_head* _wbuf, struct iproto* _request);

#endif // __BOX_OP_H
