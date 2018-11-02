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
#ifndef TARANTOOL_SILVERBOX_H
#define TARANTOOL_SILVERBOX_H

#include <index.h>

#import <iproto.h>
#import <net_io.h>
#import <objc.h>
#import <log_io.h>

@class Box;
struct index;

/**
 * @brief Максимальное число индексов для таблицы объектов
 *
 * При изменении данного числа необходимо изменить соответствующее
 * определение в box.lua
 */
#define MAX_IDX 10

/**
 * @brief Таблица объектов
 */
struct object_space
{
	/**
	 * @brief Код таблицы (фактически является её именем)
	 */
	int n;

	/**
	 * @brief Данный код таблицы игнорируется
	 */
	bool ignored;

	/**
	 * @brief FIXME: ???
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
 * @brief Цикл по индексам таблицы объектов @a _osp начиная с первого
 */
#define foreach_index(_idx, _osp) \
	for (Index<BasicIndex>* _idx = (_osp)->index[0]; _idx; _idx = (Index<BasicIndex>*)_idx->next)

/**
 * @brief Цикл по индексам таблицы объектов @a _osp начиная с первого,
 *        если параметр @a i равен 0 и со второго, если @a i > 0
 */
#define foreach_indexi(i, _idx, _osp) \
	for (Index<BasicIndex>* _idx = ((i == 0) ? (_osp)->index[0] : (_osp)->index[0]->next); _idx; _idx = (Index<BasicIndex>*)_idx->next)

/**
 * @brief Коды типов данных, которые могут индексироваться
 */
enum object_type
{
	/**
	 * @brief Обычная запись, размер блока данных которой больше 255 байт
	 */
	BOX_TUPLE = 1,

	/**
	 * @brief Компактная запись, размер блока данных которой меньше 256 байт
	 *
	 * Экономия на каждой записи по сравнению с BOX_TUPLE составляет 6 байт
	 */
	BOX_SMALL_TUPLE = 2,

	/**
	 * @brief Список изменений индекса
	 */
	BOX_PHI = 3
};

/**
 * @brief Запись, размер данных которой больше 255 байт
 */
struct box_tuple
{
	/**
	 * @brief Размер блока данных, который начинается с @a data
	 */
	u32 bsize;

	/**
	 * @brief Количество полей в записи
	 */
	u32 cardinality;

	/**
	 * @brief Начало блока данных
	 */
	u8 data[0];
} __attribute__((packed));

/**
 * @brief Запись, размер данных которой меньше 256 байт
 */
struct box_small_tuple
{
	/**
	 * @brief Размер блока данных, который начинается с @a data
	 */
	uint8_t bsize;

	/**
	 * @brief Количество полей в записи
	 */
	uint8_t cardinality;

	/**
	 * @brief Начало блока данных
	 */
	uint8_t data[0];
};

/**
 * @brief Объявляем тип заголовка списка (phi_tailq), который предназначен
 *        для хранения box_phi_cell
 */
TAILQ_HEAD(phi_tailq, box_phi_cell);

/**
 * @brief Запись, которая замещает реальную запись в индексе и представляет
 *        собой список всех последовательных изменений данного индекса по
 *        заданному ключу
 *
 * Связь box_phi_cell выполняется по полю box_phi_cell::link
 */
struct box_phi
{
	/**
	 * @brief Заголовок, который позволяет подставлять box_phi в индекс вместо
	 *        реальной записи
	 */
	struct tnt_object header;

	/**
	 * @brief Указатель на версию записи, которая была сохранена в результате
	 *        предыдущего коммита перед началом данной транзакции
	 */
	struct tnt_object* obj;

	/**
	 * @brief Список всех изменений данного индекса
	 *
	 * Данный список содержит указатели на box_phi_cell, однако этими объектами не
	 * владеет. box_phi_cell'ы являются собственностью box_op'ов. При этом самим
	 * объектом box_phi владеет индекс, в который он добавлен, так как он симулирует
	 * обычную запись с данными как структурой, так и поведением
	 */
	struct phi_tailq tailq;

	/**
	 * @brief Указатель на первую (последнюю?) операцию, которая изменила
	 *        данный индекс
	 *
	 * Используется только для отладки
	 */
	struct box_op* bop;

	/**
	 * @brief Индекс, в котором находится данный объект
	 */
	Index<BasicIndex>* index;
};

/**
 * @brief Запись об изменении индекса
 */
struct box_phi_cell
{
	/**
	 * @brief Указатель на связанную версию записи, для которой изменён индекс
	 */
	struct tnt_object* obj;

	/**
	 * @brief Указатель на список всех изменений индекса, в котором находится
	 *        данное изменение
	 */
	struct box_phi* head;

	/**
	 * @brief Операция, которая привела к появлению данного изменения
	 *
	 * Используется только для отладки
	 */
	struct box_op* bop;

	/**
	 * @brief Структура для связывания изменений одного индекса
	 */
	TAILQ_ENTRY(box_phi_cell) link;

	/**
	 * @brief Структура для связывания изменений всех индексов, выполненых
	 *        одной операцией
	 */
	TAILQ_ENTRY(box_phi_cell) bop_link;
};

/**
 * @brief Заглушка для настройки пула распределителя памяти
 *
 * Для распределения памяти под эти два типа объектов используем один
 * и тот же пул распределяя память по наибольшему из объектов. Это
 * позволяет уменьшить фрагментацию памяти мелкими объектами
 */
union box_phi_union
{
	struct box_phi phi;
	struct box_phi_cell cell;
};

/**
 * @brief Первая версия записи
 */
struct tnt_object* phi_left (struct tnt_object* _obj);

/**
 * @brief Последняя версия записи
 */
struct tnt_object* phi_right (struct tnt_object* _obj);

/**
 * @brief Первый объект списка изменений
 *
 * Для случая если первой операцией были удаление или обновление данных, то
 * возвращается запись до удаления и обновления. Для случая если первой операцией
 * была вставка записи, то возвращается вставленная запись
 */
struct tnt_object* phi_obj (const struct tnt_object* _obj);

/**
 * @brief Альтернативное название функции phi_left
 */
struct tnt_object* tuple_visible_left (struct tnt_object* _obj);

/**
 * @brief Альтернативное название функции phi_right
 */
struct tnt_object* tuple_visible_right (struct tnt_object* _obj);

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
static inline struct box_snap_row*
box_snap_row (const struct tbuf* t)
{
	return (struct box_snap_row *)t->ptr;
}

/**
 * @brief FIXME: ?
 */
extern struct dtor_conf box_tuple_dtor;

/**
 * @brief Конфигурация индекса
 */
struct index_conf* cfg_box2index_conf (struct octopus_cfg_object_space_index* c, int sno, int ino, int panic);

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
	 * @brief Запись до выполнения операции
	 */
	struct tnt_object* old_obj;

	/**
	 * @brief Запись после выполнения операции
	 */
	struct tnt_object* obj;

	/**
	 * @brief Результат операции
	 *
	 * Может быть указателем либо на old_obj либо obj
	 */
	struct tnt_object* ret_obj;

	/**
	 * @brief Список изменений всех индексов для данной операции
	 *
	 * Связь box_phi_cell выполняется по полю box_phi_cell::bop_link.
	 * Всеми объектами box_phi_cell в этом списке владеет сама
	 * операция. Эти объекты одновременно добавлены в списки объекта
	 * box_phi, который этими объектами не владеет
	 */
	struct phi_tailq phi;

	/**
	 * @brief Количество объектов операции
	 *
	 * Если это операция добавления записи, то 1, если замены, то 2, если
	 * удаления, то FIXME: ???
	 */
	u32 obj_affected;

	/**
	 * @brief Структура для связывания операций в список операций, выполненных
	 *        одной транзакцией
	 */
	TAILQ_ENTRY(box_op) link;

	/**
	 * @brief Указатель на транзакцию, в рамках которой выполнена операция
	 *
	 * Используется только для отладки
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
	 * подтверждена или откачена
	 */
	UNDECIDED,

	/**
	 * @brief Транзакция подтверждена
	 */
	COMMIT,

	/**
	 * @brief Транзакция откачена
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
	 * @brief FIXME: ???
	 */
	u32 submit;

	/**
	 * @brief Идентификатор транзакции
	 */
	int id;

	/**
	 * @brief Сопрограмма, в рамках которой выполняется транзакция
	 *
	 * Только для отладки
	 */
	struct Fiber* fiber;

	/**
	 * @brief FIXME: ???
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
 * @brief Создание транзакции
 */
struct box_txn* box_txn_alloc (int _shard_id, enum txn_mode _mode, const char* _name);

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

/**
 * @brief Загрузака данных из снапшота в память
 */
void snap_insert_row (struct object_space* _osp, size_t _cardinality, const void* _data, u32 _len);

/**
 * @brief Функция-диспетчер для подготовки изменений структуры БД
 *
 * Модифицировать здесь что либо нельзя, так как потом невозможно будет
 * откатить изменения. Поэтому в prepare делаем только анализ возможности
 * выполнения изменения и запоминаем необходимые параметры (возможно с
 * предварительным созданием заготовки объекта)
 */
void box_prepare_meta (struct box_meta_txn* _tx, struct tbuf* _data);

/**
 * @brief Подтвердить изменения в структуре
 */
void box_commit_meta (struct box_meta_txn* _tx);

/**
 * @brief Откатить изменения в структуре
 */
void box_rollback_meta (struct box_meta_txn* _tx);

void box_shard_create (int n, int type);

/**
 * @brief Инициализация обработчиков запросов
 */
void box_service (struct iproto_service* _s);

/**
 * @brief Инициализация обработчиков запросов для систем только-чтение
 */
void box_service_ro (struct iproto_service* _s);

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
 * В случае существования предыдущей версии записи детектируется ошибка
 */
#define BOX_ADD 2

/**
 * @brief Запрошена замена записи
 *
 * В случае отсутствия предыдущей версии записи детектируется ошибка
 */
#define BOX_REPLACE 4
/** @}*/

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
 * @brief FIXME: ???
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
extern char const* const box_ops[];

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
 * @brief Заполнить внутреннюю базу данных пространства имён
 */
void object_space_fill_stat_names (struct object_space* _osp);

/**
 * @brief Очистить внутреннюю базу данных пространства имён
 */
void object_space_clear_stat_names (struct object_space* _osp);

/**
 * @brief Максимально возможное число таблиц
 */
#define OBJECT_SPACE_MAX (1024)

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

	const int object_space_max_idx;

	int version;
}
@end

/**
 * @brief Таблица с заданным номером
 */
struct object_space* object_space (Box* box, int n);

/**
 * @brief Версия модуля
 */
int box_version ();

/**
 * @brief Получить указатель на начало следующего поля записи
 */
void* next_field (void* _f);

/**
 * @brief Общий размер записи, посчитанные по включённым в неё полям
 */
ssize_t fields_bsize (u32 _cardinality, const void* _data, u32 _len);

/**
 * @brief Выбросить исключение о неверном типе объекта
 */
void __attribute__((noreturn)) bad_object_type (void);

/**
 * @brief Указатель на область памяти за заголовком tnt_object
 */
#define box_tuple(_obj) ((struct box_tuple*)((_obj) + 1))

/**
 * @brief Указатель на область памяти за заголовком tnt_object
 */
#define box_small_tuple(obj) ((struct box_small_tuple*)((obj) + 1))

/**
 * @brief Размер записи
 *
 * Работает только для реальных записей. Для box_phi не работает
 */
static inline int tuple_bsize (const struct tnt_object* _obj)
{
	switch (_obj->type)
	{
		case BOX_TUPLE:
			return box_tuple (_obj)->bsize;

		case BOX_SMALL_TUPLE:
			return box_small_tuple (_obj)->bsize;

		default:
			bad_object_type ();
	}
}

/**
 * @brief Количество полей записи
 *
 * Для box_phi возвращает количество полей первой версии записи (это может быть
 * либо запись от предыдущего коммита, либо первая добавленная запись, если запись
 * на момент завершения предыдущего коммита не существовала)
 */
static inline int tuple_cardinality (const struct tnt_object* _obj)
{
	switch (_obj->type)
	{
		case BOX_TUPLE:
			return box_tuple (_obj)->cardinality;

		case BOX_SMALL_TUPLE:
			return box_small_tuple (_obj)->cardinality;

		case BOX_PHI:
			return tuple_cardinality (phi_obj (_obj));

		default:
			bad_object_type();
	}
}

/**
 * @brief Данные записи
 *
 * Для box_phi возвращает данные первой версии записи (это может быть либо запись
 * от предыдущего коммита, либо первая добавленная запись, если запись на момент
 * завершения предыдущего коммита не существовала)
 */
static inline void* tuple_data (struct tnt_object* _obj)
{
	switch (_obj->type)
	{
		case BOX_TUPLE:
			return box_tuple (_obj)->data;

		case BOX_SMALL_TUPLE:
			return box_small_tuple (_obj)->data;

		case BOX_PHI:
			return tuple_data (phi_obj (_obj));

		default:
			bad_object_type ();
	}
}

/**
 * @brief Поле записи по заданному индексу (начиная с 0)
 *
 * @param[in] _obj запись
 * @param[in] _i индекс поля записи
 */
void* tuple_field (struct tnt_object* _obj, size_t _i);

/**
 * @brief Проверка записи на валидность
 *
 * Запись считается валидной, если её размер, записанный в соответствующем поле
 * совпадает с размером, посчитанным по полям записи
 */
int tuple_valid (struct tnt_object* _obj);

/**
 * @brief Удалить запись
 */
void tuple_free (struct tnt_object* _obj);

/**
 * @brief Добавить запись в буфер вывода
 */
void net_tuple_add (struct netmsg_head* _h, struct tnt_object* _obj);

int box_cat_scn (i64 stop_scn);

int box_cat (const char* filename);

void box_print_row (struct tbuf* out, u16 tag, struct tbuf* r);

const char* box_row_to_a (u16 tag, struct tbuf* r);

struct print_dups_arg
{
	int space;
	int index;
	struct tbuf* positions;
};

void box_idx_print_dups (void* arg, struct index_node* a, struct index_node* b, uint32_t position);

void box_op_init (void);

/**
 * @brief Констурирование пустого буфера, указывающего на заданную область памяти
 */
#define TBUF_BUF(buf) (struct tbuf){.ptr = (buf), .end = (buf), .free = sizeof (buf), .pool = NULL}

/**
 * @brief Конструирование буфера, указывающего на заданную область памяти,
 *        инициализированную данными заданной длины
 */
#define TBUF_BUFL(buf, l) (struct tbuf){.ptr = (buf), .end = (buf) + (l), .free = sizeof (buf) - (l), .pool = NULL}

#endif
