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
#ifndef __BOX_TUPLE_H
#define __BOX_TUPLE_H

#import <third_party/queue.h>

#import <index.h>
#import <fiber.h>
#import <octopus_ev.h>
#import <octopus.h>

//
// Предварительное объявление структуры, чтобы можно было объявлять
// указатели на неё. Эти указатели используются только для отладки
//
struct box_op;

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
 * @brief Указатель на область памяти за заголовком tnt_object
 */
#define box_small_tuple(obj) ((struct box_small_tuple*)((obj) + 1))

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
 * @brief Указатель на область памяти за заголовком tnt_object
 */
#define box_tuple(_obj) ((struct box_tuple*)((_obj) + 1))

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
 * @brief Получить указатель на структуру box_phi по указателю на её заголовок
 */
#define box_phi(_obj) container_of (_obj, struct box_phi, header)

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
 * @brief Выбросить исключение о неверном типе объекта
 */
void __attribute__((noreturn)) bad_object_type (void);

/**
 * @brief Распределить память для записи с заданным числом полей
 *
 * @param[in] _cardinality число полей записи
 * @param[in] _size общий размер данных записи
 *
 * Следует обратить внимание, что память под обычную запись управляется
 * с помощью подсчёта ссылок. Зачем так сделано не совсем понятно, так
 * как в основном коде запись всегда принадлежит только одному владельцу.
 */
struct tnt_object* tuple_alloc (unsigned _cardinality, unsigned _size);

/**
 * @brief Удалить запись
 */
void tuple_free (struct tnt_object* _obj);

/**
 * @brief Проверка записи на валидность
 *
 * Запись считается валидной, если её размер, записанный в соответствующем поле
 * совпадает с размером, посчитанным по полям записи
 */
int tuple_valid (struct tnt_object* _obj);

/**
 * @brief Размер записи
 *
 * Работает только для реальных записей. Для box_phi не работает
 */
int tuple_bsize (const struct tnt_object* _obj);

/**
 * @brief Количество полей записи
 *
 * Для box_phi возвращает количество полей первой версии записи (это может быть
 * либо запись от предыдущего коммита, либо первая добавленная запись, если запись
 * на момент завершения предыдущего коммита не существовала)
 */
int tuple_cardinality (const struct tnt_object* _obj);

/**
 * @brief Данные записи
 *
 * Для box_phi возвращает данные первой версии записи (это может быть либо запись
 * от предыдущего коммита, либо первая добавленная запись, если запись на момент
 * завершения предыдущего коммита не существовала)
 */
void* tuple_data (struct tnt_object* _obj);

/**
 * @brief Получить указатель на начало следующего поля записи
 */
void* next_field (void* _f);

/**
 * @brief Общий размер записи, посчитанные по включённым в неё полям
 */
ssize_t fields_bsize (u32 _cardinality, const void* _data, u32 _len);

/**
 * @brief Поле записи по заданному индексу (начиная с 0)
 *
 * @param[in] _obj запись
 * @param[in] _i индекс поля записи
 */
void* tuple_field (struct tnt_object* _obj, size_t _i);

/**
 * @brief Добавить запись в буфер вывода
 */
void net_tuple_add (struct netmsg_head* _h, struct tnt_object* _obj);

/**
 * @brief Создать структуру box_phi для заданных индекса, объекта и операции
 */
struct box_phi* phi_alloc (Index<BasicIndex>* _index, struct tnt_object* _obj, struct box_op* _bop);

/**
 * @brief Создать структуру для фиксации изменений для заданных списка изменений,
 *        объекта и операции
 */
struct box_phi_cell* phi_cell_alloc (struct box_phi* _phi, struct tnt_object* _obj, struct box_op* _bop);

/**
 * @brief Удалить заданную структуру
 */
void phi_free (struct box_phi* _phi);

/**
 * @brief Удалить заданную структуру
 */
void phi_cell_free (struct box_phi_cell* _cell);

/**
 * @brief Первый объект списка изменений
 *
 * Для случая если первой операцией были удаление или обновление данных, то
 * возвращается запись до удаления и обновления. Для случая если первой операцией
 * была вставка записи, то возвращается вставленная запись
 */
struct tnt_object* phi_obj (const struct tnt_object* _obj);

/**
 * @brief Первая версия записи
 */
struct tnt_object* phi_left (struct tnt_object* _obj);

/**
 * @brief Последняя версия записи
 */
struct tnt_object* phi_right (struct tnt_object* _obj);

/**
 * @brief Альтернативное название функции phi_left
 */
struct tnt_object* tuple_visible_left (struct tnt_object* _obj);

/**
 * @brief Альтернативное название функции phi_right
 */
struct tnt_object* tuple_visible_right (struct tnt_object* _obj);

/**
 * @brief Инициализация аллокатора памяти для структур box_phi и box_phi_cell
 */
void phi_cache_init (void);

#endif // __BOX_TUPLE_H
