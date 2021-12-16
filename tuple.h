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
 * @brief Коды типов объектов
 */
enum object_type
{
	/**
	 * @brief Обычный объект, размер блока данных которого больше 255 байт
	 */
	BOX_TUPLE = 1,

	/**
	 * @brief Компактный объект, размер блока данных которого меньше 256 байт
	 *
	 * Экономия на каждом объекте по сравнению с BOX_TUPLE составляет 6 байт
	 */
	BOX_SMALL_TUPLE = 2,

	/**
	 * @brief Список изменений объекта
	 */
	BOX_PHI = 3
};

/**
 * @brief Объект, размер данных которого меньше 256 байт
 */
struct box_small_tuple
{
	/**
	 * @brief Размер блока данных, который начинается с @a data
	 */
	uint8_t bsize;

	/**
	 * @brief Количество полей
	 */
	uint8_t cardinality;

	/**
	 * @brief Начало блока данных
	 */
	uint8_t data[0];
};

/**
 * @brief Указатель на объект, извлекаемый из tnt_object
 *
 * @param[in] obj указатель на tnt_object
 */
#define box_small_tuple(obj) ((struct box_small_tuple*)((obj) + 1))

/**
 * @brief Объект, размер данных которого больше 255 байт
 */
struct box_tuple
{
	/**
	 * @brief Размер блока данных, который начинается с @a data
	 */
	u32 bsize;

	/**
	 * @brief Количество полей
	 */
	u32 cardinality;

	/**
	 * @brief Начало блока данных
	 */
	u8 data[0];
} __attribute__((packed));

/**
 * @brief Указатель на объект, извлекаемый из tnt_object
 *
 * @param[in] obj указатель на tnt_object
 */
#define box_tuple(_obj) ((struct box_tuple*)((_obj) + 1))

/**
 * @brief Объявляем список (phi_cells), который предназначен для
 *        связывания box_phi_cell
 */
TAILQ_HEAD(phi_cells, box_phi_cell);

/**
 * @brief Список версий объекта, который замещает соответвующий объект
 *        в индексе
 *
 * Связь box_phi_cell выполняется по полю box_phi_cell::link
 */
struct box_phi
{
	/**
	 * @brief Заголовок, который позволяет подставлять в индекс список версий
	 *        вместо реального объекта
	 */
	struct tnt_object header;

	/**
	 * @brief Указатель на версию объекта, которая была актуальна перед началом
	 *        всей транзакции
	 *
	 * Данный объект возвращается в индекс при откате транзакции.
	 */
	struct tnt_object* obj;

	/**
	 * @brief Список всех версий объекта в рамках данного индекса
	 *
	 * Этот список содержит указатели на box_phi_cell, однако ими не владеет.
	 * box_phi_cell'ы являются собственностью box_op'ов. При этом самим box_phi
	 * владеет индекс, в который он добавлен, так как он симулирует обычный
	 * объект как структурой, так и поведением
	 */
	struct phi_cells cells;

	/**
	 * @brief Указатель на первую операцию, которая изменила данный индекс
	 *
	 * Используется только для отладки
	 */
	struct box_op* bop;

	/**
	 * @brief Индекс, в котором находится данный список изменений
	 */
	Index<BasicIndex>* index;
};

/**
 * @brief Получить указатель на структуру box_phi по указателю на её заголовок
 */
#define box_phi(_obj) container_of (_obj, struct box_phi, header)

/**
 * @brief Запись о версии объекта в индексе
 */
struct box_phi_cell
{
	/**
	 * @brief Указатель на версию объекта, которая внесена в индекс в результате
	 *        данной операции
	 */
	struct tnt_object* obj;

	/**
	 * @brief Указатель на список версий объекта в индексе
	 */
	struct box_phi* phi;

	/**
	 * @brief Операция, которая привела к появлению данной версии объекта
	 *
	 * Используется только для отладки
	 */
	struct box_op* bop;

	/**
	 * @brief Поле для связывания в список всех версий одного объекта в рамках
	 *        индекса
	 */
	TAILQ_ENTRY(box_phi_cell) phi_link;

	/**
	 * @brief Поле для связывания в список всех версий объекта для всех
	 *        изменений индексов, которые были сделаны одной операцией
	 */
	TAILQ_ENTRY(box_phi_cell) bop_link;
};

/**
 * @brief Выбросить исключение о неверном типе объекта
 */
void __attribute__((noreturn)) bad_object_type (void);

/**
 * @brief Распределить память для объекта с заданным числом полей
 *
 * @param[in] _cardinality число полей
 * @param[in] _size общий размер данных
 *
 * Следует обратить внимание, что памятью обычного объекта управляем с
 * помощью подсчёта ссылок. Зачем так сделано не совсем понятно, так
 * как в основном коде запись всегда принадлежит только одному владельцу.
 */
struct tnt_object* tuple_alloc (unsigned _cardinality, unsigned _size);

/**
 * @brief Удалить объект
 */
void tuple_free (struct tnt_object* _obj);

/**
 * @brief Проверка объекта на валидность
 *
 * Объект считается валидным, если его размер, записанный в соответствующем поле
 * совпадает с размером, посчитанным по его полям
 */
int tuple_valid (struct tnt_object* _obj);

/**
 * @brief Размер объекта
 *
 * Работает только для реальных объектов, для box_phi не работает
 */
int tuple_bsize (const struct tnt_object* _obj);

/**
 * @brief Количество полей объекта
 *
 * Для box_phi возвращает количество полей первой версии объекта (это может быть
 * либо объект от предыдущего коммита, либо первый добавленный объект, если объекта
 * на момент завершения предыдущего коммита не существовало)
 */
int tuple_cardinality (const struct tnt_object* _obj);

/**
 * @brief Данные объекта
 *
 * Для box_phi возвращает данные первой версии объекта (это может быть либо объект
 * от предыдущего коммита, либо первый добавленный объект, если объекта на момент
 * завершения предыдущего коммита не существовало)
 */
void* tuple_data (struct tnt_object* _obj);

/**
 * @brief Получить указатель на начало следующего поля объекта
 */
void* next_field (void* _f);

/**
 * @brief Общий размер объекта, посчитанный по включённым в него полям
 */
ssize_t fields_bsize (u32 _cardinality, const void* _data, u32 _len);

/**
 * @brief Поле объекта по заданному индексу (начиная с 0)
 *
 * @param[in] _obj объект
 * @param[in] _i индекс поля
 */
void* tuple_field (struct tnt_object* _obj, size_t _i);

/**
 * @brief Добавить объект в буфер вывода
 */
void net_tuple_add (struct netmsg_head* _h, struct tnt_object* _obj);

/**
 * @brief Создать структуру box_phi для заданных индекса, объекта и операции
 */
struct box_phi* phi_alloc (Index<BasicIndex>* _index, struct tnt_object* _obj, struct box_op* _bop);

/**
 * @brief Создать структуру для фиксации изменения для заданного списка изменений,
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
 * возвращается объект до удаления и обновления. Для случая если первой операцией
 * была вставка объекта, то возвращается добавленный объект
 */
struct tnt_object* phi_obj (const struct tnt_object* _obj);

/**
 * @brief Первая версия объекта
 */
struct tnt_object* phi_left (struct tnt_object* _obj);

/**
 * @brief Последняя версия объекта
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
 * @brief Проверка соответствия объекта индексу
 *
 * FIXME: пока объект индексируется, если индекс не частичный, или
 *        индексируемые поля присутствуют в объекте, и имеют значение
 *        отличное от NULL (то есть длина поля должна быть отлична от
 *        нуля, для полей типа STRING это означает, что строки не должны
 *        быть пустыми)
 */
bool tuple_match (struct index_conf* _ic, struct tnt_object* _obj);

/**
 * @brief Инициализация аллокатора памяти для структур box_phi и box_phi_cell
 */
void phi_cache_init (void);

#endif // __BOX_TUPLE_H
