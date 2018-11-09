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
#ifndef PICKLE_H
#define PICKLE_H

#include <util.h>
#include <string.h>
#include <tbuf.h>

/**
 * @brief Выброс исключения при нехватке памяти в буфере
 */
__attribute__((noreturn)) void tbuf_too_short ();

/**
 * @brief Проверить наличие в буфере данных заданного размера
 *
 * @param[in] _b проверяемый буфер
 * @param[in] _n размер данных, которые должен содержать буфер
 */
static inline void
_read_must_have (struct tbuf* _b, i32 _n)
{
	if (unlikely (tbuf_len (_b) < _n))
		tbuf_too_short ();
}

/**
 * @brief Проверить наличие в буфере данных заданного размера
 *
 * @param[in] _b проверяемый буфер
 * @param[in] _n размер данных, которые должен содержать буфер
 */
void read_must_have (struct tbuf* _b, i32 _n);

/**
 * @brief Проверить, что в буфере больше нет данных и выбросить исключение
 *
 * @param[in] _b проверяемый буфер
 * @param[in] _e опциональное сообщение
 */
void read_must_end (struct tbuf* _b, const char* _e);

/**
 * @brief Шаблон для генерации функций чтения из буфера целых со знаком
 */
#define read_u(bits)                                     \
	static inline u##bits read_u##bits (struct tbuf* _b) \
	{                                                    \
		_read_must_have (_b, bits/8);                    \
		u##bits r = *(u##bits*)_b->ptr;                  \
		tbuf_ltrim (_b, bits/8);                         \
		return r;                                        \
	}

/**
 * @brief Шаблон для генерации функций чтения из буфера целых со знаком
 */
#define read_i(bits)                                     \
	static inline i##bits read_i##bits (struct tbuf* _b) \
	{                                                    \
		_read_must_have (_b, bits/8);                    \
		i##bits r = *(i##bits*)_b->ptr;                  \
		tbuf_ltrim (_b, bits/8);                         \
		return r;                                        \
	}

read_u(8)
read_u(16)
read_u(32)
read_u(64)
read_i(8)
read_i(16)
read_i(32)
read_i(64)

/**
 * @brief Прочитать из буфера 32х битное беззнаковое целое
 */
u32 read_varint32 (struct tbuf* _b);

/**
 * @brief Прочитать из буфера пару {длина, данные}
 *
 * @return указатель на пару {длина, данные}
 */
void* read_field (struct tbuf* _b);

/**
 * @brief Прочитать из буфера блок данных заданного размера
 *
 * @return указатель на блок данных
 */
void* read_bytes (struct tbuf* _b, u32 _n);

/**
 * @brief Прочитать из буфера указатель
 *
 * @return значение прочитанного указателя
 */
void* read_ptr (struct tbuf* _b);

/**
 * @brief Прочитать из буфера данные заданного размера в заданную область памяти
 */
void read_to (struct tbuf* _b, void* _p, u32 _n);

/**
 * @brief Прочитать из буфера данные заданного размера в заданную область памяти
 *
 * @param[inout] _b буфер для чтения данных
 * @param[out]   _p область памяти для сохранения данныъ
 * @param[in]    _n размер данных для чтения
 */
static inline void _read_to (struct tbuf* _b, void* _p, u32 _n)
{
	memcpy (_p, read_bytes (_b, _n), _n);
}

/**
 * @brief Загрузить из буфера заданную структуру
 */
#define read_into (_b, _s) \
	_read_to ((_b), (_s), sizeof (*(_s)))

/**
 * @name Чтение из буфера знаковых и беззнаковых целых разной битности
 */
/** @{*/
u8  read_field_u8  (struct tbuf* _b);
u16 read_field_u16 (struct tbuf* _b);
u32 read_field_u32 (struct tbuf* _b);
u64 read_field_u64 (struct tbuf* _b);
i8  read_field_i8  (struct tbuf* _b);
i16 read_field_i16 (struct tbuf* _b);
i32 read_field_i32 (struct tbuf* _b);
i64 read_field_i64 (struct tbuf* _b);
/** @}*/

/**
 * @brief Прочитать из буфера блок данных, длина которого записана перед
 *        блоком
 *
 * @param[inout] _b буфер
 *
 * @return буфер, который содержит прочитанный блок данных
 */
struct tbuf* read_field_s (struct tbuf* _b);

/**
 * @name Запись в буфер целых со знаком разной битности
 */
/** @{*/
void write_i8  (struct tbuf* _b, i8  _v);
void write_i16 (struct tbuf* _b, i16 _v);
void write_i32 (struct tbuf* _b, i32 _v);
void write_i64 (struct tbuf* _b, i64 _v);
/** @}*/

/**
 * @brief Записать в буфер 32х битное беззнаковое целое
 *
 * @param[inout] _b буфер для записи
 * @param[in]    _v значение
 */
void write_varint32 (struct tbuf* _b, u32 _v);

/**
 * @name Запись в буфер целого со знаком в виде пары {размер, значение}
 */
/** @{*/
void write_field_i8  (struct tbuf* _b, i8  _v);
void write_field_i16 (struct tbuf* _b, i16 _v);
void write_field_i32 (struct tbuf* _b, i32 _v);
void write_field_i64 (struct tbuf* _b, i64 _v);
/** @}*/

/**
 * @brief Запись в буфер блока данных заданной длины
 *
 * Блок данных будет записан в виде {длина, данные}
 *
 * @param[inout] _b буфер для записи
 * @param[in]    _s блок данных
 * @param[in]    _n длина блока данных
 */
void write_field_s (struct tbuf* _b, const u8* _s, u32 _n);

/**
 * @brief Количество байт, которые потребуются для сохранения заданного
 *        беззнакового числа
 */
size_t varint32_sizeof (u32 _v);

/**
 * @brief Сохранить упакованное целое число без знака в заданной области памяти
 *
 * Буфер должен иметь достаточный размер для сохранения данных.
 *
 * @param[inout] _p указатель на область памяти для сохранения данных
 * @param[in] _v сохраняемое значение
 *
 * @return указатель на свободную область памяти после сохранения числа
 */
u8* save_varint32 (u8* _p, u32 _v);

/**
 * @brief Прочитать из заданной области памяти целое число
 *
 * @param[in] _pp указатель на указатель, в котором хранится позиция чтения
 *                следующего байта данных в буфере. После чтения числа указатель
 *                будет передвинут на начало данных, следующих за прочитанным
 *                числом
 *
 * @return прочитанное значение
 */
u32 _load_varint32 (void** _pp);

/**
 * @brief Декодирование 32х битных чисел, представленных в кодировке
 *        с переменным числом байт
 *
 * @param[inout] _data указатель на указатель на начало буфера
 *
 * @return значение числа с перемоткой начала буфера на следующие за числом данные
 */
static inline u32
load_varint32 (void** _data)
{
	//
	// Случай с числом, закодированном в одном байте
	//
	{
		//
		// Представляем переданный указатель как указатель на
		// последовательность байт
		//
		u8** data = (u8**)_data;

		//
		// Первый байт числа
		//
		u8 p = **data;
		//
		// Если старший бит байта числа не установлен, то это
		// число, для кодировки которого достаточно одного байта
		//
		if ((p & 0x80) == 0)
		{
			//
			// Перематываем указатель буфера на один байт вперёд
			//
			++(*data);
			//
			// ... и возвращаем значение декодированного числа
			//
			return p;
		}
	}

	//
	// Для декодирования многобайтных чисел используем другую функцию
	//
	return _load_varint32 (_data);
}

/**
 * @brief Декодирование целого числа не большего чем 2048383
 *
 * Для того, чтобы исключить конфликт по именам переменных все внутренние
 * переменные этого макроопределения дополнены префиксом LOAD_VARINT32_
 */
#define LOAD_VARINT32(_p) ({                                                        \
	const unsigned char* LOAD_VARINT32_p = (_p);                                    \
	int LOAD_VARINT32_v = *LOAD_VARINT32_p & 0x7f;                                  \
	if (*LOAD_VARINT32_p & 0x80)                                                    \
	{                                                                               \
		LOAD_VARINT32_v = (LOAD_VARINT32_v << 7) | (*++LOAD_VARINT32_p & 0x7f);     \
		if (*LOAD_VARINT32_p & 0x80)                                                \
			LOAD_VARINT32_v = (LOAD_VARINT32_v << 7) | (*++LOAD_VARINT32_p & 0x7f); \
	}                                                                               \
	_p = (__typeof__(_p))(LOAD_VARINT32_p + 1);                                     \
	LOAD_VARINT32_v;                                                                \
})

/**
 * @brief Tag/Len/Value структура для описания произвольных данных, упакованных в единый блок
 */
struct tlv
{
	/**
	 * @brief Тэг, определяющий тип упакованных данных
	 */
	u16 tag;

	/**
	 * @brief Размер блока данных
	 */
	u32 len;

	/**
	 * @brief Начало блока данных
	 */
	u8 val[0];
} __attribute((packed));

/**
 * @brief Добавить в буфер заголовок tlv-структуры с заданным тэгом
 *
 * Возвращается смещение в байтах в буфере, с которого начинается данный
 * заголовок. Возвращаем смещение а не указатель, так как в процессе
 * добавления данных в буфер память может перераспределяться и указатель
 * может стать невалидным
 */
static inline int
tlv_add (struct tbuf* _buf, u16 _tag)
{
	//
	// Заголовок tlv-структуры с заданным тэгом
	//
	struct tlv header = {.tag = _tag, .len = 0};

	//
	// Вычисляем смещение по текущему заполнению буфера
	//
	int off = (u8*)_buf->end - (u8*)_buf->ptr;

	//
	// Добавляем в буфер заголовок
	//
	tbuf_append (_buf, &header, sizeof (header));

	//
	// Возвращаем смещение
	//
	return off;
}

/**
 * @brief Зафиксировать tlv-структуру в буфере
 *
 * В данной функции вычисляется реальный размер данных tlv-структуры и
 * эта длина запоминается в соответствующем заголовке (идентифицируемом
 * по его смещению в буфере)
 */
static inline void
tlv_end (struct tbuf* _buf, int _off)
{
	//
	// Указатель на заголовок
	//
	struct tlv* header = (struct tlv*)((u8*)_buf->ptr + _off);

	//
	// Размер данных tlv-структуры равен текущему размеру буфера минус
	// смещение заголовка tlv-структуры и минус размер этого заголовка
	//
	header->len = (u8*)_buf->end - (u8*)_buf->ptr - _off - sizeof (*header);
}

#endif
