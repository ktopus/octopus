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
#import <util.h>
#import <fiber.h>
#import <objc.h>
#import <tbuf.h>
#import <palloc.h>
#import <pickle.h>
#import <say.h>

#include <stdlib.h>

void __attribute__((noreturn))
tbuf_too_short ()
{
	@throw [Error with_reason:"tbuf too short"];
}

void
read_must_have (struct tbuf* _b, i32 _n)
{
	_read_must_have (_b, _n);
}

void
read_must_end (struct tbuf* _b, const char* _e)
{
	if (unlikely (tbuf_len (_b) != 0))
		@throw [Error with_reason:(_e ?: "tbuf not empty")];
}

u8*
save_varint32 (u8* _p, u32 _v)
{
	if (_v >= (1 << 7))
	{
		if (_v >= (1 << 14))
		{
			if (_v >= (1 << 21))
			{
				if (_v >= (1 << 28))
					*(_p++) = (u8)(_v >> 28) | 0x80;
				*(_p++) = (u8)(_v >> 21) | 0x80;
			}
			*(_p++) = (u8)((_v >> 14) | 0x80);
		}
		*(_p++) = (u8)((_v >> 7) | 0x80);
	}
	*(_p++) = (u8)((_v) & 0x7F);

	return _p;
}

void
write_varint32 (struct tbuf* _b, u32 _v)
{
	//
	// Проверяем, что буфер имеет достаточно памяти для сохранения максимального
	// значения числа. Памяти в буфере не достаточно, то буфер будет переаллоцирован
	// с резервированием достаточного количества памяти
	//
	// Здесь можно было бы использовать varint32_sizeof вместо задания максимально
	// возможного размера, однако это привело бы к дополнительным вычислениям при
	// сомнительной экономии памяти
	//
	tbuf_ensure (_b, 5);

	u8* end = save_varint32 (_b->end, _v);

	_b->free -= end - (u8*)_b->end;
	_b->end = end;
}

static u32
_safe_load_varint32 (struct tbuf* _b)
{
	u32 v = 0;

	u8* p = _b->ptr;
	for (; (p < (u8*)_b->end) && ((p - (u8*)_b->ptr) < 5); v <<= 7, ++p)
	{
		v |= *p & 0x7f;

		if ((*p & 0x80) == 0)
		{
			_b->ptr = p + 1;
			return v;
		}
	}

	if (p == _b->end)
		tbuf_too_short ();

	if ((v > ((u32)0xffffffff >> 7)) || ((*p & 0x80) != 0))
		raise_fmt ("bad varint32");

	v |= *p & 0x7f;
	_b->ptr = p + 1;
	return v;
}

static inline u32
safe_load_varint32 (struct tbuf* _b)
{
	u8* p = _b->ptr;
	if ((_b->end > _b->ptr) && (*p & 0x80) == 0)
	{
		++_b->ptr;
		return *p;
	}

	return _safe_load_varint32 (_b);
}

u32
read_varint32 (struct tbuf* _b)
{
	return safe_load_varint32 (_b);
}

void*
read_field (struct tbuf* _b)
{
	void* p = _b->ptr;

	u32 size = safe_load_varint32 (_b);
	_b->ptr += size;
	if (unlikely (_b->ptr > _b->end))
	{
		_b->ptr = p;
		tbuf_too_short ();
	}

	return p;
}

void*
read_bytes (struct tbuf* _b, u32 _n)
{
	_read_must_have (_b, _n);

	void* p = _b->ptr;
	_b->ptr += _n;
	return p;
}

void
read_to (struct tbuf* _b, void* _p, u32 _n)
{
	_read_to (_b, _p, _n);
}

void*
read_ptr (struct tbuf* _b)
{
	return *(void**)read_bytes (_b, sizeof (void*));
}

/**
 * @brief Шаблон функций чтения из буфера беззнаковых целых разной
 *        битности записанных в виде пар {длина, значение}
 */
#define read_field_u(bits)                       \
	u##bits read_field_u##bits (struct tbuf* _b) \
	{                                            \
		_read_must_have (_b, bits/8 + 1);        \
		if (unlikely (*(u8*)_b->ptr != bits/8))  \
			raise_fmt ("bad field");             \
		u##bits r = *(u##bits*)(_b->ptr + 1);    \
		_b->ptr += bits/8 + 1;                   \
		return r;                                \
	}

/**
 * @brief Шаблон функций чтения из буфера целых со знаком разной
 *        битности записанных в виде пар {длина, значение}
 */
#define read_field_i(bits)                      \
	i##bits read_field_i##bits(struct tbuf* _b) \
	{                                           \
		_read_must_have (_b, bits/8 + 1);       \
		if (unlikely (*(u8*)_b->ptr != bits/8)) \
			raise_fmt ("bad field");            \
		i##bits r = *(i##bits*)(_b->ptr + 1);   \
		_b->ptr += bits/8 + 1;                  \
		return r;                               \
	}

read_field_u(8)
read_field_u(16)
read_field_u(32)
read_field_u(64)
read_field_i(8)
read_field_i(16)
read_field_i(32)
read_field_i(64)

struct tbuf*
read_field_s (struct tbuf* _b)
{
	void* p = _b->ptr;

	u32 n = safe_load_varint32 (_b);
	if ((_b->end - _b->ptr) < n)
	{
		_b->ptr = p;
		tbuf_too_short ();
	}

	return tbuf_split (_b, n);
}

size_t
varint32_sizeof (u32 _v)
{
	int s = 1;

	while (_v >= (1 << 7))
	{
		_v >>= 7;
		++s;
	}

	return s;
}

u32
_load_varint32 (void** _pp)
{
	u32 v = 0;

	u8* p = *_pp;
	do
		v = (v << 7) | (*p & 0x7f);
	while ((*p++ & 0x80) && ((p - (u8 *)*_pp) < 5));
	*_pp = p;

	return v;
}

/**
 * @brief Шаблон функций записи в буфер целого без знака
 */
#define write_u(bits)                                \
	void write_u##bits (struct tbuf* _b, u##bits _v) \
	{                                                \
		tbuf_ensure (_b, bits/8);                    \
		*(u##bits*)_b->end = _v;                     \
		_b->end  += bits/8;                          \
		_b->free -= bits/8;                          \
	}

/**
 * @brief Шаблон функций записи в буфер целого со знаком
 */
#define write_i(bits)                                \
	void write_i##bits (struct tbuf* _b, i##bits _v) \
	{                                                \
		tbuf_ensure (_b, bits/8);                    \
		*(i##bits*)_b->end = _v;                     \
		_b->end  += bits/8;                          \
		_b->free -= bits/8;                          \
	}

/**
 * @brief Шаблон функции записи в буфер целого со знаком в виде
 *        пары {длина, значение}
 */
#define write_field_i(bits)                                \
	void write_field_i##bits (struct tbuf* _b, i##bits _v) \
	{                                                      \
		tbuf_ensure (_b, bits/8 + 1);                      \
		*(u8*)_b->end = bits/8;                            \
		*(i##bits*)(_b->end + 1) = _v;                     \
		_b->end  += bits/8 + 1;                            \
		_b->free -= bits/8 + 1;                            \
	}

write_u(8)
write_u(16)
write_u(32)
write_u(64)
write_i(8)
write_i(16)
write_i(32)
write_i(64)
write_field_i(8)
write_field_i(16)
write_field_i(32)
write_field_i(64)

void
write_field_s (struct tbuf* _b, const u8* _s, u32 _n)
{
	write_varint32 (_b, _n);
	tbuf_append (_b, _s, _n);
}

register_source ();
