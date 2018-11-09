/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
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
#import <tbuf.h>
#import <log_io.h>
#import <pickle.h>
#import <fiber.h>
#import <say.h>

#import <mod/box/common.h>
#import <mod/box/op.h>
#import <mod/box/box.h>
#import <mod/box/print.h>

#include <sysexits.h>

/**
 * @brief Таблица для печати записей
 *
 * По умолчанию выводятся все таблицы
 */
static int snap_space = -1;

/**
 * @brief Формат по умочанию
 */
static const char* fmt_ini = "@";

/**
 * @brief Вспомогательная переменная для анализа формата
 */
static const char* fmt;

/**
 * @brief Проверка необходимости экранирования символа при печати
 */
static int
quote (int _c)
{
	return !((0x20 <= _c) && (_c < 0x7f) && !((_c == '"') || (_c == '\\')));
}

/**
 * @brief Вывести в буфер заданное поле {длина, значение} в соответствии
 *        с форматом
 */
static void
field_print (struct tbuf* _b, void* _f, bool _sep)
{
	//
	// Размер поля для вывода с одновременной перемоткой
	// указателя поля на его данные
	//
	uint32_t size = LOAD_VARINT32 (_f);

	//
	// Символ формата
	//
	char c = *fmt;
	//
	// Если строка формата не полностью обработана, то сдвигаемся к
	// следующему символу, иначе последний символ формата будет работать
	// для всех выводимых далее полей
	//
	if (*(fmt + 1) != '\0')
		++fmt;

	//
	// Если в качестве формата печати поля задан пробел, то данное поле не выводим
	//
	if (c == ' ')
		return;

	//
	// Выводим разделитель если задано
	//
	if (_sep)
		tbuf_append_lit (_b, ", ");

	//
	// В зависимости от формата вывода поля
	//
	switch (c)
	{
		//
		// Вывод целого со знаком
		//
		case 'i':
			//
			// В зависимости от битности числа
			//
			switch (size)
			{
				case 1: // 8 бит
					tbuf_puti (_b, *(i8*)_f);
					break;

				case 2: // 16 бит
					tbuf_puti (_b, *(i16*)_f);
					break;

				case 4: // 32 бита
					tbuf_puti (_b, *(i32*)_f);
					break;

				case 8: // 64 бита
					tbuf_putl (_b, *(i64*)_f);
					break;

				default:
					tbuf_printf (_b, "<invalid int size>");
					break;
			};
			break;

		//
		// Вывод целого без знака
		//
		case 'u':
			//
			// В зависимости от битности числа
			//
			switch (size)
			{
				case 1: // 8 бит
					tbuf_putu (_b, *(u8*)_f);
					break;

				case 2: // 16 бит
					tbuf_putu (_b, *(u16*)_f);
					break;

				case 4: // 32 бита
					tbuf_putu (_b, *(u32*)_f);
					break;

				case 8: // 64 бита
					tbuf_putul (_b, *(u64*)_f);
					break;

				default:
					tbuf_printf (_b, "<invalid int size>");
					break;
			};
			break;

		//
		// Вывод строки с экранированием символов
		//
		case 's':
			tbuf_putc (_b, '"');
			while (size-- > 0)
			{
				if (quote (*(u8*)_f))
				{
					tbuf_append_lit (_b, "\\x");
					tbuf_putx (_b, *(char*)_f++);
				}
				else
				{
					tbuf_putc (_b, *(char*)_f++);
				}
			}
			tbuf_putc (_b, '"');
			break;

		//
		// Вывод данных в шестнадцатеричном представлении
		//
		case 'x':
			tbuf_putxs (_b, _f, size);
			break;

		//
		// Вывод беззнакового 16 или 32 разрядного целого в формате:
		//    <значение числа>:<память под значение в виде строки с экранированием символов>
		//
		// Поля других размеров выводятся как текст с экранированием символов
		//
		case '@':
			if (size == 2)
			{
				tbuf_putu (_b, *(u16*)_f);
				tbuf_putc (_b, ':');
			}
			else if (size == 4)
			{
				tbuf_putu (_b, *(u32*)_f);
				tbuf_putc (_b, ':');
			}

			tbuf_putc (_b, '"');
			while (size-- > 0)
			{
				if (quote (*(u8*)_f))
				{
					tbuf_append_lit (_b, "\\x");
					tbuf_putx (_b, *(char*)_f++);
				}
				else
				{
					tbuf_putc (_b, *(char*)_f++);
				}
			}
			tbuf_putc (_b, '"');
			break;

		default:
			tbuf_printf (_b, "<invalid format symbol '%c'>", (int)c);
			break;
	}
}

/**
 * @brief Вывод полей записи в буфер в соответствии с форматом
 *
 * @param[out] _b буфер для вывода
 * @param[in]  _c количество полей
 * @param[in]  _f указатель на первое поле
 */
static void
tuple_data_print (struct tbuf* _b, u32 _c, void* _f)
{
	fmt = fmt_ini;
	for (size_t i = 0; i < _c; ++i, _f = next_field (_f))
		field_print (_b, _f, i > 0);
}

void
tuple_print (struct tbuf* _b, u32 _c, void* _f)
{
	tbuf_putc (_b, '<');
	tuple_data_print (_b, _c, _f);
	tbuf_putc (_b, '>');
}

/**
 * @brief Печать в буфер записи из xlog
 */
static void
xlog_print (struct tbuf* _out, u16 _op, struct tbuf* _b)
{
	//
	// Флаги операции. Вынесены отдельно для того, чтобы можно было
	// использовать в операциях DELETE, DELETE_1_3
	//
	u32 flags = 0;

	//
	// Код таблицы
	//
	u32 n = read_u32 (_b);

	switch (_op)
	{
		case INSERT:
		{
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			flags           = read_u32(_b);
			u32 cardinality = read_u32(_b);
			u32 data_len    = tbuf_len(_b);
			void* data      = read_bytes (_b, data_len);

			tbuf_printf (_out, "flags:%08X ", flags);
			if (fields_bsize (cardinality, data, data_len) == data_len)
				tuple_print (_out, cardinality, data);
			else
				tbuf_printf (_out, "<CORRUPT TUPLE>");
			break;
		}

		case DELETE:
			flags = read_u32 (_b);
		case DELETE_1_3:
		{
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			u32 key_cardinality = read_u32 (_b);
			u32 key_bsize       = tbuf_len (_b);
			void* key           = read_bytes (_b, key_bsize);

			if (fields_bsize (key_cardinality, key, key_bsize) != key_bsize)
			{
				tbuf_printf(_out, "<CORRUPT KEY>");
				break;
			}

			if (_op == DELETE)
				tbuf_printf (_out, "flags:%08X ", flags);
			tuple_print (_out, key_cardinality, key);
			break;
		}

		case UPDATE_FIELDS:
		{
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			flags               = read_u32 (_b);
			u32 key_cardinality = read_u32 (_b);
			u32 key_bsize       = fields_bsize (key_cardinality, _b->ptr, tbuf_len (_b));
			void* key           = read_bytes (_b, key_bsize);
			u32 op_cnt          = read_u32 (_b);

			tbuf_printf (_out, "flags:%08X ", flags);
			tuple_print (_out, key_cardinality, key);

			while (op_cnt-- > 0)
			{
				u32 field_no = read_u32 (_b);
				u8 op        = read_u8 (_b);
				void* arg    = read_field (_b);

				tbuf_printf (_out, " [field_no:%i op:", field_no);
				switch (op)
				{
					case 0:
						tbuf_printf (_out, "set ");
						break;

					case 1:
						tbuf_printf (_out, "add ");
						break;

					case 2:
						tbuf_printf (_out, "and ");
						break;

					case 3:
						tbuf_printf (_out, "xor ");
						break;

					case 4:
						tbuf_printf (_out, "or ");
						break;

					case 5:
						tbuf_printf (_out, "splice ");
						break;

					case 6:
						tbuf_printf (_out, "delete ");
						break;

					case 7:
						tbuf_printf (_out, "insert ");
						break;

					default:
						tbuf_printf (_out, "CORRUPT_OP:%i", op);
						break;
				}
				tuple_print (_out, 1, arg);
				tbuf_printf (_out, "] ");
			}
			break;
		}

		case NOP:
			tbuf_printf (_out, "NOP");
			break;

		case CREATE_OBJECT_SPACE:
		{
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			struct index_conf ic = {.n = 0};

			flags           = read_u32 (_b);
			u32 cardinality = read_i8 (_b);
			index_conf_read (_b, &ic);

			tbuf_printf (_out, "flags:%08X ", flags);
			tbuf_printf (_out, "cardinalty:%i ", cardinality);
			tbuf_printf (_out, "PK: ");
			index_conf_print (_out, &ic);
			break;
		}

		case CREATE_INDEX:
		{
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			struct index_conf ic = {.n = 0};

			flags = read_u32 (_b);
			ic.n  = read_i8 (_b);
			index_conf_read (_b, &ic);

			tbuf_printf (_out, "flags:%08X ", flags);
			index_conf_print (_out, &ic);
			break;
		}

		case DROP_OBJECT_SPACE:
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			flags = read_u32 (_b);
			tbuf_printf (_out, "flags:%08X ", flags);
			break;

		case DROP_INDEX:
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			flags   = read_u32 (_b);
			u32 idx = read_i8 (_b);

			tbuf_printf (_out, "flags:%08X ", flags);
			tbuf_printf (_out, "index:%i", idx);
			break;

		case TRUNCATE:
			tbuf_printf (_out, "%s n:%i ", box_op_name (_op), n);

			flags = read_u32(_b);
			break;

		default:
			tbuf_printf (_out, "unknown wal op %" PRIi32, _op);
	}

	//
	// Если в буфере остались данные, то выводим их в шестнадцатеричном представлении
	//
	if (tbuf_len (_b) > 0)
		tbuf_printf (_out, ", %i bytes unparsed %s", tbuf_len (_b), tbuf_to_hex (_b));
}

/**
 * @brief Вывод в буфер строки снапшота
 */
static void
snap_print (struct tbuf* _out, struct tbuf* _row)
{
	struct box_snap_row* snap = box_snap_row (_row);

	if (snap_space == -1)
	{
		tbuf_printf (_out, "n:%i ", snap->object_space);
		tuple_print (_out, snap->tuple_size, snap->data);
	}
	else if (snap_space == snap->object_space)
	{
		tuple_data_print (_out, snap->tuple_size, snap->data);
	}
}

/**
 * @brief Вывод в буфер Tag/Len/Value структуры
 */
static void
tlv_print (struct tbuf* _out, struct tlv* _tlv)
{
	switch (_tlv->tag)
	{
		//
		// В случае если это последовательность команд, то данные
		// представляют собой последовательность tlv структур
		//
		case BOX_MULTI_OP:
		{
			//
			// Начало и конец блока данных
			//
			const u8* val = _tlv->val;
			const u8* vnd = _tlv->val + _tlv->len;

			//
			// Префикс мульти-операции
			//
			tbuf_printf (_out, "BOX_MULTY { ");

			//
			// Пока не все вложенные tlv-структуры обработаны
			//
			// ВНИМАНИЕ: используем проверку на <, а не на !=, так как
			//           возможен приход невалидных данных, а способа их
			//           распознать нет
			//
			while (val < vnd)
			{
				//
				// Начало вложенной tlv-структуры
				//
				struct tlv* nested = (struct tlv*)val;

				//
				// Рекурсивно вызываем сами себя для обработки вложенной tlv-структуры
				//
				tlv_print (_out, nested);
				//
				// Выводим разделитель tlv-структур
				//
				tbuf_printf (_out, "; ");

				//
				// Переходим к следующей tlv-структуре
				//
				val += sizeof (*nested) + nested->len;
			}

			//
			// Суффикс мульти-операции
			//
			tbuf_printf (_out, " }");
			break;
		}

		//
		// В случае, если tlv-структура содержит одну команду
		//
		case BOX_OP:
			xlog_print (_out, *(u16*)_tlv->val, &TBUF (_tlv->val + 2, _tlv->len - 2, NULL));
			break;

		default:
			tbuf_printf (_out, "unknown tlv %i", _tlv->tag);
			break;
	}
}

void
box_print_row (struct tbuf* _out, u16 _tag, struct tbuf* _r)
{
	int tag_type = _tag & ~TAG_MASK;
	_tag &= TAG_MASK;

	if (_tag == wal_data)
	{
		u16 op = read_u16 (_r);

		xlog_print (_out, op, _r);
	}

	else if (_tag == tlv)
	{
		while (tbuf_len (_r) > 0)
		{
			//
			// Читаем из буфера заголовок tlv-структуры
			//
			struct tlv* tlv = read_bytes (_r, sizeof (*tlv));
			//
			// Переходим к данным tlv-структуры
			//
			tbuf_ltrim (_r, tlv->len);

			assert (tbuf_len (_r) >= 0);
			tlv_print (_out, tlv);
		}
	}

	else if (tag_type == TAG_WAL)
	{
		u16 op = _tag >> 5;

		xlog_print (_out, op, _r);
	}

	else if (tag_type == TAG_SNAP)
	{
		if (_tag == snap_data)
		{
			snap_print (_out, _r);
		}
		else
		{
			u16 op = _tag >> 5;

			xlog_print (_out, op, _r);
		}
	}
}

const char*
box_row_to_a (u16 _tag, struct tbuf* _data)
{
	@try
	{
		struct tbuf* buf = tbuf_alloc (fiber->pool);
		struct tbuf  tmp = *_data;

		box_print_row (buf, _tag, &tmp);

		return buf->ptr;
	}
	@catch (id e)
	{
		return tbuf_to_hex (_data);
	}
}


@interface BoxPrint : Box<RecoverRow>
{
	i64 stop_scn;
}

-(id) init_stop_scn:(i64)_stop_scn;
-(void) recover_row:(struct row_v12*)_r;
-(void) wal_final_row;
@end

@implementation BoxPrint
-(id)
init_stop_scn:(i64)_stop_scn
{
	[super init];
	stop_scn = _stop_scn;
	return self;
}

-(void)
recover_row:(struct row_v12*)_r
{
	struct tbuf* buf = tbuf_alloc (fiber->pool);

	[self print:_r into:buf];
	puts (buf->ptr);

	if ((_r->scn >= stop_scn) && ((_r->tag & ~TAG_MASK) == TAG_WAL))
		exit (0);
}

-(void)
wal_final_row
{
	say_error ("unable to find record with SCN:%"PRIi64, stop_scn);
	exit (EX_OSFILE);
}
@end

int
box_cat_scn (i64 _stop_scn)
{
	BoxPrint*   printer = [[BoxPrint alloc] init_stop_scn:_stop_scn];
	XLogReader* reader  = [[XLogReader alloc] init_recovery:(id)printer];

	XLog* snap = [snap_dir find_with_scn:_stop_scn shard:0];

	[reader load_full:snap];
	return 0;
}

int
box_cat (const char* _fname)
{
	//
	// Формат вывода записей
	//
	{
		const char* q = getenv ("BOX_CAT_FMT");
		if (q)
			fmt_ini = q;
	}

	//
	// Таблица для вывода
	//
	{
		const char* q = getenv ("BOX_CAT_SNAP_SPACE");
		if (q)
			snap_space = atoi (q);
	}

	read_log (_fname, box_print_row);

	return 0; /* игнорируем результат вызова read_log */
}

register_source ();
