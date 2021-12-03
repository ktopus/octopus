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
#import <say.h>

#import <mod/box/tuple.h>
#import <mod/box/tuple_index.h>

/**
 * @brief Конструктор узла индекса по заданному полю записи типа 32х битное целое
 *
 * @param _obj объект, который индексируется
 * @param _node конструируемый узел индекса
 * @param _arg номер поля записи, по которому выполняется индексирование
 */
static struct index_node*
box_tuple_u32_dtor (struct tnt_object* _obj, struct index_node* _node, void* _arg)
{
	//
	// Номер поля записи. Хранится в указателе
	//
	int n = (uintptr_t)_arg;

	//
	// Получаем указатель на индексируемое поле и проверяем его валидность
	//
	u8* f = tuple_field (_obj, n);
	if (f == NULL)
		index_raise ("cardinality too small");
	if (*f != sizeof (u32))
		index_raise ("expected u32");

	//
	// Проиндексированный объект
	//
	_node->obj = _obj;

	//
	// Устанавливаем данные индекса
	//
	_node->key.u32 = *(u32*)(f + 1);

	return _node;
}

/**
 * @brief Конструктор узла индекса по заданному полю записи типа 64х битное целое
 *
 * @param _obj объект, который индексируется
 * @param _node конструируемый узел индекса
 * @param _arg номер поля записи, по которому выполняется индексирование
 */
static struct index_node*
box_tuple_u64_dtor (struct tnt_object* _obj, struct index_node* _node, void* _arg)
{
	//
	// Номер поля записи. Хранится в указателе
	//
	int n = (uintptr_t)_arg;

	//
	// Получаем указатель на индексируемое поле и проверяем его валидность
	//
	u8* f = tuple_field (_obj, n);
	if (f == NULL)
		index_raise ("cardinality too small");
	if (*f != sizeof (u64))
		index_raise ("expected u64");

	//
	// Проиндексированный объект
	//
	_node->obj = _obj;

	//
	// Устанавливаем данные индекса
	//
	_node->key.u64 = *(u64*)(f + 1);

	return _node;
}

/**
 * @brief Конструктор узла индекса по заданному полю записи типа строка
 *
 * @param _obj объект, который индексируется
 * @param _node конструируемый узел индекса
 * @param _arg номер поля записи, по которому выполняется индексирование
 */
static struct index_node*
box_tuple_lstr_dtor (struct tnt_object* _obj, struct index_node* _node, void* _arg)
{
	//
	// Номер поля записи. Хранится в указателе
	//
	int n = (uintptr_t)_arg;

	//
	// Получаем указатель на индексируемое поле и проверяем его валидность
	//
	u8* f = tuple_field (_obj, n);
	if (f == NULL)
		index_raise ("cardinality too small");
	size_t size = LOAD_VARINT32 (f);
	if (size > 0xffff)
		index_raise ("string key too long");

	//
	// Проиндексированный объект
	//
	_node->obj = _obj;

	//
	// Устанавливаем данные индекса
	//
	set_lstr_field (&_node->key, size, f);

	return _node;
}

/**
 * @brief Конструктор узла индекса по общему описанию
 *
 * @param _obj объект, который индексируется
 * @param _node конструируемый узел индекса
 * @param _arg общее описание индекса
 */
static struct index_node*
box_tuple_gen_dtor (struct tnt_object* _obj, struct index_node* _node, void* _arg)
{
	//
	// Общее описание индекса
	//
	struct index_conf* desc = (struct index_conf*)_arg;

	//
	// Проиндексированный объект
	//
	_node->obj = _obj;

	//
	// Если проиндексировано одно поле
	//
	if (desc->cardinality == 1)
	{
		//
		// Получаем указатель на индексируемое поле и проверяем его валидность
		//
		u8* f = tuple_field (_obj, desc->field[0].index);
		if (f == NULL)
			index_raise ("cardinality too small");

		//
		// Получаем размер индексируемого поля и переходим к данным поля
		//
		u32 len = LOAD_VARINT32 (f);

		//
		// Устанавливаем данные индекса
		//
		gen_set_field (&_node->key, desc->field[0].type, len, f);

		return _node;
	}

	//
	// Если количество полей записи меньше минимального количества
	// полей записи для индексации
	//
	if (tuple_cardinality (_obj) < desc->min_tuple_cardinality)
		index_raise ("cardinality too small");

	//
	// Данные записи
	//
	u8* data = tuple_data (_obj);

	//
	// Проходим по всем полям записи пока не будут записаны в узел все
	// индексируемые поля
	//
	// Для построения сложного индекса используется специальный маппинг,
	// который позволяет построить описание узла индекса при последовательном
	// проходе полей записи. Это сделано для ускорения построения узла индекса,
	// так как поля в индексе могут быть указаны не в том порядке, в котором
	// они встречаются в записи
	//
	for (int i = 0, j = 0; (i < desc->cardinality) && (j < tuple_cardinality (_obj)); ++j)
	{
		//
		// Получаем размер поля и переходим к началу собственно данных поля
		//
		u32 len = LOAD_VARINT32 (data);

		//
		// Описание индексируемого поля
		//
		struct index_field_desc* field = &desc->field[(int)desc->fill_order[i]];

		//
		// Если найдено поле, включённое в индекс
		//
		//
		if (j == field->index)
		{
			//
			// Заполняем данные узла индекса
			//
			gen_set_field ((void*)&_node->key + field->offset, field->type, len, data);

			//
			// Переходим к описанию следующего индексируемого поля
			//
			++i;
		}

		//
		// Переходим к следующему полю записи
		//
		data += len;
	}

	return (struct index_node*)_node;
}

struct dtor_conf*
box_tuple_dtor (void)
{
	static struct dtor_conf dtor =
	{
		.u32     = box_tuple_u32_dtor,
		.u64     = box_tuple_u64_dtor,
		.lstr    = box_tuple_lstr_dtor,
		.generic = box_tuple_gen_dtor
	};

	return &dtor;
}

/**
 * @brief Описание типа состоящее из его кода и вариантов
 *        текстового представления
 */
typedef struct
{
	/**
	 * @brief Код типа
	 */
	int type;

	/**
	 * @brief Массив вариантов текстового представления типа
	 */
	char** name;
} typenames;

/**
 * @brief Определение типов для одноколоночных индексов
 */
static typenames* one_column_types = (typenames[])
{
	{SNUM32, (char*[]){"NUM", "SNUM", "NUM32", "SNUM32", NULL}},
	{SNUM64, (char*[]){"NUM64", "SNUM64", NULL}},
	{STRING, (char*[]){"STR", "STRING", NULL}},
	{UNUM32, (char*[]){"UNUM", "UNUM32", NULL}},
	{UNUM64, (char*[]){"UNUM64", NULL}},
	{SNUM16, (char*[]){"NUM16", "SNUM16", NULL}},
	{UNUM16, (char*[]){"UNUM16", NULL}},
	{SNUM8 , (char*[]){"NUM8", "SNUM8", NULL}},
	{UNUM8 , (char*[]){"UNUM8", NULL}},
	{UNDEF , (char*[]){NULL}}
};

/**
 * @brief Определение типов для многоколоночных индексов
 */
static typenames* many_column_types = (typenames[])
{
	{UNUM32, (char*[]){"NUM", "UNUM", "NUM32", "UNUM32", NULL}},
	{UNUM64, (char*[]){"NUM64", "UNUM64", NULL}},
	{STRING, (char*[]){"STR", "STRING", NULL}},
	{SNUM32, (char*[]){"SNUM", "SNUM32", NULL}},
	{SNUM64, (char*[]){"SNUM64", NULL}},
	{UNUM16, (char*[]){"NUM16", "UNUM16", NULL}},
	{SNUM16, (char*[]){"SNUM16", NULL}},
	{UNUM16, (char*[]){"NUM8", "UNUM8", NULL}},
	{SNUM16, (char*[]){"SNUM8", NULL}},
	{UNDEF , (char*[]){NULL}},
};

extern void out_warning (int _v, char* _format, ...);

/**
 * @brief Проверка эквивалентности строк
 */
#define eq(_s1, _s2) (strcmp ((_s1), (_s2)) == 0)

/**
 * @brief Реакция на ошибку в зависимости от значения параметра @a _do_panic
 *
 * Только для использования в функции cfg_box2index_conf, так как завязана на
 * имена передаваемых в неё параметров
 */
#define exception(_fmt, ...)                                                       \
	do                                                                             \
	{                                                                              \
		if (_do_panic)                                                             \
		{                                                                          \
			panic ("space %d index %d " _fmt, _sno, _ino, ##__VA_ARGS__);          \
		}                                                                          \
		else                                                                       \
		{                                                                          \
			out_warning (0, "space %d index %d " _fmt, _sno, _ino, ##__VA_ARGS__); \
			return NULL;                                                           \
		}                                                                          \
	}                                                                              \
	while (0)

struct index_conf*
cfg_box2index_conf (struct octopus_cfg_object_space_index* _c, int _sno, int _ino, int _do_panic)
{
	//
	// Распределения папяти под конфигурацию и её начальная инициализация
	//
	struct index_conf* d = calloc (1, sizeof (*d));
	for (int i = 0; i < nelem (d->field); ++i)
		d->field[i].index = d->fill_order[i] = d->field[i].offset = -1;

	d->unique  = _c->unique;
	d->relaxed = _c->relaxed;

	if (eq (_c->type, "HASH"))
		d->type = HASH;
	else if (eq (_c->type, "NUMHASH"))
		d->type = NUMHASH;
	else if (eq (_c->type, "TREE"))
		d->type = COMPACTTREE;
	else if (eq (_c->type, "FASTTREE"))
		d->type = FASTTREE;
	else if (eq (_c->type, "SPTREE"))
		d->type = SPTREE;
	else if (eq (_c->type, "POSTREE"))
		d->type = POSTREE;
	else if (eq (_c->type, "HUGEHASH"))
		d->type = PHASH;
	else
		exception ("unknown index type");

	//
	// Хэш индексы могут быть только уникальными
	//
	if (!d->unique && ((d->type == HASH) || (d->type == NUMHASH) || (d->type == PHASH)))
		exception ("hash index should be unique");

	//
	// Проверяем конфигурацию полей индекса с подсчётом количества полей в индексе
	//
	for (d->cardinality = 0; _c->key_field[(int)d->cardinality] != NULL; ++d->cardinality)
	{
		__typeof__ (_c->key_field[0]) key_field = _c->key_field[(int)d->cardinality];

		if (key_field->fieldno == -1)
			exception ("key %d fieldno should be set", d->cardinality);
		if (key_field->fieldno > 255)
			exception ("key %d fieldno must be between 0 and 255", d->cardinality);
		if (!eq (key_field->sort_order, "ASC") && !eq (key_field->sort_order, "DESC"))
			exception ("key %d unknown sort order", d->cardinality);
		if (d->cardinality > nelem(d->field))
			exception ("key %d index cardinality is too big", d->cardinality);
	}

	//
	// Поля индекса не заданы
	//
	if (d->cardinality == 0)
		exception ("index cardinality is 0");

	for (int k = 0; k < d->cardinality; ++k)
	{
		__typeof__ (_c->key_field[0]) key_field = _c->key_field[k];

		d->fill_order[k]       = k;
		d->field[k].index      = key_field->fieldno;
		d->field[k].sort_order = eq (key_field->sort_order, "ASC") ? ASC : DESC;

		//
		// Преобразуем текстовое представление типа поля индекса в его код
		//
		int type = UNDEF;
		for (typenames* names = (d->cardinality == 1) ? one_column_types : many_column_types; (type == UNDEF) && (names->type != UNDEF); ++names)
		{
			//
			// Проверяем варианты написания типа
			//
			for (char** name = names->name; *name != NULL; ++name)
			{
				//
				// Если один из вариантов написания типа совпал с написанием заданного типа
				//
				if (eq (key_field->type, *name))
				{
					type = names->type;
					break;
				}
			}
		}

		if (type == UNDEF)
			exception ("key %d unknown field data type: `%s'", k, key_field->type);

		//
		// Запоминаем код типа в конфигурации индекса
		//
		d->field[k].type = type;

		//
		// Корректируем минимально допустимое количество полей в записи (количество
		// полей в записи не может быть меньше, чем максимальная позиция индексируемого
		// поля плюс 1, иначе будет невозможно построить индекс)
		//
		if ((key_field->fieldno + 1) > d->min_tuple_cardinality)
			d->min_tuple_cardinality = key_field->fieldno + 1;
	}

	//
	// Сортируем поля
	//
	index_conf_sort_fields (d);

	return d;
}

register_source ();
