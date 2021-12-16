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
#import <objc.h>
#import <assert.h>

#import <salloc.h>
#import <say.h>
#import <net_io.h>

#import <mod/box/common.h>
#import <mod/box/tuple.h>

/**
 * @brief Заглушка для настройки пула распределителя памяти
 *
 * Для распределения памяти под эти два типа объектов используем один
 * и тот же пул по объекту наибольшего размера. Это позволяет уменьшить
 * фрагментацию памяти мелкими объектами
 */
union box_phi_union
{
	struct box_phi phi;
	struct box_phi_cell cell;
};

/**
 * @brief slab-кэш для распределения объектов box_phi и box_phi_cell
 */
static struct slab_cache g_phi_cache;

void __attribute__((noreturn))
bad_object_type (void)
{
	raise_fmt ("bad object type");
}

struct tnt_object*
tuple_alloc (unsigned _cardinality, unsigned _size)
{
	struct tnt_object* obj = NULL;

	//
	// Если данные объекта могут быть упакованы компактно, то создаём
	// компактный объект
	//
	if ((_cardinality < 256) && (_size < 256))
	{
		obj = object_alloc (BOX_SMALL_TUPLE, 0, sizeof (struct box_small_tuple) + _size);

		struct box_small_tuple* tuple = box_small_tuple (obj);
		tuple->bsize       = _size;
		tuple->cardinality = _cardinality;
	}
	//
	// Иначе создаём обычный объект
	//
	else
	{
		obj = object_alloc (BOX_TUPLE, 1, sizeof (struct box_tuple) + _size);
		object_incr_ref (obj);

		struct box_tuple* tuple = box_tuple (obj);
		tuple->bsize       = _size;
		tuple->cardinality = _cardinality;
	}

	say_debug3 ("%s (%u, %u) = %p", __func__, _cardinality, _size, obj->data);
	return obj;
}

void
tuple_free (struct tnt_object* _obj)
{
	say_debug ("%s (%p) of type (%i)", __func__, _obj, _obj->type);

	switch (_obj->type)
	{
		case BOX_SMALL_TUPLE:
			sfree (_obj);
			break;

		case BOX_TUPLE:
			object_decr_ref (_obj);
			break;

		default:
			assert (false);
	}
}

int
tuple_valid (struct tnt_object* _obj)
{
	@try
	{
		//
		// Явно заданный размер объекта должен совпадать с вычисленным по его полям
		//
		return fields_bsize (tuple_cardinality (_obj), tuple_data (_obj), tuple_bsize (_obj)) == tuple_bsize (_obj);
	}
	@catch (Error* e)
	{
		say_error ("%s", e->reason);
		[e release];
		return 0;
	}
}

int
tuple_bsize (const struct tnt_object* _obj)
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

int
tuple_cardinality (const struct tnt_object* _obj)
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
			bad_object_type ();
	}
}

void*
tuple_data (struct tnt_object* _obj)
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

void*
next_field (void* _f)
{
	//
	// Размер поля. Одновременно указатель _f смещается на начало
	// данных поля
	//
	u32 size = LOAD_VARINT32 (_f);

	//
	// Следующее поле объекта
	//
	return (u8*)_f + size;
}

ssize_t
fields_bsize (u32 _cardinality, const void* _data, u32 _len)
{
	//
	// Буфер для чтения данных из объекта
	//
	struct tbuf tmp = TBUF (_data, _len, NULL);

	//
	// Читаем все поля объекта
	//
	for (int i = 0; i < _cardinality; ++i)
	{
		void* p = read_field (&tmp);
		say_debug ("read_filed %d with size = %zd and data = %s", i, tmp.ptr - p, dump (p, tmp.ptr - p));
	}

	//
	// Возвращаем реальный размер объекта, посчитанный по его полям
	//
	return tmp.ptr - _data;
}

void*
tuple_field (struct tnt_object* _obj, size_t _i)
{
	//
	// Проверяем, что индекс поля меньше числа полей объекта
	//
	if (_i >= tuple_cardinality (_obj))
		return NULL;

	//
	// Первое поле объекта
	//
	void* f = tuple_data (_obj);

	//
	// Последовательно идём по всем полям пока не дойдём до поля
	// с заданным индексом
	//
	while (_i-- > 0)
		f = next_field (f);

	//
	// Возвращаем указатель на заданное поле
	//
	return f;
}

void
net_tuple_add (struct netmsg_head* _h, struct tnt_object* _obj)
{
	switch (_obj->type)
	{
		case BOX_SMALL_TUPLE:
		{
			struct box_small_tuple* small_tuple = box_small_tuple (_obj);

			//
			// Компактный объект передаётся как обычный объект, чтобы не
			// усложнять протокол. Память под него распределяется в пуле
			// буфера и будет удалена вместе с ним
			//
			struct box_tuple* tuple = net_add_alloc (_h, sizeof (struct box_tuple) + small_tuple->bsize);
			tuple->bsize       = small_tuple->bsize;
			tuple->cardinality = small_tuple->cardinality;
			memcpy (tuple->data, small_tuple->data, small_tuple->bsize);
			break;
		}

		case BOX_TUPLE:
		{
			//
			// Объект для вывода в буфер
			//
			struct box_tuple* tuple = box_tuple (_obj);

			//
			// Полный размер объекта вместе с заголовком
			//
			size_t size = sizeof (struct box_tuple) + tuple->bsize;

			//
			// Вывод объекта в буфер
			//
			net_add_obj_iov (_h, _obj, tuple, size);
			break;
		}

		case BOX_PHI:
			//
			// Структура box_phi является внутренней и на сторону клиента не
			// передаётся
			//
			assert (false);

		default:
			bad_object_type ();
	}
}

struct box_phi*
phi_alloc (Index<BasicIndex>* _index, struct tnt_object* _obj, struct box_op* _bop)
{
	//
	// Получаем блок памяти из фиксированного slab-кэша
	//
	struct box_phi* head = slab_cache_alloc (&g_phi_cache);
	//
	// ... и инициализируем его
	//
	bzero (head, sizeof (struct box_phi));

	//
	// Заполняем поля
	//
	head->header.type = BOX_PHI;
	head->index       = _index;
	head->obj         = _obj;
	head->bop         = _bop;
	TAILQ_INIT (&head->tailq);

	say_debug3 ("%s: head:%p index:%d obj:%p TAILQ_FIRST(&index_obj->tailq):%p",
				__func__, head, _index->conf.n, _obj, TAILQ_FIRST(&head->tailq));
	return head;
}

struct box_phi_cell*
phi_cell_alloc (struct box_phi* _index_obj, struct tnt_object* _obj, struct box_op* _bop)
{
	//
	// Получаем блок памяти из фиксированного slab-кэша
	//
	struct box_phi_cell* cell = slab_cache_alloc (&g_phi_cache);
	//
	// ... и инициализируем его
	//
	bzero (cell, sizeof (struct box_phi_cell));

	//
	// Заполняем поля
	//
	cell->head = _index_obj;
	cell->obj  = _obj;
	cell->bop  = _bop;

	//
	// Добавляем запись об изменении в список изменений объекта в индексе
	//
	TAILQ_INSERT_TAIL (&_index_obj->tailq, cell, link);

	say_debug3 ("%s: index:%d _index_obj:%p cell:%p TAILQ_FIRST(&index_obj->tailq):%p obj:%p",
				__func__, cell->head->index->conf.n, cell->head, cell, TAILQ_FIRST (&cell->head->tailq), cell->obj);
	return cell;
}

void
phi_free (struct box_phi* _phi)
{
	sfree (_phi);
}

void
phi_cell_free (struct box_phi_cell* _cell)
{
	sfree (_cell);
}

struct tnt_object*
phi_obj (const struct tnt_object* _obj)
{
	assert (_obj->type == BOX_PHI);

	struct box_phi* phi = box_phi (_obj);

	return phi->obj ? phi->obj : TAILQ_FIRST (&phi->tailq)->obj;
}

struct tnt_object*
phi_left (struct tnt_object* _obj)
{
	if (_obj && (_obj->type == BOX_PHI))
		_obj = box_phi (_obj)->obj;

	assert ((_obj == NULL) || (_obj->type != BOX_PHI));
	return _obj;
}

struct tnt_object*
phi_right (struct tnt_object* _obj)
{
	if (_obj && (_obj->type == BOX_PHI))
		_obj = TAILQ_LAST (&box_phi (_obj)->tailq, phi_tailq)->obj;

	assert ((_obj == NULL) || (_obj->type != BOX_PHI));
	return _obj;
}

struct tnt_object*
tuple_visible_left (struct tnt_object* _obj)
{
	return phi_left (_obj);
}

struct tnt_object*
tuple_visible_right (struct tnt_object* _obj)
{
	return phi_right (_obj);
}

bool
tuple_match (struct index_conf *_ic, struct tnt_object *_obj)
{
	say_debug ("%s: index:%d relaxed:%d _obj:%p", __func__, _ic->n, _ic->relaxed, _obj);
	if (_ic->relaxed > 0)
	{
		for (int f = 0; f < _ic->cardinality; ++f)
		{
			u8* fdata = tuple_field (_obj, _ic->field[f].index);
			if (!fdata)
				return false;

			u32 len = LOAD_VARINT32 (fdata);
			if (len == 0)
				return false;
		}
	}

	return true;
}

void
phi_cache_init (void)
{
	slab_cache_init (&g_phi_cache, sizeof (union box_phi_union), SLAB_GROW, "phi_cache");
}

register_source ();
