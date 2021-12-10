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
#import <util.h>
#import <fiber.h>
#import <iproto.h>
#import <log_io.h>
#import <net_io.h>
#import <pickle.h>
#import <salloc.h>
#import <say.h>
#import <stat.h>
#import <octopus.h>
#import <tbuf.h>
#import <objc.h>
#import <index.h>
#import <shard.h>

#import <log_io.h>

#import <mod/box/meta_op.h>
#import <mod/box/box.h>
#if CFG_lua_path
#import <mod/box/src-lua/moonbox.h>
#endif
#if CFG_caml_path
#import <mod/box/src-ml/camlbox.h>
#endif

#include <stdint.h>

#import <mod/box/op.h>

/**
 * @brief Вставить запись об изменении объекта в индекс
 *
 * @param _bop текущая операция
 * @param _index индекс, в который вставляется объект
 * @param _index_obj указатель на замещаемый объект в индексе (может указывать
 *                   либо на собственно замещаемый объект либо на список версий
 *                   объекта)
 * @param _obj новая версия объекта для вставки в индекс
 */
static void
phi_insert (struct box_op* _bop, Index<BasicIndex>* _index, struct tnt_object* _index_obj, struct tnt_object* _obj)
{
	assert ((_index_obj != NULL) || (_obj != NULL));
	assert ((_obj == NULL) || (_obj->type != BOX_PHI));

	//
	// Созданная запись об изменении
	//
	struct box_phi_cell* cell = NULL;

	//
	// В случае, если замещаемый объект представляет собой список
	// версий объекта
	//
	if (_index_obj && (_index_obj->type == BOX_PHI))
	{
		//
		// Список версий объекта
		//
		struct box_phi* index_obj = box_phi (_index_obj);

		//
		// Проверяем согласованность индекса и находящегося в нём объекта
		//
		assert (index_obj->index == _index);

		//
		// Добавляем в индекс запись о новой версии объекта
		//
		cell = phi_cell_alloc (index_obj, _obj, _bop);
	}
	else
	{
		//
		// Создаём список версий объекта
		//
		// Это первая операция над индексом в транзакции, поэтому в индексе
		// пока находится обычный объект
		//
		struct box_phi* index_obj = phi_alloc (_index, _index_obj, _bop);

		//
		// Добавляем в него запись о новой версии объекта
		//
		cell = phi_cell_alloc (index_obj, _obj, _bop);

		//
		// Замещаем объект в индексе списком его версий
		//
		@try
		{
			[_index replace:&index_obj->header];
		}
		@catch (id e)
		{
			phi_cell_free (cell);
			phi_free (index_obj);
			@throw;
		}
	}

	//
	// Добавляем информацию о новой версии объекта в список версий объекта для
	// для индексов, изменённых данной операцией
	//
	say_debug3 ("%s(0): cell:%p index:%d index_obj:%p TAILQ_FIRST(&index_obj->tailq):%p obj:%p",
				__func__, cell, cell->head->index->conf.n, cell->head, TAILQ_FIRST (&cell->head->tailq), cell->obj);
	TAILQ_INSERT_TAIL (&_bop->phi, cell, bop_link);
	say_debug3 ("%s(1): cell:%p index:%d index_obj:%p TAILQ_FIRST(&index_obj->tailq):%p obj:%p",
				__func__, cell, cell->head->index->conf.n, cell->head, TAILQ_FIRST (&cell->head->tailq), cell->obj);
}

/**
 * @brief Зафиксировать заданную версию объекта в индексе
 */
static void
phi_commit (struct box_phi_cell* _cell)
{
	say_debug3 ("%s: cell:%p index:%d index_obj:%p TAILQ_FIRST(&index_obj->tailq):%p obj:%p",
				__func__, _cell, _cell->head->index->conf.n, _cell->head, TAILQ_FIRST (&_cell->head->tailq), _cell->obj);

	//
	// Список версий объекта, в котором находится данная версия
	//
	struct box_phi* index_obj = _cell->head;

	//
	// Проверяем, что данная версия является самой первой в списке версий
	//
	// Коммитить версии из середины списка нельзя
	//
	assert (_cell == TAILQ_FIRST (&index_obj->tailq));
	//
	// Объект должен существовать либо до либо после либо и до и после операции
	//
	// Нельзя два раза подряд удалить один и тот же объект
	//
	assert ((index_obj->obj != NULL) || (_cell->obj != NULL));

	//
	// Если это последняя версия объекта в списке
	//
	if (_cell == TAILQ_LAST (&index_obj->tailq, phi_tailq))
	{
		//
		// Если операция удалила объект из индекса, то удаляем весь список
		// версий из индекса
		//
		if (_cell->obj == NULL)
			[index_obj->index remove:&index_obj->header];
		//
		// Если операция добавила или обновила объект, то замещаем список
		// версий в индексе финальной версией объекта
		//
		else
			[index_obj->index replace:_cell->obj];

		//
		// Удаляем список версий объекта в индексе из памяти, он больше не нужен
		//
		// Саму версию объекта из памяти не удаляем, так как она принадлежит операции
		// и будет удалена вместе с ней. Из списка версий объекта в индексе её тоже не
		// удаляем, так как удаляется сам список
		//
		phi_free (index_obj);
	}
	//
	// Если это не последняя версия объекта в индексе
	//
	else
	{
		//
		// Объект должен существовать либо после данной операции либо после
		// последующей операции либо и после них обеих (нельзя два раза подряд
		// удалить один и тот же объект)
		//
		assert ((_cell->obj != NULL) || (TAILQ_NEXT (_cell, link)->obj != NULL));

		//
		// Фиксируем версию объекта как текущую актуальную для всей транзакции
		// для данного индекса
		//
		// Саму версию объекта из памяти не удаляем, так как она принадлежит операции
		// и будет удалена вместе с ней
		//
		index_obj->obj = _cell->obj;

		//
		// Удаляем версию объекта из списка версий объекта в индексе
		//
		TAILQ_REMOVE (&index_obj->tailq, _cell, link);
	}
}

/**
 * @brief Отменить заданную версию объекта в индексе
 */
static void
phi_rollback (struct box_phi_cell* _cell)
{
	say_debug3 ("%s: cell:%p index:%d index_obj:%p obj:%p", __func__, _cell, _cell->head->index->conf.n, _cell->head, _cell->obj);

	//
	// Список версий объекта в индексе, в котором находится данная версий
	//
	struct box_phi* index_phi = _cell->head;

	//
	// Проверяем, что данная версия является самой последней в списке
	//
	// Отменять версии из середины списка нельзя
	//
	assert (_cell == TAILQ_LAST (&index_phi->tailq, phi_tailq));

	//
	// Если это не первая версия объекта в списке (то есть в списке
	// присутствуют ещё версии)
	//
	if (_cell != TAILQ_FIRST (&index_phi->tailq))
	{
		//
		// В списке присутствует удаление два раза подряд одного и того же объекта.
		// Такого быть не должно, это ошибка кода, поэтому ассертимся
		//
		assert ((TAILQ_PREV (_cell, phi_tailq, link)->obj != NULL) || (_cell->obj != NULL));

		//
		// Просто удаляем версию объекта из списка делая актуальным предущее изменение.
		// Саму версию из памяти не удаляем, так как она принадлежит операции и будет
		// удалена вместе с ней
		//
		TAILQ_REMOVE (&index_phi->tailq, _cell, link);
	}
	//
	// Если это последняя версия объекта в списке
	//
	else
	{
		//
		// Последней версией объекта в списке присутствует операция удаления объекта
		// которого не было до начала транзакции. Такого быть не должно, это ошибка
		// кода, поэтому ассертимся
		//
		assert ((index_phi->obj != NULL) || (_cell->obj != NULL));

		//
		// Если до начала транзакции объекта не существовало, то просто удаляем список
		// версий объекта из индекса
		//
		say_debug ("index_phi = %p, index_phi->header = %p", index_phi, &index_phi->header);
		if (index_phi->obj == NULL)
			[index_phi->index remove:&index_phi->header];
		//
		// Если объект существовал до начала транзакции, то вставляем его изначальную
		// версию в индекс вместо списка версий
		//
		else
			[index_phi->index replace:index_phi->obj];

		//
		// Удаляем список изменений из памяти, он больше не нужен
		//
		// Саму запись об изменении из памяти не удаляем, так как она
		// принадлежит операции. Из списка её тоже не удаляем, так как
		// удаляется сам список
		//
		phi_free (index_phi);
	}
}

/**
 * @brief Удалить запись из таблицы
 */
static void
object_space_delete (struct box_op* _bop, struct tnt_object* _pk_obj, struct tnt_object* _obj)
{
	if (_obj == NULL)
		return;

	//
	// Модифицируемая таблица
	//
	struct object_space* osp = _bop->object_space;

	//
	// Добавляем изменение с удалением объекта в первичный индекс
	//
	// Первичный индекс не может быть частичным, так как он должен
	// содержать все объекты таблицы
	//
	phi_insert (_bop, osp->index[0], _pk_obj, NULL);

	//
	// Проходим по всем остальным индексам
	//
	foreach_indexi (1, index, osp)
	{
		//
		// Должен ли объект находиться в индексе
		//
		bool m = tuple_match (&index->conf, _obj);

		//
		// Ищем объект в индексе (в случае частичного индекса объекта может в
		// индексе и не быть)
		//
		struct tnt_object* index_obj = [index find_obj:_obj];

		//
		// Проверяем нахождение удаляемого объекта в индексе в зависимости
		// от того, подходит ли он под условие включения в индекс или нет
		//
		assert ((m && (phi_right (index_obj) == _obj)) || (!m && (phi_right (index_obj) == NULL)));

		//
		// Если объект находится в индексе, то исключаем его оттуда
		//
		// Здесь если по каким-то причинам объекта в индексе не было или он
		// был оттуда уже удалён при выполнении одной из предыдущих операций,
		// то повторное исключение объекта из индекса не делаем (то, что в
		// индексе не должно быть двух операций исключения объекта из индекса
		// подряд проверяется при подтверждении или откате тразнакции)
		//
		if (phi_right (index_obj))
			phi_insert (_bop, index, index_obj, NULL);
	}
}

/**
 * @brief Добавить объект в таблицу
 *
 * @param[in] _bop выполняемая операция
 * @param[in] _pk_obj объект в первичном индексе, который модифицируется операцией
 * @param[in] _obj вставляемый объект
 */
static void
object_space_insert (struct box_op* _bop, struct tnt_object* _pk_obj, struct tnt_object* _obj)
{
	//
	// Последняя версия объекта в первичном индексе должна быть пустой,
	// однако цепочка версий может присутствовать
	//
	assert (phi_right (_pk_obj) == NULL);

	//
	// Модифицируемая таблица
	//
	struct object_space* osp = _bop->object_space;

	//
	// Добавляем новую версию объекта в первичный индекс
	//
	// Первичный индекс не может быть частичным, так как он должен содержать
	// все объекты таблицы
	//
	// Проверку на наличие объекта с таким ключём в первичном индексе уже
	// выполнила вызывающая процедура, поэтому здесь её не делаем
	//
	phi_insert (_bop, osp->index[0], _pk_obj, _obj);

	//
	// Проходим по всем вторичным индексам
	//
	foreach_indexi (1, index, osp)
	{
		//
		// Объект в индексе, соответствующий новому объекту
		//
		// Для объекта, который не соответствовал до данного момента условию включения
		// в индекс данный метод должен вернуть NULL, поскольку все подобные объекты не
		// включаются в индекс. Поэтому условные и безусловные индексы здесь можно
		// обрабатывать по общему алгоритму
		//
		// Здесь поиск ведётся по полям объекта, которые позволяют его однозначно
		// идентифицировать, так как при создании неуникального индекса к нему
		// неявно добавляются все поля первичного индекса, чтобы сделать его
		// уникальным и контролировать наличие в индексе конкретного объекта
		//
		struct tnt_object* index_obj = [index find_obj:_obj];

		//
		// Для неуникального индекса наличие актуального объекта в индексе является
		// программной ошибкой, так как в ключи такого индекса добавлены поля первичного
		// ключа и, поскольку объект по заданному первичному ключу отсутствует, то и
		// в индексе он должен отсутствовать
		//
		assert (index->conf.unique || (phi_right (index_obj) == NULL));

		//
		// Если объект должен быть включён в индекс
		//
		// Для объектов, которые не соответствуют условию включения в индекс
		// ничего с индексом не делаем, так как если объект и найден в индексе,
		// то его последняя версия помечена как неактуальная (удалённая). Это
		// гарантируется предыдущими проверками
		//
		if (tuple_match (&index->conf, _obj))
		{
			//
			// Если объект в индексе содержит актуальную версию объекта, то это ошибка
			//
			// Поскольку для уникальных индексов поля первичного индекса не добавляются,
			// то здесь мы контролируем уникальность объекта в пределах уникального индекса
			// (так как объекты с разными первичными ключами могут быть эквивалентны по
			// индексируемым полям). Это проблема клиента, сообщаем ему об этом
			//
			if (phi_right (index_obj) != NULL)
			{
				iproto_raise_fmt (ERR_CODE_INDEX_VIOLATION,
									"duplicate key value violates unique index %i:%s",
									index->conf.n, [[index class] name]);
			}

			phi_insert (_bop, index, index_obj, _obj);
		}
	}
}

/**
 * @brief Заменить объект в таблице
 *
 * @param[in] _bop операция, вызвавшая данное изменение
 * @parma[in] _pk_old_obj объект в первичном индексе, который соответствут предыдущей
 *                        версии объекта
 * @parma[in] _old_obj предыдущая версия объекта
 * @param[in] _obj новая версия объекта
 */
static void
object_space_replace (struct box_op* _bop, int _pk_modified, struct tnt_object* _pk_index_old_obj, struct tnt_object* _old_obj, struct tnt_object* _obj)
{
	//
	// Поскольку это операция замещения, то должны существовать обе
	// версии объекта и старая и новая
	//
	assert (_pk_index_old_obj);
	assert (_old_obj);
	assert (_obj);

	//
	// Предыдущая версия объекта в первичном индексе должна совпадать
	// с переданной предыдущей версией
	//
	assert (phi_right (_pk_index_old_obj) == _old_obj);

	//
	// Модифицируемая таблица
	//
	struct object_space* osp = _bop->object_space;

	//
	// Если данные, по которым объект индексируется первичным ключём,
	// не изменены, то просто замещаем предыдущую версию объекта в
	// первичном индексе новой версией
	//
	if (!_pk_modified)
	{
		phi_insert (_bop, osp->index[0], _pk_index_old_obj, _obj);
	}
	else
	{
		//
		// Объект в первичном индексе, который совпадает по ключу с новой
		// версией объекта
		//
		// Поскольку в данной ветке алгоритма поля первичного ключа объекта
		// не совпадают с полями первичного ключа старой версии объекта, то
		// это гарантированно должен быть объект, отличный от старого
		//
		struct tnt_object* pk_index_obj = [osp->index[0] find_obj:_obj];

		//
		// Если такой объект был найден и его последняя версия актуальна, то
		// диагностируем ошбку - попытка добавить в таблицу объект с неуникальным
		// первичным ключём
		//
		if (phi_right (pk_index_obj))
		{
			iproto_raise_fmt (ERR_CODE_INDEX_VIOLATION,
								"duplicate key value violates unique primary index %i:%s",
								0, [[osp->index[0] class] name]);
		}

		//
		// Удаляем из индекса предыдущую версию объекта
		//
		phi_insert (_bop, osp->index[0], _pk_index_old_obj, NULL);
		//
		// Добавляем в индекс новую версию объекта
		//
		phi_insert (_bop, osp->index[0], pk_index_obj, _obj);
	}

	//
	// Проходим по всем вторичным индексам
	//
	foreach_indexi (1, index, osp)
	{
		//
		// Объект в индексе, соответсвующий новой версии объекта
		//
		// Здесь поиск ведётся по полям объекта, которые позволяют его однозначно
		// идентифицировать, так как при создании неуникального индекса к нему
		// неявно добавляются все поля первичного индекса, чтобы сделать его
		// уникальным и контролировать наличие в индексе конкретного объекта
		//
		struct tnt_object* index_obj = [index find_obj:_obj];
		say_debug ("index_obj = %p, phi_right(index_obj) = %p, _obj = %p", index_obj, phi_right (index_obj), _obj);

		//
		// Если объект в индексе существует и содержит актуальную версию объекта,
		// совпадающего по ключевым полям индекса с новым объектом, но при этом
		// не является его предыдущей версией, то это ошибка
		//
		// Поскольку для уникальных индексов поля первичного индекса не добавляются,
		// то здесь мы контролируем уникальность объекта в пределах уникального индекса
		// (так как объекты с разными первичными ключами могут быть эквивалентны по
		// индексируемым полям). Это проблема клиента, сообщаем ему об этом
		//
		if (phi_right (index_obj) && (phi_right (index_obj) != _old_obj))
		{
			iproto_raise_fmt (ERR_CODE_INDEX_VIOLATION,
								"duplicate key value violates unique index %i:%s",
								index->conf.n, [[index class] name]);
		}

		//
		// Если это тот же самый объект, то это означает, что его индексируемые поля
		// для данного индекса не изменились, а следовательно не изменились и условия
		// включения данного объекта в индекс, если он условный. Так что дополнительно
		// можно ничего не проверять и просто добавить в индекс новую версию объекта
		//
		say_debug ("phi_right(index_obj) = %p, _old_obj = %p", phi_right (index_obj), _old_obj);
		if (phi_right (index_obj) == _old_obj)
		{
			phi_insert (_bop, index, index_obj, _obj);
		}
		//
		// Если объекта с такими ключевыми полями в данном индексе нет (был из индекса
		// удалён предыдущей операцией или его вовсе не было в индексе)
		//
		else
		{
			//
			// Ищем в индексе предыдущую версию объекта
			//
			struct tnt_object* index_old_obj = [index find_obj:_old_obj];
			say_debug ("index_old_obj = %p, phi_right(index_old_obj) = %p, _old_obj = %p", index_old_obj, phi_right (index_old_obj), _old_obj);

			//
			// Если такая версия есть и она актуальна (поскольку индекс может быть
			// условным, то её может и не быть или она может быть не актуальна и
			// тогда ничего со старой версией делать не надо), то исключаем её из
			// индекса (так как это операция замещения старого объекта на новый, то
			// делаем это не зависимо от того, попадает ли новая версия объекта в
			// индекс или нет)
			//
			if (phi_right (index_old_obj))
				phi_insert (_bop, index, index_old_obj, NULL);

			//
			// Если новая версия объекта попадает под условие включения его в
			// индекс, то добавляем её туда
			//
			if (tuple_match (&index->conf, _obj))
				phi_insert (_bop, index, index_obj, _obj);
		}
	}
}

void
prepare_replace (struct box_op* _bop, size_t _cardinality, const void* _data, u32 _len)
{
	//
	// Если количество полей объекта равно 0, то это ошибка
	//
	if (_cardinality == 0)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "cardinality can't be equal to 0");

	//
	// Если размер блока данных объекта имеет нулевой размер или он
	// не совпадает с размером объекта, посчитанным по его полям, то
	// это ошибка
	//
	say_debug ("_len = %d, fields_bsize = %zd", _len, fields_bsize (_cardinality, _data, _len));
	if ((_len == 0) || (fields_bsize (_cardinality, _data, _len) != _len))
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "tuple encoding error");

	//
	// Копируем данные новой версии объекта в операцию
	//
	_bop->obj = tuple_alloc (_cardinality, _len);
	memcpy (tuple_data (_bop->obj), _data, _len);

	//
	// Первичный индекс таблицы
	//
	Index<BasicIndex>* pk = _bop->object_space->index[0];

	//
	// Ищем объект в первичном индексе. Это может быть как сам объект, так
	// и box_phi структура, которая содержит выполненные в данной транзакции
	// операции над индексом
	//
	struct tnt_object* index_obj = [pk find_obj:_bop->obj];

	//
	// Запоминаем в операции указатель на предыдущую версию объекта
	//
	_bop->old_obj = phi_right (index_obj);

	//
	// Если это замена предыдущей версии объекта, то количество изменённых
	// объектов равно 2 иначе - 1
	//
	_bop->obj_affected = (_bop->old_obj != NULL) ? 2 : 1;

	//
	// Если это должна быть операция добавления объекта и при этом
	// найдена предыдущая версия, то это ошибка
	//
	if ((_bop->flags & BOX_ADD) && (_bop->old_obj != NULL))
		iproto_raise (ERR_CODE_NODE_FOUND, "tuple found");

	//
	// Если это должна быть операция замены предыдущей версии объекта и при
	// этом предыдущая версия объекта не найдена, то это ошибка
	//
	if ((_bop->flags & BOX_REPLACE) && (_bop->old_obj == NULL))
		iproto_raise (ERR_CODE_NODE_NOT_FOUND, "tuple not found");

	say_debug ("%s: old_obj:%p obj:%p", __func__, _bop->old_obj, _bop->obj);

	//
	// Если предыдущая версия объекта отсутствует, то добавляем объект
	//
	if (_bop->old_obj == NULL)
		object_space_insert (_bop, index_obj, _bop->obj);
	//
	// Иначе заменяем объект на его новую версию
	//
	else
		object_space_replace (_bop, 0, index_obj, _bop->old_obj, _bop->obj);
}

/**
 * @brief Полный размер заголовка для компактной записи объекта
 */
#define small_tuple_overhead (sizeof (struct tnt_object) + sizeof (struct box_small_tuple))

/**
 * @brief Полный размер заголовка для обычной записи объекта
 */
#define tuple_overhead (sizeof (struct gc_oct_object) + sizeof (struct box_tuple))

/**
 * @brief Обновить объём используемой таблицей оперативной памяти под данные
 */
static void
bytes_usage (struct object_space* _osp, struct tnt_object* _obj, int _sign)
{
	switch (_obj->type)
	{
		case BOX_TUPLE:
			_osp->obj_bytes += _sign*(tuple_bsize (_obj) + tuple_overhead);
			break;

		case BOX_SMALL_TUPLE:
			_osp->obj_bytes += _sign*(tuple_bsize (_obj) + small_tuple_overhead);
			break;

		default:
			assert (false);
	}

	_osp->slab_bytes += _sign*salloc_usable_size (_obj);
}

void
snap_insert_row (struct object_space* _osp, size_t _cardinality, const void* _data, u32 _len)
{
	//
	// Число полей в записи должно быть больше нуля
	//
	if (_cardinality == 0)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "cardinality can't be equal to 0");

	//
	// Проверяем валидность переданных данных
	//
	if ((_len == 0) || (fields_bsize (_cardinality, _data, _len) != _len))
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "tuple encoding error");

	//
	// Создаём запись в памяти и копируем туда данные
	//
	struct tnt_object* obj = tuple_alloc (_cardinality, _len);
	memcpy (tuple_data (obj), _data, _len);

	@try
	{
		//
		// Повторно проверяем валидность записи
		//
		// Непонятно, зачем это делать, так как эта проверка была выполнена выше
		// при проверке _len
		//
		if (!tuple_valid (obj))
			raise_fmt ("tuple misformatted");

		//
		// Добавляем запись в первичный индекс
		//
		// Добавление объекта в другие индексы будет сделано при их перестроении
		// после полной загрузки данных
		//
		[_osp->index[0] replace: obj];

		//
		// Увеличиваем объём данных
		//
		bytes_usage (_osp, obj, +1);
	}
	@catch (id e)
	{
		tuple_free (obj);
		@throw;
	}
}

/**
 * @brief Выполнение набора арифметических операций над данными
 */
static void
do_field_arith (u8 _op, struct tbuf* _field, const void* _arg, u32 _arg_size)
{
	if (tbuf_len (_field) != _arg_size)
		iproto_raise_fmt (ERR_CODE_ILLEGAL_PARAMS, "num op arg size (%d) not equal to field size (%d)", _arg_size, tbuf_len (_field));

	switch (_arg_size)
	{
		case 2:
			switch (_op)
			{
				case 1:
					*(u16*)_field->ptr += *(u16*)_arg;
					break;

				case 2:
					*(u16*)_field->ptr &= *(u16*)_arg;
					break;

				case 3:
					*(u16*)_field->ptr ^= *(u16*)_arg;
					break;

				case 4:
					*(u16*)_field->ptr |= *(u16*)_arg;
					break;

				default:
					iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "unknown num op");
			}
			break;

		case 4:
			switch (_op)
			{
				case 1:
					*(u32*)_field->ptr += *(u32*)_arg;
					break;

				case 2:
					*(u32*)_field->ptr &= *(u32*)_arg;
					break;

				case 3:
					*(u32*)_field->ptr ^= *(u32*)_arg;
					break;

				case 4:
					*(u32*)_field->ptr |= *(u32*)_arg;
					break;

				default:
					iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "unknown num op");
			}
			break;

		case 8:
			switch (_op)
			{
				case 1:
					*(u64*)_field->ptr += *(u64*)_arg;
					break;

				case 2:
					*(u64*)_field->ptr &= *(u64*)_arg;
					break;

				case 3:
					*(u64*)_field->ptr ^= *(u64*)_arg;
					break;

				case 4:
					*(u64*)_field->ptr |= *(u64*)_arg;
					break;

				default:
					iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "unknown num op");
			}
			break;

		default:
			iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "bad num op size");
	}
}

/**
 * @brief Полный размер памяти, занимаемый упакованным полем, значение которого
 *        находится в буфере
 *
 * Складывается из длины закодированного размера буфера и самого размера буфера
 */
static inline size_t __attribute__((pure))
field_len (const struct tbuf* _buf)
{
	return varint32_sizeof (tbuf_len(_buf)) + tbuf_len (_buf);
}

/**
 * @brief Заменить заданную часть буфера заданным блоком
 *
 * @param[inout] _buf буфер, в котором должна быть замещена часть
 * @param[in] _args аргументы операции
 * @param[in] _size размер аргументов операции
 *
 * @return разницу размеров буфера до и после операции
 *
 * Аргументы представляют собой три закодированных так же как и поля записи (размер +
 * значение) параметра:
 *    * смещение вырезаемой части в буфере (если значение смещения отрицательное, то
 *      это значит, что оно задаётся относительно конца буфера);
 *    * размер вырезаемой части буфера (если значение размера отрицательное, то это
 *      значит, что оно задаётся по направлению от конца буфера к его началу);
 *    * блок, который должен заместить заданную часть буфера.
 */
static size_t
do_field_splice (struct tbuf* _buf, const void* _args, u32 _size)
{
	//
	// Аргументы операции, представленные как буфер для упрощения чтения
	//
	struct tbuf args = TBUF (_args, _size, NULL);

	//
	// Новый буфер распределяем в пуле памяти текущей сопрограммы
	//
	struct tbuf* buf_new = tbuf_alloc (fiber->pool);

	//
	// Расставляем указатели на аргументы
	//
	const u8* foffset = read_field (&args);
	const u8* flength = read_field (&args);
	const u8* fdata   = read_field (&args);

	//
	// Если размер буфера аргументов после их чтения не пуст, значит что-то
	// пошло не так
	//
	if (tbuf_len (&args) != 0)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: bad args");

	//
	// Смещение вырезаемой части относительно начала буфера. По умолчанию
	// с начала буфера
	//
	u32 noffset = 0;
	//
	// Размер значения смещения
	//
	u32 offset_size = LOAD_VARINT32 (foffset);
	//
	// Если размер значения поля смещения равен 32-битному целому со знаком
	//
	if (offset_size == sizeof (i32))
	{
		//
		// Декодируем значение смещения
		//
		i32 offset = *(u32*)foffset;

		//
		// Если размер смещения отрицателен, то смещение указано относительно
		// конца буфера, а не его начала
		//
		if (offset < 0)
		{
			//
			// Если абсолютная величина смещения больше размера буфера, то
			// это ошибка так как позиция, идентифицируемая этим смещением,
			// выйдет за границы буфера
			//
			if (tbuf_len (_buf) < -offset)
				iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: noffset is negative");

			//
			// Преобразуем отрицательное значение смещения в смещение в буфере относительно
			// начала буфера
			//
			noffset = tbuf_len (_buf) + offset;
		}
		//
		// Иначе смещение указано относительно начала буфера
		//
		else
		{
			noffset = offset;
		}
	}
	//
	// Иначе если размер значения смещение не равен нулю, то передали что-то странное
	//
	else if (offset_size != 0)
	{
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: bad size of offset field");
	}

	//
	// Принудительно обрезаем смещение по размеру буфера
	//
	if (noffset > tbuf_len (_buf))
		noffset = tbuf_len (_buf);

	//
	// Длина вырезаемой части буфера по направлению от его начала до конца
	// буфера. По умолчанию до конца буфера
	//
	u32 nlength = tbuf_len (_buf) - noffset;
	//
	// Размер значения длины
	//
	u32 length_size = LOAD_VARINT32 (flength);
	//
	// Если размер поля длины равен 32-битному целому со знаком
	//
	if (length_size == sizeof (i32))
	{
		//
		// Если размер значения смещения равен нулю, то размер значения длины так же должен
		// быть равен нулю (то есть вырезаем весь буфер)
		//
		if (offset_size == 0)
			iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: offset field is empty but length is not");

		//
		// Декодируем значение длины
		//
		i32 length = *(i32*)flength;

		//
		// Если длина меньше нуля, то это значит, что длина отсчитывается по направлению
		// к началу буфера
		//
		if (length < 0)
		{
			//
			// Если поле выходит за пределы начала буфера, то устанавливаем
			// нормализованную длину в 0 (то есть ничего не вырезаем, а просто
			// вставляем новые данные в заданную позицию)
			//
			if ((tbuf_len (_buf) - noffset) < -length)
				nlength = 0;
			//
			// Иначе преобразуем длину в длину относительно нормализованного
			// смещения по направлению к концу буфера
			//
			else
				nlength = tbuf_len (_buf) - noffset + length;
		}
		//
		// Иначе длина и так указана относительно нормализованного смещения
		// по направлению к концу буфера
		//
		else
		{
			nlength = length;
		}
	}
	//
	// Иначе если размер значения длины не равен нулю, то передали что-то странное
	//
	else if (length_size != 0)
	{
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: bad size of length field");
	}

	//
	// При необходимости обрезаем длину замещаемой части по размеру буфера
	//
	if (nlength > (tbuf_len (_buf) - noffset))
		nlength = tbuf_len (_buf) - noffset;

	//
	// Размер блока, который должен быть скопирован в выходной буфер. После чтения
	// длины fdata будет указывать на начало блока данных, вставляемого в буфер
	//
	u32 data_size = LOAD_VARINT32 (fdata);
	//
	// Если размер блока данных больше нуля, то должен быть задан и размер замещаемой
	// части
	//
	if ((data_size > 0) && (length_size == 0))
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: length field is empty but data is not");
	//
	// Итоговый размер буфера после вставки новых данных не должен превышать значение,
	// которое может храниться в 32-битном целом
	//
	if (data_size > (UINT32_MAX - (tbuf_len (_buf) - nlength)))
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "do_field_splice: list_size is too long");

	say_debug ("do_field_splice: noffset = %i, nlength = %i, data_size = %u", noffset, nlength, data_size);

	//
	// Инициализируем новый буфер (FIXME: а есть необходимость после tbuf_alloc?)
	//
	tbuf_reset (buf_new);
	//
	// Добавляем в новый буфер часть входного буфера от его начала до начала вырезаемой
	// части
	//
	tbuf_append (buf_new, _buf->ptr, noffset);
	//
	// Добавляем в новый буфер заданный в аргументах блок памяти
	//
	tbuf_append (buf_new, fdata, data_size);
	//
	// Добавляем в новый буфер хвост входного буфера после вырезаемой части и до его конца
	//
	tbuf_append (buf_new, _buf->ptr + noffset + nlength, tbuf_len (_buf) - (noffset + nlength));

	//
	// Изменение размера буфера
	//
	size_t diff = field_len (buf_new) - field_len (_buf);

	//
	// Передаём на выход новый буфер
	//
	*_buf = *buf_new;

	return diff;
}

/**
 * @brief Возвращает признак того, что индексируемые поля объекта изменятся
 *        при изменении заданного поля
 *
 * @param[in] _idx проверяемый индекс
 * @param[in] _field индекс проверяемого поля
 * @param[in] _modified предыдущее значение признака (для оптимизации)
 *
 * @return 1 если поле с заданым индексом входит в поле и 0 в противном случае
 */
static int
idxModified (Index<BasicIndex>* _idx, u32 _field, int _modified)
{
	for (int i = 0; (_modified == 0) && (i < _idx->conf.cardinality); ++i)
	{
		if (_idx->conf.field[i].index == _field)
			_modified = 1;
	}

	return _modified;
}

/**
 * @brief Возвращает признак того, что индексируемые поля объекта будут
 *        сдвинуты при добавлении или удалении заданного поля
 *
 * @param _idx проверяемый индекс
 * @param _field индекс проверяемого поля
 *
 * @return 1 если индексы полей, входящих в первичный ключ, изменятся
 *         при удалении или добавлении заданного поля и 0 в противном
 *         случае
 */
static int
idxAffected (Index<BasicIndex>* _idx, u32 _field)
{
	int affected = 0;

	for (int i = 0; (affected == 0) && (i < _idx->conf.cardinality); ++i)
	{
		if (_idx->conf.field[i].index >= _field)
			affected = 1;
	}

	return affected;
}

/**
 * @brief Выполнение заданного списка операций
 */
static void __attribute__((noinline))
prepare_update_fields (struct box_op* _bop, struct tbuf* _args)
{
	//
	// Размерность ключа модифицируемой записи
	//
	u32 key_cardinality = read_u32 (_args);
	//
	// Размерность ключа записи должна совпадать с размерностью первичного индекса
	// модифицируемой таблицы
	//
	if (key_cardinality != _bop->object_space->index[0]->conf.cardinality)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "key fields count doesn't match");

	//
	// Первичный ключ модифицируемой таблицы
	//
	Index<BasicIndex>* pk = _bop->object_space->index[0];

	//
	// Ищем предыдущую версию записи по первичному ключу
	//
	struct tnt_object* index_old = [pk find_key:_args cardinalty:key_cardinality];
	//
	// Собственно предыдущая версия записи (она может быть как версией, зафиксированной
	// предыдущим коммитом так и версией в рамках текущей транзакции, модифицированной
	// предыдущими операциями)
	//
	_bop->old_obj = phi_right (index_old);

	//
	// Если предыдущая версия записи не найдена, то завершаем работу, так как
	// модифицировать нечего. При этом устанавливаем буфер аргументов в полностью
	// прочитанное состояние, чтобы вызывающий код не зафиксировал ошибку
	//
	if (_bop->old_obj == NULL)
	{
		tbuf_ltrim (_args, tbuf_len (_args));
		return;
	}

	//
	// Число операций над записью
	//
	u32 op_count = read_u32 (_args);
	if (op_count == 0)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "no ops");

	//
	// Число изменённых в рамках операции записей. Поскольку ключ задаётся
	// только один, то сколько бы операций не было задано измениться ровно
	// одна запись
	//
	_bop->obj_affected = 1;

	//
	// Параметры модифицируемой записи: размер, число полей и данные
	//
	size_t bsize       = tuple_bsize (_bop->old_obj);
	int    cardinality = tuple_cardinality (_bop->old_obj);

	//
	// Размер массива буферов под поля модифицируемой записи. Поскольку
	// возможны операции добавления полей в запись, то для уменьшения
	// количества перераспределений памяти применяем некоторую магию для
	// вычисления резерва по буферам. Магия заключается в том, что на
	// каждые пять полей записи мы добавляем один дополнительный буфер
	//
	int buffer_count = cardinality + cardinality/5;

	//
	// Распределяем память под буфера для полей в пуле памяти сопрограммы
	//
	struct tbuf* buffers = palloc (fiber->pool, buffer_count*sizeof (struct tbuf));

	//
	// Заполняем массив буферами, ссылающимися на память соответствующих полей
	// в записи
	//
	// ВНИМАНИЕ!!! Каждый буфер не является нормальным буфером, а содержит
	// следующую информацию:
	//     .ptr  - указатель на начало поля (на начало закодированной длины поля);
	//     .end  - указатель на начало данных поля;
	//     .free - размер данных поля.
	//
	{
		const u8* p = (const u8*)tuple_data (_bop->old_obj);
		for (int i = 0; i < cardinality; ++i)
		{
			//
			// Начало поля (указывает на закодированную длину данных поля)
			//
			const u8* f = p;
			//
			// Длина данных поля. После выполнения этой операции p передвинется
			// на начало данных
			//
			int len = LOAD_VARINT32 (p);

			//
			// Заполняем очередной буфер
			//
			buffers[i].ptr  = (void*)f;
			buffers[i].end  = (void*)p;
			buffers[i].free = len;
			buffers[i].pool = NULL;

			//
			// Переходим к следующему полю
			//
			p += len;
		}
	}

	//
	// Признак изменения параметров ключа записи. По умочанию считаем,
	// что перевичный ключ не модифицируется
	//
	int pk_modified = 0;

	//
	// Проходим по всему списку операций
	//
	while (op_count-- > 0)
	{
		//
		// Буфер, который указывает на модифицирумое поле
		//
		struct tbuf* buf = NULL;

		//
		// Индекс поля, к которому применяется операция
		//
		u32 field_no = read_u32 (_args);
		//
		// Код операции
		//
		u8 op = read_u8 (_args);
		//
		// Начало аргументов операции. После этого указатель _args переместится на
		// начало следующей операции
		//
		const u8* arg = read_field (_args);
		//
		// Размер аргументов операции. После этого указатель arg переместится собственно
		// на начало аргументов
		//
		i32 arg_size = LOAD_VARINT32 (arg);

		//
		// Проверяем, что модифицированное поле затрагивает данные первичного ключа
		//
		pk_modified = idxModified (pk, field_no, pk_modified);

		//
		// Для всех операций, кроме добавления поля
		//
		if (op <= 6)
		{
			//
			// Проверяем, что индекс модифицируемого поля не выходит за пределы
			// полей записи
			//
			if (field_no >= cardinality)
				iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "update of field beyond tuple cardinality");

			//
			// Инициализируем указатель на модифицируемое поле указателем на
			// соответствующий буфер массива
			//
			buf = &buffers[field_no];
		}

		//
		// Для всех операций, кроме операций удаления или добавления поля записи
		//
		if (op < 6)
		{
			//
			// Если заданное поле ещё не модифицировалось
			//
			if (buf->pool == NULL)
			{
				//
				// Указатель на начало данных поля
				//
				void* fdata = buf->end;
				//
				// Размер данных поля
				//
				int fsize = buf->free;
				//
				// Ожидаемый размер данных поля после модификации равен максимальному
				// из размера данных аргумента операции или размера данных поля
				//
				int expected_size = MAX (arg_size, fsize);

				//
				// Распределяем память под модифицированные данные. Если ожидаемый
				// размер данных равен нулю, то распределяем 8 байт
				//
				buf->ptr = palloc (fiber->pool, expected_size ?: 8);
				//
				// Копируем исходные данные поля в буфер
				//
				memcpy (buf->ptr, fdata, fsize);
				//
				// Устанавливаем указатель на конец данных поля
				//
				buf->end = buf->ptr + fsize;
				//
				// Устанавливаем размер свободной памяти в буфере
				//
				buf->free = expected_size - fsize;
				//
				// Устанавливаем пул, в котором распределена память
				//
				buf->pool = fiber->pool;

				//
				// После этого данный буфер является нормальным буфером,
				// а не псевдо-буфером
				//
			}
		}

		//
		// Начинаем выполнение операции
		//
		switch (op)
		{
			//
			// Установить новое значение заданного поля
			//
			case 0:
				//
				// Уменьшаем общий размер записи на размер модифицируемого поля
				//
				bsize -= field_len (buf);
				//
				// Увеличиваем общий размер записи на размер новой версии поля
				//
				bsize += varint32_sizeof (arg_size) + arg_size;
				//
				// Устанавливаем новое значение поля
				//
				tbuf_reset  (buf);
				tbuf_append (buf, arg, arg_size);
				break;

			//
			// Операции над полем
			//
			case 1 ... 4:
				do_field_arith (op, buf, arg, arg_size);
				break;

			//
			// Заменить заданную часть поля с коррекцией итогового размера записи
			//
			case 5:
				bsize += do_field_splice (buf, arg, arg_size);
				break;

			//
			// Удалить поле
			//
			case 6:
				//
				// Операция удаления поля не должна иметь аргументов
				//
				if (arg_size != 0)
					iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "delete must have empty arg");

				//
				// Поле, которое влияет на первичный ключ, удалять нельзя
				//
				if (idxAffected (pk, field_no) == 1)
					iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "unable to delete field because PK is affected");

				//
				// Уменьшаем общий размер записи на размер удаляемого поля
				//
				if (buf->pool == NULL)
					bsize -= tbuf_len (buf) + tbuf_free (buf);
				else
					bsize -= field_len (buf);

				//
				// Удаляем поле с заданным индексом из массива буферов
				//
				for (int i = field_no; i < (cardinality - 1); i++)
					buffers[i] = buffers[i + 1];

				//
				// Уменьшаем число полей записи
				//
				--cardinality;
				break;

			//
			// Добавить поле
			//
			case 7:
				//
				// Поле нельзя добавить, если это повлияет на первичный ключ
				//
				if (idxAffected (pk, field_no) == 1)
					iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "unable to insert field because PK is affected");

				//
				// Нельзя вставить поле вне записи. Единственное исключение - если
				// field_no равен cardinality то это означает, что поле будет добавлено
				// в конец записи
				//
				if (field_no > cardinality)
					iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "update of field beyond tuple cardinality");

				//
				// Если исчерпался запас зарезервированных буферов, то увеличиваем
				// количество буферов на 128, чтобы уж с запасом, но считаем этот
				// случай маловероятным
				//
				if (unlikely (cardinality == buffer_count))
				{
					struct tbuf* b = buffers;
					buffers = palloc (fiber->pool, (buffer_count + 128)*sizeof (struct tbuf));
					memcpy (buffers, b, buffer_count*sizeof (struct tbuf));
					buffer_count += 128;
				}

				//
				// Освобождаем место под вставляемое поле
				//
				for (int i = cardinality - 1; i >= field_no; --i)
					buffers[i + 1] = buffers[i];

				//
				// Инициализируем данные поля
				//
				{
					void* p = palloc (fiber->pool, arg_size);
					buffers[field_no] = TBUF (p, arg_size, fiber->pool);
					memcpy (buffers[field_no].ptr, arg, arg_size);
				}

				//
				// Увеличиваем итоговый размер записи
				//
				bsize += varint32_sizeof (arg_size) + arg_size;

				//
				// Увеличиваем число полей записи
				//
				++cardinality;
				break;

			//
			// Остальные коды операций не поддерживаются
			//
			default:
				iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "invalid op");
		}
	}

	//
	// Если операции закончились, а аргументы ещё нет, то это ошибка
	// входящего запроса
	//
	if (tbuf_len (_args) != 0)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");

	//
	// Создаём новую запись с модифицированным количеством полей и
	// размером, достаточным для упаковки всех полей
	//
	_bop->obj = tuple_alloc (cardinality, bsize);

	//
	// Упаковываем данные из буферов в запись
	//
	{
		//
		// Указатель на свободную область памяти в записи
		//
		u8* p = tuple_data (_bop->obj);

		//
		// Проходим по всем заполненным буферам
		//
		for (int i = 0; i < cardinality;)
		{
			//
			// Если буфер не модифицировался
			//
			if (buffers[i].pool == NULL)
			{
				//
				// Реальные начало и конец области памяти для копирования
				// в итоговую запись
				//
				void* ptr = buffers[i].ptr;
				void* end = buffers[i].end + buffers[i].free;

				//
				// Пока следующие далее поля составляют непрерывную с текущим
				// область памяти добавляем их к текущей области
				//
				for (++i; i < cardinality; ++i)
				{
					//
					// Если начало области памяти следующего поля не совпадает
					// с концом предыдущего, то прерываем расширение буфера
					//
					if (end != buffers[i].ptr)
						break;

					//
					// Если конец области памяти предыдущего поля совпадает с
					// началом текущего, то включаем поле в область для копирования
					//
					end = buffers[i].end + buffers[i].free;
				}

				//
				// Копируем немодифицированную область памяти исходной записи
				// в новую запись
				//
				memcpy (p, ptr, end - ptr);

				//
				// Смещаем текущий указатель на свободную память в конец заполненной
				//
				p += end - ptr;
			}
			//
			// Если буфер модифицирован
			//
			else
			{
				//
				// Размер поля
				//
				int len = tbuf_len (&buffers[i]);
				//
				// Упаковываем размер поля и смещаем указатель на свободную область памяти
				//
				p = save_varint32 (p, len);
				//
				// Копируем значение поля
				//
				memcpy (p, buffers[i].ptr, len);
				//
				// Смещаем указатель на свободную память в конец заполненной области
				//
				p += len;

				//
				// Переходим к следующему полю
				//
				++i;
			}
		}
	}

	//
	// Если изменился ключ объекта, то увеличиваем количество изменённых объектов
	//
	// FIXME: не понятно, почему используется функция, а не переменная pk_modified
	//
	if (![pk eq:_bop->old_obj :_bop->obj])
		++_bop->obj_affected;

	//
	// Замещаем объект его новой версией
	//
	object_space_replace (_bop, pk_modified, index_old, _bop->old_obj, _bop->obj);
}

/**
 * @brief Преобразование кодов сообщений
 */
static inline enum BOX_SPACE_STAT
message_to_boxstat (enum messages _msg)
{
	switch (_msg)
	{
		case INSERT:
			return BSS_INSERT;

		case UPDATE_FIELDS:
			return BSS_UPDATE;

		case DELETE:
			return BSS_DELETE;

		case DELETE_1_3:
			return BSS_DELETE;

		default:
			panic ("MSG == %d", _msg);
	}
}

/**
 * @brief Выполнить выборку данных из базы
 *
 * @param _h хендлер для отправки результатов
 * @param _idx индекс, по которому выполняются запросы
 * @param _limit количество записей в результате
 * @param _offset смещение окна результатов
 * @param _count число запрошенных ключей
 * @param _data параметры запросов
 *
 * @return количество найденых записей
 */
static u32 __attribute__((noinline))
process_select (struct netmsg_head* _h, Index<BasicIndex>* _idx, u32 _limit, u32 _offset, u32 _count, struct tbuf* _data)
{
	say_debug ("SELECT");

	//
	// Признак того, что индекс является хэшем
	//
	bool is_hash = index_type_is_hash (_idx->conf.type);

	//
	// Функция сравнения ключа. Используется только для древовидного индекса
	// в случае его обхода по неполному совпадению количества полей ключа в
	// запросе с ключом индекса
	//
	index_cmp cmp = NULL;
	if (!is_hash)
		cmp = [(Tree*)_idx compare];

	//
	// Первым в результате будет возвращаться число найденных записей,
	// поэтому добавляем к результату память под соответствующее число
	// и запоминаем указатель на него, чтобы можно было его корректировать
	// в процессе поиска
	//
	uint32_t* found = net_add_alloc (_h, sizeof (uint32_t));
	*found = 0;

	//
	// Для всех заданных запросов
	//
	for (u32 i = 0; i < _count; ++i)
	{
		//
		// Число полей ключа в запросе
		//
		u32 c = read_u32 (_data);

		//
		// Если число полей индекса совпадает с числом полей ключа
		// в запросе, то не важно каким именно является индекс, так
		// как поиск выполняется с помощью полиморфной функции
		//
		// Поскольку при неуникальном древовидном индексе к нему неявно
		// добавляется в конец первичный ключ, который делает его уникальным,
		// то получается, что данный код нормально работает и в случае
		// неуникального древовидного индекса
		//
		if (_idx->conf.cardinality == c)
		{
			//
			// Ищем запись по индексу. Поскольку данные запакованы в буфер,
			// то при их чтении буфер будет автоматически смещён к следующему
			// запросу
			//
			struct tnt_object* obj = tuple_visible_left ([_idx find_key:_data cardinalty:c]);

			//
			// Запись не найдена, продолжнаем перебор запросов
			//
			if (obj == NULL)
				continue;

			//
			// Лимит исчерпан, продолжаем перебор запросов
			//
			// Проверку на лимит ставим именно здесь, так как нужно, чтобы find_key
			// индекса вычитал из буфера данных значение ключа в любом случае, независимо
			// от того исчерпано количество записей для выдачи или нет. Иначе невозможно
			// будет проверить корректность запроса
			//
			if (unlikely (_limit == 0))
				continue;

			//
			// Если задано смещение и оно ещё не достигнуто, то переходим к следующему
			// запросу
			//
			if (unlikely (_offset > 0))
			{
				--_offset;
				continue;
			}

			//
			// Увеличиваем число найденных записей
			//
			++(*found);

			//
			// Добавляем запись в результат
			//
			net_tuple_add (_h, obj);

			//
			// Уменьшаем число оставшихся для выборки записей
			//
			--_limit;
		}
		//
		// Если число полей индекса не совпадает с числом полей ключа
		// поиска, то поиск можно выполнять только по древовидному
		// индексу
		//
		else if (!is_hash)
		{
			Tree* tree = (Tree*)_idx;

			//
			// Инициализация итератора по дереву для заданного ключа. Поскольку
			// данные запакованы в буфер, то при их чтении буфер будет автоматически
			// смещён к следующему запросу
			//
			[tree iterator_init_with_key:_data cardinalty:c];

			//
			// Лимит исчерпан, переходим к следующему запросу
			//
			// Проверку на лимит ставим именно здесь, так как нужно, чтобы iterator_init_with_key
			// индекса вычитал из буфера данных значение ключа в любом случае, независимо от того
			// исчерпано количество записей для выдачи или нет. Иначе невозможно будет проверить
			// корректность запроса
			//
			if (unlikely (_limit == 0))
				continue;

			//
			// Проходим по индексу с использованием заданной функции сравнения,
			// поскольку в результате теперь может быть не одна запись
			//
			struct tnt_object* obj = NULL;
			while ((obj = [tree iterator_next_check:cmp]) != NULL)
			{
				//
				// Найденная запись
				//
				obj = tuple_visible_left (obj);

				//
				// Если запись не найдена. По идее этого случиться не должно, но
				// на всякий случай
				//
				if (unlikely (obj == NULL))
					continue;

				//
				// Если задано смещение и оно ещё не достигнуто, то продолжаем проход
				// про индексу и/или выполнение операций
				//
				if (unlikely (_offset > 0))
				{
					--_offset;
					continue;
				}

				//
				// Увеличиваем число найденных записей
				//
				++(*found);

				//
				// Добавляем запись в результат
				//
				net_tuple_add (_h, obj);

				//
				// Уменьшаем число оставшихся для выборки записей. Если лимит достигнут,
				// то прерываем проход по индексу
				//
				if (--_limit == 0)
					break;
			}
		}
		//
		// Число полей ключа не совпадает с числом полей индекса и индекс является
		// хэшем. В этом случае завершаемся с ошибкой, так как по хэшу можно искать
		// записи только по полному совпадению с ключем
		//
		else
		{
			iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "cardinality mismatch");
		}
	}

	//
	// Проверяем, что все запросы обработаны
	//
	if (tbuf_len (_data) != 0)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");

	//
	// Возвращаем число найденных записей
	//
	return *found;
}

/**
 * @brief Удалить запись из базы
 *
 * @param _bop
 * @param _key
 */
static void __attribute__((noinline))
prepare_delete (struct box_op* _bop, struct tbuf* _key)
{
	//
	// Число полей ключа
	//
	u32 c = read_u32 (_key);

	//
	// Запись для удаления
	//
	struct tnt_object* index_old = [_bop->object_space->index[0] find_key:_key cardinalty:c];

	//
	// Последняя версия удалённой записи
	//
	_bop->old_obj = phi_right (index_old);

	//
	// Число затронутых записей
	//
	_bop->obj_affected = (_bop->old_obj != NULL);

	//
	// Заносим информацию об удалении в индекс
	//
	object_space_delete (_bop, index_old, _bop->old_obj);
}

/**
 * @brief Создать операцию и добавить её в список операций транзакции
 */
static struct box_op*
box_op_alloc (struct box_txn* _txn, int _op, const void* _data, int _len)
{
	//
	// Распределяем память под операцию в пуле текущей сопрограммы и инициализируем
	// её нулями
	//
	struct box_op* bop = p0alloc (fiber->pool, sizeof (struct box_op) + _len);

	//
	// Заполняем данные операции
	//
	bop->txn      = _txn;
	bop->op       = _op & 0xffff;
	bop->data_len = _len;
	TAILQ_INIT (&bop->phi);

	memcpy (bop->data, _data, _len);

	//
	// Добавляем операцию в список операций транзакции
	//
	TAILQ_INSERT_TAIL (&_txn->ops, bop, link);

	return bop;
}

static void box_op_rollback (struct box_op* _bop);

struct box_op *
box_prepare (struct box_txn* _txn, int _op, const void* _data, u32 _len)
{
	say_debug ("%s op:%i/%s", __func__, _op, box_op_name (_op));

	//
	// Изменение данных в режиме "только чтение" заблокировано
	//
	if (_txn->mode == RO)
		iproto_raise (ERR_CODE_NONMASTER, "txn is readonly");

	//
	// Буфер для распаковки данных стандартными процедурами
	//
	struct tbuf buf = TBUF (_data, _len, NULL);

	//
	// Выполняемая операция
	//
	struct box_op* bop = box_op_alloc (_txn, _op, _data, _len);
	@try
	{
		//
		// Читаем код таблицы
		//
		i32 n = read_u32 (&buf);

		//
		// Таблица, для которой выполняется операция
		//
		bop->object_space = object_space (_txn->box, n);
		if (bop->object_space->ignored)
		{
			//
			// bop->object_space == NULL означает, то данная транзакция будет проигнорирована
			//
			bop->object_space = NULL;
			return bop;
		}

		//
		// В зависимости от кода операции
		//
		switch (_op)
		{
			//
			// Операция добавления/обновления записи
			//
			case INSERT:
				//
				// Флаги операции
				//
				bop->flags = read_u32 (&buf);
				//
				// Количество полей в записи
				//
				u32 cardinality = read_u32 (&buf);
				//
				// Размер данных записи
				//
				u32 tuple_blen = tbuf_len (&buf);
				//
				// Данные записи
				//
				const void* tuple_bytes = read_bytes (&buf, tuple_blen);

				//
				// Выполняем операцию вставки записи
				//
				say_debug ("insert: bop->flags = %d, cardinality = %d, tuple_blen = %d, data = %s",
						   bop->flags, cardinality, tuple_blen, dump (tuple_bytes, tuple_blen));
				prepare_replace (bop, cardinality, tuple_bytes, tuple_blen);

				//
				// Возвращаем добавленную запись
				//
				bop->ret_obj = bop->obj;
				break;

			//
			// Операция удаления записи
			//
			case DELETE:
				//
				// Флаги операции DELETE (только для версии > 1.3)
				//
				bop->flags = read_u32 (&buf); /* RETURN_TUPLE */

			//
			// Для операции удаления v1.3 флаги операции не передаются
			//
			case DELETE_1_3:
				//
				// Выполняем операцию удаления записи
				//
				prepare_delete (bop, &buf);

				//
				// Возвращаем удалённую запись
				//
				bop->ret_obj = bop->old_obj;
				break;

			//
			// Операция обновления данных записи
			//
			case UPDATE_FIELDS:
				//
				// Флаги операции
				//
				bop->flags = read_u32 (&buf);

				//
				// Обновляем данные записи
				//
				prepare_update_fields (bop, &buf);

				//
				// Возвращаем обновлённую запись
				//
				bop->ret_obj = bop->obj;
				break;

			//
			// Операции NOP игнорируем
			//
			case NOP:
				break;

			//
			// Всё остальные случаи ошибочны
			//
			default:
				iproto_raise_fmt (ERR_CODE_ILLEGAL_PARAMS, "unknown opcode:%"PRIi32, _op);
		}

		//
		// Если в результате выполнения операции создана новая запись
		//
		if (bop->obj)
		{
			//
			// Проверяем, что если задано количество полей таблицы, то оно равно количеству полей новой записи
			//
			if ((bop->object_space->cardinality > 0) && (bop->object_space->cardinality != tuple_cardinality (bop->obj)))
				iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "tuple cardinality must match object_space cardinality");

			//
			// Проверяем валидность новой записи
			//
			if (!tuple_valid (bop->obj))
				iproto_raise (ERR_CODE_UNKNOWN_ERROR, "internal error");
		}

		//
		// Если не все данные запроса обработаны
		//
		if (tbuf_len (&buf) != 0)
			iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "can't unpack request");
	}
	@catch (id e)
	{
		//
		// В случае ошибки удаляем операцию из списка операций транзакции
		//
		TAILQ_REMOVE (&_txn->ops, bop, link);

		//
		// Откатываем операцию
		//
		box_op_rollback (bop);

		//
		// Передаём ошибку выше по стеку
		//
		@throw;
	}

	return bop;
}

/**
 * @brief Сбор статистики по транзакции
 */
static ev_tstamp
txn_stat_cpu (struct box_txn* _tx)
{
	if (cfg.box_extended_stat && _tx->name)
	{
		struct tbuf name = TBUF (NULL, 0, fiber->pool);

		//
		// Вычисляем время выполенния транзакции
		//
		ev_tstamp now = ev_time ();
		ev_tstamp diff = (now - _tx->start)*1000;

		//
		// Имя статистики
		//
		tbuf_append (&name, _tx->name, _tx->namelen);
		tbuf_append_lit (&name, ":cpu");

		//
		// Собираем статистику
		//
		box_stat_aggregate_named (name.ptr, tbuf_len (&name), diff);
		box_stat_aggregate_named ("TXN:cpu", 7, diff);

		return now;
	}

	return 0;
}

/**
 * @brief Завершение транзакции и запись статистики
 */
static void
txn_cleanup (struct box_txn* _tx)
{
	say_debug3 ("%s: txn:%i/%p", __func__, _tx->id, _tx);

	//
	// Проверяем, чтобы транзакция текущей сопрограммы была
	// данной транзакцией
	//
	// При переключении на сопрограмму средствами Fiber указатель на
	// сопрограмму, которая запущена на выполнение запоминается в
	// глобальной переменной fiber
	//
	assert (fiber->txn == _tx);

	//
	// Отключаем транзакцию от сопрограммы
	//
	fiber->txn = NULL;

	//
	// Пишем статистику
	//
	if (cfg.box_extended_stat && _tx->name)
	{
		box_stat_sum_named (_tx->name, _tx->namelen, 1);
		box_stat_sum_named ("TXN", 3, 1);
	}
}

/**
 * @brief Подтверждение выполнения операции
 */
static void
box_op_commit (struct box_op* _bop)
{
	say_debug2 ("%s: old_obj:%p obj:%p", __func__, _bop->old_obj, _bop->obj);

	if (!_bop->object_space)
		return;

	//
	// Если добавлена новая запись
	//
	if (_bop->obj)
		bytes_usage (_bop->object_space, _bop->obj, +1);

	//
	// Если это изменение предыдущей записи
	//
	if (_bop->old_obj)
		bytes_usage (_bop->object_space, _bop->old_obj, -1);

	//
	// Проходим по всему списку изменений, которые привязаны к
	// данной операции
	//
	struct box_phi_cell* cell;
	struct box_phi_cell* tmp;
	TAILQ_FOREACH_SAFE (cell, &_bop->phi, bop_link, tmp)
	{
		say_debug3 ("%s: cell:%p index:%d index_obj:%p TAILQ_FIRST(&index_obj->tailq):%p obj:%p",
					__func__, cell, cell->head->index->conf.n, cell->head, TAILQ_FIRST (&cell->head->tailq), cell->obj);
	}
	TAILQ_FOREACH_SAFE (cell, &_bop->phi, bop_link, tmp)
	{
		//
		// Подтверждаем изменение и удаляем его из списка,
		// привязанного к операции
		//
		phi_commit (cell);

		//
		// Удаляем изменение из памяти, оно больше не нужно
		//
		phi_cell_free (cell);
	}

	//
	// Удаляем старую версию записи, она больше не нужна, так
	// как операция полностью подтверждена
	//
	if (_bop->old_obj)
		tuple_free (_bop->old_obj);

	//
	// Статистика выполнения операций
	//
	if (cfg.box_extended_stat && (_bop->op != NOP) && (_bop->object_space->statbase > -1))
		stat_sum_static (_bop->object_space->statbase, message_to_boxstat (_bop->op), 1);
	box_stat_collect (_bop->op, 1);
}

void
box_commit (struct box_txn* _tx)
{
	@try
	{
		//
		// Здесь транзакция может находиться только в неподтверждённом состоянии
		//
		assert (_tx->state == UNDECIDED);

		//
		// Переводим транзакцию в подтверждённое состояние
		//
		_tx->state = COMMIT;

		//
		// Последовательно подтверждаем все операции транзакции
		//
		struct box_op* bop;
		TAILQ_FOREACH (bop, &_tx->ops, link)
			box_op_commit (bop);

		//
		// Завершаем транзакцию
		//
		txn_cleanup (_tx);
	}
	@catch (Error* e)
	{
		panic_exc_fmt (e, "can't handle exception after WAL write: %s", e->reason);
	}
	@catch (id e) {
		panic_exc_fmt (e, "can't handle unknown exception after WAL write");
	}
}

/**
 * @brief Отменяем операцию
 */
static void
box_op_rollback (struct box_op* _bop)
{
	say_debug3 ("%s:", __func__);

	if (!_bop->object_space)
		return;

	//
	// Проходим по всему списку изменений, которые привязаны к
	// данной операции
	//
	struct box_phi_cell* cell;
	struct box_phi_cell* tmp;
	TAILQ_FOREACH_REVERSE_SAFE (cell, &_bop->phi, phi_tailq, bop_link, tmp)
	{
		//
		// Отменяем изменение и удаляем его из списка,
		// привязанного к операции
		//
		phi_rollback (cell);

		//
		// Удаляем изменение, оно больше не нужно
		//
		phi_cell_free (cell);
	}

	//
	// Удаляем новую версию объекта, так как операция отменена
	// и новая версия больше не нужна
	//
	if (_bop->obj)
		tuple_free (_bop->obj);
}

void
box_rollback (struct box_txn* _tx)
{
	say_debug2 ("%s: txn:%i/%p state:%i", __func__, _tx->id, _tx, _tx->state);

	//
	// Здесь транзакция может находиться только в неподтверждённом состоянии
	//
	assert(_tx->state == UNDECIDED);

	//
	// Переводим транзакцию в откаченное состояние
	//
	_tx->state = ROLLBACK;

	//
	// Откатываем все операции транзакции начиная с последней и до самой первой
	//
	struct box_op* bop;
	TAILQ_FOREACH_REVERSE (bop, &_tx->ops, box_op_tailq, link)
		box_op_rollback (bop);

	//
	// Статистика по загрузке
	//
	txn_stat_cpu (_tx);

	//
	// Завершаем транзакцию
	//
	txn_cleanup (_tx);

	//
	// Дополнительная статистика для именованных транзакций
	//
	if (cfg.box_extended_stat && _tx->name)
	{
		struct tbuf buf = TBUF (NULL, 0, fiber->pool);

		tbuf_append (&buf, _tx->name, _tx->namelen);
		tbuf_append_lit (&buf, ":rollback");

		box_stat_sum_named (buf.ptr, tbuf_len (&buf), 1);
		box_stat_sum_named ("TXN:rollback", 12, 1);
	}
}

int
box_submit (struct box_txn* _tx)
{
	say_debug2 ("%s: txn:%i/%p state:%i", __func__, _tx->id, _tx, _tx->state);

	//
	// Общий размер данных всех выполненных операций
	//
	int len = 0;

	//
	// Счётчик операций с данными
	//
	int count = 0;

	//
	// Переменная цикла для прохода по всем операциям транзакции
	//
	struct box_op* bop;

	//
	// В случае единственной операции с модификацией данных в транзакции
	// в данной переменной будет сохранён указатель на эту операцию
	//
	struct box_op* single = NULL;

	//
	// Момент запуска записи в журнал
	//
	ev_tstamp submit_start = txn_stat_cpu (_tx);

	//
	// Для транзакций "только для чтения" проверяем, что список операций
	// пуст. Если он не пуст, то это ошибка разработки, поэтому падаем с
	// диагностикой и стеком
	//
	if (_tx->mode == RO)
	{
		assert (TAILQ_FIRST (&_tx->ops) == NULL);
		return 0;
	}

	//
	// Проходим по всем операциям транзакции
	//
	TAILQ_FOREACH (bop, &_tx->ops, link)
	{
		//
		// Для операции обязательно должна быть задана таблица
		//
		assert (bop->object_space);

		//
		// Если к операции привязаны данные, то увеличиваем счётчики и запоминаем
		// данную операцию на случай, если она окажется единственной с данными
		//
		if (bop->data_len > 0)
		{
			single = bop;

			len += bop->data_len;
			++count;
		}
		//
		// Иначе проверяем, что добавленные или обновлённые данные для этой операции
		// во всех индексах отсутствуют
		//
		else
		{
			struct box_phi_cell* cell;
			TAILQ_FOREACH (cell, &bop->phi, bop_link)
				assert (cell->obj == NULL);
		}

		//
		// Увеличиваем общее количество изменённых в транзакции объектов
		//
		_tx->obj_affected += bop->obj_affected;
	}

	//
	// Если никакие данные ни для одной операции не передавались, то выходим
	//
	if (count == 0)
	{
		_tx->submit = 0;
		return 0;
	}

	//
	// Если была обработана ровно одна операция с дополнительными данными
	//
	if (count == 1)
	{
		//
		// Записываем данные в журнал с кодом операции в качестве тэга
		//
		_tx->submit = [_tx->box->shard submit:single->data len:single->data_len tag:(single->op<<5)|TAG_WAL];
	}
	else
	{
		//
		// Буфер для записи данных обо всех выполненых операциях
		//
		struct tbuf* buf = tbuf_alloc (fiber->pool);

		//
		// Добавляем в буфер заголовок tlv-структуры с тэгом
		// последовательности операций
		//
		int offm = tlv_add (buf, BOX_MULTI_OP);

		//
		// Для каждой операции транзакции
		//
		TAILQ_FOREACH (bop, &_tx->ops, link)
		{
			//
			// Добавляем в буфер заголовок tlv-структуры с тэгом
			// одиночной операции
			//
			int offt = tlv_add (buf, BOX_OP);

			//
			// Добавляем в буфер код операции
			//
			tbuf_append (buf, &bop->op, sizeof (bop->op));

			//
			// Добавляем в буфер данные операции
			//
			tbuf_append (buf, bop->data, bop->data_len);

			//
			// Фиксируем размер данных tlv-структуры
			//
			tlv_end (buf, offt);
		}

		//
		// Фиксируем размер данных tlv-структуры
		//
		tlv_end (buf, offm);

		//
		// Записываем данные из буфера в журнал с тэгом tlv
		//
		_tx->submit = [_tx->box->shard submit:buf->ptr len:tbuf_len (buf) tag:tlv|TAG_WAL];
	}

	//
	// Расширенная статистика по времени записи в журнал данных транзакций
	//
	if (cfg.box_extended_stat && (submit_start != 0))
		box_stat_aggregate_named ("TXN:submit", 10, (ev_time () - submit_start)*1000);

	//
	// Если данные в журнал не были записаны
	//
	if (_tx->submit == 0)
		box_stat_collect (SUBMIT_ERROR, 1);

	//
	// Возвращаем количество записей в журнал или -1 если данные не были записаны
	//
	return _tx->submit ?: -1;
}

struct box_txn*
box_txn_alloc (int _shard_id, enum txn_mode _mode, const char* _name)
{
	/**
	 * @brief Глобальный счётчик транзакций для генерации их идентификаторов
	 */
	static int cnt = 0;

	//
	// Распределяем память под транзакцию в пуле сопрограммы и инициализируем
	// её нулями
	//
	struct box_txn* tx = p0alloc (fiber->pool, sizeof (struct box_txn));

	//
	// Инициализируем данные транзакции
	//
	tx->box   = (shard_rt + _shard_id)->shard->executor;
	tx->id    = cnt++;
	tx->mode  = _mode;
	tx->state = UNDECIDED;
	tx->fiber = fiber;
	TAILQ_INIT (&tx->ops);

	//
	// Текущая исполняемая сопрограмма ещё не должна иметь ассоциированную транзакцию
	//
	assert (fiber->txn == NULL);
	//
	// Привязываем транзакцию к сопрограмме
	//
	fiber->txn = tx;

	//
	// Если задана расширенная статистика и это именованная транзакция
	//
	if (cfg.box_extended_stat && _name && (_name[0] != '\0'))
	{
		//
		// Именуем транзакцию
		//
		tx->namelen = strlen (_name);
		tx->name    = palloc (fiber->pool, tx->namelen);
		memcpy ((void*)tx->name, _name, tx->namelen);

		//
		// Фиксируем время запуска транзакции
		//
		tx->start = ev_time ();

		say_debug2 ("%s: txn:%i/%p name:%*.s", __func__, tx->id, tx, (int)tx->namelen, tx->name);
	}
	else
	{
		say_debug2 ("%s: txn:%i/%p", __func__, tx->id, tx);
	}

	return tx;
}

#if CFG_lua_path || CFG_caml_path
void
box_proc_cb (struct netmsg_head* _wbuf, struct iproto* _request)
{
	say_debug ("%s: op:0x%02x sync:%u", __func__, _request->msg_code, _request->sync);

	//
	// Буфер, представляющий собой переданный запрос
	//
	struct tbuf req = TBUF (_request->data, _request->data_len, NULL);

	//
	// Игнорируем переданные флаги
	//
	tbuf_ltrim (&req, sizeof (u32));

	//
	// Длина имени переданной процедуры
	//
	int len = read_varint32 (&req);

	//
	// Имя переданной процедуры
	//
	const char* proc = req.ptr;

	//
	// Имя транзакции для расширенной статистики
	//
	struct tbuf txnname = TBUF (NULL, 0, fiber->pool);
	if (cfg.box_extended_stat)
	{
		//
		// Имя статистической переменной
		//
		tbuf_append_lit (&txnname, "exec_lua.");
		tbuf_append (&txnname, proc, len);

		//
		// Заменяем символы '.' на символы '-' только в имени вызываемой процедуры
		//
		char* p = txnname.ptr + sizeof ("exec_lua");
		while ((p = strchr (p + 1, '.')) != NULL)
			*p = '-';
	}

	//
	// Начинаем транзакцию, если это не реплика, то разрешаем чтение и запись
	//
	struct box_txn* tx = box_txn_alloc (_request->shard_id, RO, txnname.ptr);
	if (![tx->box->shard is_replica])
		tx->mode = RW;

	@try
	{
#if CFG_caml_path
		//
		// Запускаем Ocaml-процедуру, если она завершилась успешно, то
		// коммитим изменения и завершаем обработку процедуры
		//
		int ret = box_dispach_ocaml (_wbuf, _request);
		if (ret == 0)
		{
			box_commit (tx);
			return;
		}

#if !CFG_lua_path
		iproto_raise_fmt (ERR_CODE_ILLEGAL_PARAMS, "no such proc '%.*s'", len, proc);
#endif
#endif

#if CFG_lua_path
		//
		// Если запуск Ocaml-процедур не поддерживается или она завершилась
		// неудачно, то запускаем LUA процедуру и коммитим изменения после
		// её завершения
		//
		box_dispach_lua (_wbuf, _request);
		box_commit (tx);
#endif
	}
	@catch (Error* e)
	{
		//
		// В случае ошибок откатываем транзакцию, печатаем сообщение и
		// пробрасываем ошибку дальше
		//
		box_rollback (tx);

		say_warn ("aborting proc request, [%s reason:\"%s\"] at %s:%d", [[e class] name], e->reason, e->file, e->line);
		if (e->backtrace)
			say_debug ("backtrace:\n%s", e->backtrace);
		@throw;
	}
	@finally
	{
		box_stat_collect (EXEC_LUA, 1);
	}
}
#endif

void
box_cb (struct netmsg_head* _wbuf, struct iproto* _request)
{
	say_debug2 ("%s: c:%p op:0x%02x sync:%u", __func__, NULL, _request->msg_code, _request->sync);

	//
	// Создаём транзакцию для выполнения команд
	//
	struct box_txn* tx = box_txn_alloc (_request->shard_id, RW, box_op_name (_request->msg_code));

	//
	// Выполненная операция
	//
	// FIXME: пока возможно выполнение только одной команды на транзакцию, хотя
	//        сама структура данных разработана для поддержки нескольких команд
	//        на транзакцию. Во всех остальных местах соответствующие изменения
	//        внесены, осталось доработать протокол и здесь добавить обработку
	//        нескольких команд
	//
	struct box_op* bop = NULL;
	@try
	{
		//
		// Для реплики команды модификации не доступны, её можно только читать
		//
		if ([tx->box->shard is_replica])
			iproto_raise (ERR_CODE_NONMASTER, "replica is readonly");

		//
		// Выполняем операцию
		//
		bop = box_prepare (tx, _request->msg_code, _request->data, _request->data_len);
		if (!bop->object_space)
			iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "ignored object space");

		//
		// Здесь все данные уже успешно изменены, поэтому пишем информацию об изменениях
		// в журнал
		//
		if (box_submit (tx) == -1)
			iproto_raise(ERR_CODE_UNKNOWN_ERROR, "unable write wal row");

		//
		// Конструируем ответ сервера на запрос
		//
		struct iproto_retcode* reply = iproto_reply (_wbuf, _request, ERR_CODE_OK);

		//
		// Добавляем в ответ количество изменённых записей
		//
		net_add_iov_dup (_wbuf, &tx->obj_affected, sizeof (u32));

		//
		// Если установлен флаг запроса записи как результата выполнения операции и есть
		// что возвращать, то добавляем запись в возвратный буфер
		//
		if ((bop->flags & BOX_RETURN_TUPLE) && bop->ret_obj)
			net_tuple_add (_wbuf, bop->ret_obj);

		//
		// Закрываем ответ (фиксируем размер данных)
		//
		iproto_reply_fixup (_wbuf, reply);

		//
		// Фиксируем транзакцию
		//
		box_commit (tx);
	}
	@catch (Error* e)
	{
		//
		// Пишем информацию в лог если для исключения задан файл исходного кода
		// и это не src/paxos.m
		//
		if (e->file && (strcmp (e->file, "src/paxos.m") != 0))
		{
			say_warn ("aborting txn, [%s reason:\"%s\"] at %s:%d peer:%s",
				 [[e class] name], e->reason, e->file, e->line,
				 net_fd_name (container_of (_wbuf, struct netmsg_io, wbuf)->fd));

			if (e->backtrace)
				say_debug ("backtrace:\n%s", e->backtrace);
		}

		//
		// Откатываем транзакцию
		//
		box_rollback (tx);

		//
		// Пробрасываем исключение выше по стеку
		//
		@throw;
	}
}

void
box_select_cb (struct netmsg_head* _wbuf, struct iproto* _request)
{
	//
	// Блок данных для выборки
	//
	Box* box = (shard_rt + _request->shard_id)->shard->executor;

	//
	// Запрос
	//
	struct tbuf data = TBUF (_request->data, _request->data_len, fiber->pool);
	//
	// Структура для формирования ответа на запрос
	//
	struct iproto_retcode* reply = iproto_reply (_wbuf, _request, ERR_CODE_OK);

	//
	// Параметры запроса: номер таблицы для выборки, номер индекса для выборки
	// смещение окна данных, размер окна данных, общее число запросов
	//
	i32 n      = read_u32 (&data);
	u32 indexn = read_u32 (&data);
	u32 offset = read_u32 (&data);
	u32 limit  = read_u32 (&data);
	u32 count  = read_u32 (&data);

	//
	// Таблица для выборки
	//
	struct object_space* osp = object_space (box, n);

	ev_tstamp start = 0;
	@try
	{
		//
		// Выход номера индекса за пределы
		//
		if (indexn > MAX_IDX)
			iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "index too big");

                if (cfg.box_extended_stat && (osp->statbase > -1))
			stat_sum_static (osp->statbase, BSS_SELECT_IDX0+indexn, 1);

		//
		// Индекс для выборки
		//
		Index<BasicIndex>* index = osp->index[indexn];
		if (index == NULL)
			iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "index is invalid");

		//
		// Статистика по количеству запрошенных ключей
		//
		box_stat_collect (SELECT_KEYS, count);
		//
		// Для расширенной статистики
		//
		if (cfg.box_extended_stat && (osp->statbase > -1))
		{
			//
			// Статистика по использованию индекса
			//
			stat_sum_static (osp->statbase, BSS_SELECT_KEYS_IDX0+indexn, count);

			//
			// Если число запросов для выборки больше одного или индекс не
			// является хэшем
			//
			bool is_hash = index_type_is_hash (index->conf.type);
			if (!((count == 1) && is_hash))
				start = ev_time ();
		}

		//
		// Выполняем запрос
		//
		u32 found = process_select (_wbuf, index, limit, offset, count, &data);
		//
		// Фиксируем ответ
		//
		iproto_reply_fixup (_wbuf, reply);

		//
		// Статистика
		//
		box_stat_collect (SELECT_TUPLES, found);
		if  (cfg.box_extended_stat && (osp->statbase > -1))
			stat_sum_static (osp->statbase, BSS_SELECT_TUPLES_IDX0+indexn, found);
	}
	@catch (id e)
	{
		char statname[] = "SELECT_ERR_000\0";
		int len = sprintf (statname, "SELECT_ERR_%d", n);
		box_stat_sum_named (statname, len, 1);

		@throw;
	}
	@finally
	{
		//
		// Если была задана статистика по времени выполнения запросов
		//
		if (start != 0)
		{
			double diff = (ev_time () - start)*1000;
			if (cfg.box_extended_stat && (osp->statbase > -1))
				stat_aggregate_static (osp->statbase, BSS_SELECT_TIME_IDX0 + indexn, diff);
			box_stat_collect_double (SELECT_TIME, diff);
		}

		//
		// Статистика по кодам запросов
		//
		box_stat_collect (_request->msg_code&0xffff, 1);
	}
}

register_source ();
