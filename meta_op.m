/*
 * Copyright (C) 2015, 2016 Mail.RU
 * Copyright (C) 2015, 2016 Yuriy Vostrikov
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
#include <stdint.h>

#import <pickle.h>
#import <say.h>

#import <mod/box/tuple_index.h>
#import <mod/box/meta_op.h>

/**
 * @brief Создание таблицы
 *
 * Таблица создаётся в транзакции и будет перенесена в основной
 * массив таблиц только при её завершении. Это позволяет сначала
 * создать таблицу и все её индексы и таким образом выполнить все
 * проверки и только затем перенести её в основной массив. Если на
 * каком-то шаге возникнет ошибка создания таблицы, то она будет
 * удалена вместе с данными транзакции и база автоматически вернётся
 * в состояние до транзакции.
 */
static void __attribute__((noinline))
prepare_create_object_space (struct box_meta_txn* _tx, int _n, struct tbuf* _data)
{
	say_debug ("%s n:%i", __func__, _n);

	//
	// Количество полей таблицы
	//
	char cardinalty = read_u8 (_data);

	//
	// Читаем конфигурацию первичного индекса
	//
	struct index_conf ic = {.n = 0};
	index_conf_read (_data, &ic);
	index_conf_validate (&ic);

	//
	// Проверяем корректность номера таблицы
	//
	if ((_n < 0) || (_n > nelem (_tx->box->object_space_registry) - 1))
		iproto_raise_fmt (ERR_CODE_ILLEGAL_PARAMS, "bad namespace number %i", _n);

	//
	// Проверяем существование таблицы с заданным номером
	//
	if (_tx->box->object_space_registry[(int)_n])
		iproto_raise_fmt (ERR_CODE_ILLEGAL_PARAMS, "object_space %i is exists", _n);

	//
	// Первичный индекс должен быть уникальным
	//
	if (!ic.unique)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "primary index must be unique");

	//
	// Первичный индекс не может быть частичным
	//
	if (ic.notnull)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "primary index can't be notnull");

	//
	// Создаём первичный индекс таблицы
	//
	_tx->index = [Index new_conf:&ic dtor:box_tuple_dtor ()];
	if (_tx->index == NULL)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "can't create index");

	//
	// Создаём таблицу с привязкой к ней первичного индекса
	//
	_tx->object_space              = xcalloc (1, sizeof (struct object_space));
	_tx->object_space->n           = _n;
	_tx->object_space->cardinality = cardinalty;
	_tx->object_space->snap        = _tx->flags&1;
	_tx->object_space->wal         = _tx->flags&2;
	_tx->object_space->index[0]    = _tx->index;

	_tx->object_space->statbase = -1;
	if (cfg.box_extended_stat)
		object_space_fill_stat_names (_tx->object_space);

	//
	// Проверяем корректность созданной таблицы
	//
	assert (_tx->object_space->snap);
	assert (_tx->object_space->wal);
}

/**
 * @brief Создание индекса
 */
static void __attribute__((noinline))
prepare_create_index (struct box_meta_txn* _tx, struct tbuf* _data)
{
	say_debug ("%s", __func__);

	//
	// Читаем конфигурацию индекса
	//
	struct index_conf ic = {.n = read_i8(_data)};
	index_conf_read (_data, &ic);
	index_conf_validate (&ic);

	//
	// Проверяем существование индекса
	//
	if (_tx->object_space->index[(int)ic.n])
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "index already exists");

	//
	// Проверяем что это вторичный индекс и дополняем его полями первичного
	// индекса для того, чтобы можно было однозначно идентифицировать объекты
	// в нём
	//
	if ((ic.n > 0) && !ic.unique)
		index_conf_merge_unique (&ic, &_tx->object_space->index[0]->conf);

	//
	// Создаём индекс таблицы
	//
	_tx->index = [Index new_conf:&ic dtor:box_tuple_dtor ()];
	if (_tx->index == nil)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "can't create index");

	//
	// Если данных в таблице нет, то больше ничего делать не надо
	//
	Index<BasicIndex>* pk = _tx->object_space->index[0];
	if ([pk size] == 0)
		return;

	//
	// Если для индекса поддерживается операция задания предварительно
	// отсортированных объектов
	//
	struct tnt_object* obj = NULL;
	if ([_tx->index respondsTo:@selector(set_sorted_nodes:count:)])
	{
		//
		// Число объектов в таблице
		//
		size_t n_tuples = [pk size];

		//
		// Размер узла с которым работает индекс
		//
		int n_size = _tx->index->node_size;

		//
		// Распределяем память под все узлы индекса сразу
		//
		void* nodes = xmalloc (n_tuples*n_size);

		//
		// Идём по всем записям таблицы (по первичному индексу)
		//
		int t = 0;
		[pk iterator_init];
		while ((obj = [pk iterator_next]))
		{
			//
			// Проверяем, что объект должен быть включён в индекс
			//
			if (tuple_match (&ic, obj))
			{
				//
				// Указатель на место в памяти под новый узел
				//
				struct index_node* node = nodes + t*n_size;

				//
				// Заполняем узел данными о записи
				//
				_tx->index->dtor (obj, node, _tx->index->dtor_arg);

				//
				// Число инициализированных узлов индекса
				//
				++t;
			}
		}

		say_debug ("n_tuples:%i, indexed:%i", (int)n_tuples, t);

		//
		// Сортируем узлы индекса с одновременным поиском и печатью дубликатов
		//
		struct print_dups_arg arg = {.space = _tx->object_space->n, .index = ic.n};
		if (![(Tree*)_tx->index sort_nodes:nodes count:t onduplicate:box_idx_print_dups arg:(void*)&arg])
		{
			free (nodes);

			iproto_raise (ERR_CODE_INDEX_VIOLATION, "duplicate values for unique index");
		}

		//
		// Создаём индекс на основе предварительно отсортированных узлов
		//
		[(Tree*)_tx->index set_sorted_nodes:nodes count:t];
	}
	else
	{
		//
		// Если функция включения в индекс предварительно отсортированных узлов не
		// поддерживается, то просто вставляем записи в новый индекс из первичного
		//
		[pk iterator_init];
		while ((obj = [pk iterator_next]))
		{
			if (tuple_match (&ic, obj))
				[_tx->index replace:obj];
		}
	}
}

/**
 * @brief Подготовить индекс для удаления
 */
static void __attribute__((noinline))
prepare_drop_index (struct box_meta_txn* _tx, struct tbuf* _data)
{
	say_debug ("%s", __func__);

	//
	// Номер удаляемого индекса
	//
	int i = read_i8 (_data);

	//
	// Индекс должен попадать в диапазон
	//
	if ((i < 0) || (i > nelem (_tx->object_space->index)))
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "bad index num");

	//
	// Первичный индекс удалять нельзя
	//
	if (i == 0)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "can't drop primary key");

	//
	// Индекс для удаления
	//
	_tx->index = _tx->object_space->index[i];

	//
	// Проверяем наличие индекса для удаления
	//
	if (_tx->index == NULL)
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "attemp to drop non existent index");
}

/**
 * @brief Удалить все данные из заданной таблицы
 */
static int
box_meta_truncate_osp (struct object_space* _osp)
{
	struct tnt_object* obj = NULL;

	//
	// Первичный индекс, по которому делаем проход для удаления
	// записей
	//
	id<BasicIndex> pk = _osp->index[0];

	//
	// Счётчик удалённых записей
	//
	int count = 0;

	//
	// Проходим по первичному индексу
	//
	[pk iterator_init];
	while ((obj = [pk iterator_next]))
	{
		//
		// Объект должен представлять собой обычную запись
		//
		assert (tuple_visible_left (obj) == obj);

		//
		//  Удаляем запись из памяти
		//
		tuple_free (obj);

		//
		// Считаем удалённые объекты
		//
		++count;
	}

	//
	// Проходим по всем индексам и очищаем их
	//
	foreach_index (index, _osp)
		[index clear];

	return count;
}

int
box_meta_truncate (int _id, int _n)
{
	say_info ("TRUNCATE shard id:%i object_space n:%i", _id, _n);

	//
	// Удаление данных с помощью данной процедуры необходимо выполнять вне
	// транзакции модификации данных
	//
	// Таким образом данную функцию можно вызывать только из LUA-поцедур,
	// которые выполняются через административный telnet-интерфейс. Этим
	// мы гарантируем целостность данных
	//
	if (fiber->txn != NULL)
		return -1;

	//
	// Проверяем корректность идентификатора шарда
	//
	if ((_id < 0) || (_id >= nelem (shard_rt)))
		return -2;

	//
	// Проверяем, что шард с заданным идентификатором корректен
	//
	if (shard_rt[_id].shard == NULL)
		return -3;

	//
	// Модуль для заданного шарда
	//
	Box* box = shard_rt[_id].shard->executor;
	if (box == NULL)
		return -4;

	//
	// Для реплик полное удаление данных из таблицы не поддерживается
	//
	if ([box->shard is_replica])
		return -5;

	//
	// Таблица
	//
	struct object_space* osp = object_space (box, _n);
	if (osp == NULL)
		return -6;

	//
	// Записываем изменения в журнал
	//
	{
		//
		// Пакуем параметры команды в буфер
		//
		// Используем функции упаковки вместо простого сохранения в массив
		// чтобы гарантировать, что при чтении с помощью функций read_u32
		// данные будут корректно распакованы
		//
		u32 data[/*_n*/sizeof (u32) + /*flags*/sizeof (u32)] = {0};
		struct tbuf buf = TBUF_BUF (data);
		write_u32 (&buf, _n);
		write_u32 (&buf,  0);

		//
		// Пишем буфер в журнал
		//
		if ([box->shard submit:buf.ptr len:tbuf_len (&buf) tag:(TRUNCATE<<5)|TAG_WAL] != 1)
		{
			box_stat_collect (SUBMIT_ERROR, 1);
			return -7;
		}
	}

	//
	// Удаляем все данные из таблицы
	//
	return box_meta_truncate_osp (osp);
}

void
box_meta_prepare (struct box_meta_txn* _tx, struct tbuf* _data)
{
	//
	// Таблица для модификации
	//
	i32 n = read_u32 (_data);

	//
	// Флаги операции
	//
	_tx->flags = read_u32 (_data);

	//
	// В зависимости от кода операции
	//
	switch (_tx->op)
	{
		case CREATE_OBJECT_SPACE:
			prepare_create_object_space (_tx, n, _data);
			break;

		case CREATE_INDEX:
			_tx->object_space = object_space (_tx->box, n);
			prepare_create_index (_tx, _data);
			break;

		case DROP_OBJECT_SPACE:
		case TRUNCATE:
			_tx->object_space = object_space (_tx->box, n);
			break;

		case DROP_INDEX:
			_tx->object_space = object_space (_tx->box, n);
			prepare_drop_index (_tx, _data);
			break;

		default:
			raise_fmt ("unknown op");
	}
}

/**
 * @brief Связать все индексы из массива в список, привязанный
 *        к первому индексу
 */
static void
link_index (struct object_space* _osp)
{
	Index* index = _osp->index[0];
	for (int i = 1; i < nelem (_osp->index); ++i)
	{
		Index* next = _osp->index[i];
		if (next)
		{
			index->next = next;
			index = next;
		}
	}

	index->next = nil;
}

void
box_meta_commit (struct box_meta_txn* _tx)
{
	switch (_tx->op)
	{
		case CREATE_OBJECT_SPACE:
			say_info ("CREATE object_space n:%i 0:%s", _tx->object_space->n, [_tx->object_space->index[0] info]);

			//
			// Переносим созданную таблицу в основной массив
			//
			_tx->box->object_space_registry[_tx->object_space->n] = _tx->object_space;
			break;

		case CREATE_INDEX:
			say_info ("CREATE index n:%i %i:%s", _tx->object_space->n, _tx->index->conf.n, [_tx->index info]);

			//
			// Переносим созданный индекс в основной массив
			//
			_tx->object_space->index[(int)_tx->index->conf.n] = _tx->index;
			//
			// Заново связываем индексы в список включая туда новый индекс
			//
			link_index (_tx->object_space);
			break;

		case DROP_INDEX:
			say_info ("DROP index n:%i %i", _tx->object_space->n, _tx->index->conf.n);

			//
			// Убираем индекс из основного массива
			//
			_tx->object_space->index[(int)_tx->index->conf.n] = NULL;
			//
			// Удаляем индекс из памяти
			//
			[_tx->index free];
			//
			// Заново связываем индексы в список исключая оттуда удалённый
			// индекс
			//
			link_index (_tx->object_space);
			break;

		case DROP_OBJECT_SPACE:
			say_info ("DROP object_space n:%i", _tx->object_space->n);

			//
			// Удаляем данные из таблицы
			//
			box_meta_truncate_osp (_tx->object_space);

			//
			// Проходим по всему массиву индексов и удаляем их
			//
			for (int i = 0; i < nelem (_tx->object_space->index); ++i)
			{
				if (_tx->object_space->index[i])
					[_tx->object_space->index[i] free];
			}

			//
			// Удаляем таблицу из списка
			//
			_tx->box->object_space_registry[_tx->object_space->n] = NULL;
			//
			// Очищаем статистику
			//
			object_space_clear_stat_names (_tx->object_space);
			//
			// Окончательно очищаем память, которую занимала таблица
			//
			free (_tx->object_space);
			break;

		case TRUNCATE:
			say_info ("TRUNCATE object_space n:%i", _tx->object_space->n);

			//
			// Удаляем данные из таблицы
			//
			box_meta_truncate_osp (_tx->object_space);
			break;

		default:
			assert (0);
	}

	++_tx->box->version;
}

void
box_meta_rollback (struct box_meta_txn* _tx)
{
	switch (_tx->op)
	{
		//
		// Удаляем ранее распределённую память под таблицу
		//
		case CREATE_OBJECT_SPACE:
			object_space_clear_stat_names (_tx->object_space);
			free (_tx->object_space);
			_tx->object_space = NULL;

		//
		// Удаляем ранее распределённую память под индекс, работает
		// как для команды создания индекса, так и для команды
		// создания таблицы для удаления её первичного индекса, поэтому
		// оператор break у предыдущего case отсутствует
		//
		case CREATE_INDEX:
			[_tx->index free];
			_tx->index = NULL;
	}
}

void
box_meta_cb (struct netmsg_head* _wbuf, struct iproto* _request)
{
	say_debug2 ("%s: op:0x%02x sync:%u", __func__, _request->msg_code, _request->sync);

	//
	// Выполнение мета-команд необходимо выполнять вне транзакции модификации
	// данных. В текущей реализации это программная ошибка, создать такую
	// ситуацию через интерфейс должно быть невозможно
	//
	assert (fiber->txn == NULL);

	//
	// Модуль, для которого вызвана процедура изменения мета-информации
	//
	Box* box = shard_rt[_request->shard_id].shard->executor;

	//
	// Создаём мета-транзакцию
	//
	struct box_meta_txn tx = {.op = _request->msg_code, .box = box};

	//
	// Для реплик изменение мета-информации не поддерживается
	//
	if ([box->shard is_replica])
		iproto_raise (ERR_CODE_NONMASTER, "replica is readonly");

	//
	// Блокируем изменение метаданных для сконфигурированных пространств имён
	//
	if (box->shard->dummy  && (tx.op != TRUNCATE))
		iproto_raise (ERR_CODE_ILLEGAL_PARAMS, "metadata updates are forbidden because cfg.object_space is configured");

	//
	// Если мета-информация задана в конфигурации, то её изменение не поддерживается
	//
	if (cfg.object_space  && (tx.op != TRUNCATE))
		say_warn ("metadata updates with configured cfg.object_space");

	@try
	{
		//
		// Подготовка выполнения мета-команды
		//
		box_meta_prepare (&tx, &TBUF (_request->data, _request->data_len, NULL));

		//
		// Записываем изменения в журнал
		//
		if ([box->shard submit:_request->data len:_request->data_len tag:(_request->msg_code<<5)|TAG_WAL] != 1)
		{
			box_stat_collect (SUBMIT_ERROR, 1);
			iproto_raise (ERR_CODE_UNKNOWN_ERROR, "unable write wal row");
		}
	}
	@catch (id e)
	{
		//
		// В случае ошибок откатываем транзакцию
		//
		box_meta_rollback (&tx);
		@throw;
	}

	@try
	{
		//
		// Подтверждаем выполнение команды
		//
		box_meta_commit (&tx);

		iproto_reply_small (_wbuf, _request, ERR_CODE_OK);
	}
	@catch (Error* e)
	{
		panic_exc_fmt (e, "can't handle exception after WAL write: %s", e->reason);
	}
	@catch (id e)
	{
		panic_exc_fmt (e, "can't handle unknown exception after WAL write");
	}
}

register_source ();
