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
#import <fiber.h>
#import <iproto.h>
#import <log_io.h>
#import <say.h>
#import <stat.h>
#import <octopus.h>
#import <tbuf.h>
#import <util.h>
#import <objc.h>
#import <index.h>
#import <spawn_child.h>
#import <shard.h>

#import <mod/box/op.h>
#import <mod/box/meta_op.h>
#import <mod/box/box.h>
#import <mod/box/src-lua/moonbox.h>
#import <mod/box/box_version.h>
#import <mod/box/print.h>
#import <mod/box/tuple_index.h>
#import <mod/feeder/feeder.h>

#if CFG_lua_path
#import <src-lua/octopus_lua.h>
#endif

#include <third_party/crc32.h>

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sysexits.h>
#include <time.h>

static struct iproto_service box_primary;
static struct iproto_service box_secondary;

#define foreach_op(...) for (int* op = (int[]){__VA_ARGS__, 0}; *op; ++op)

/**
 * @brief Инициализация обработчиков запросов
 */
static void
box_service (struct iproto_service* s)
{
	//
	// Регистрируем обработчики выборок
	//
	foreach_op (NOP, SELECT, SELECT_LIMIT)
		service_register_iproto (s, *op, box_select_cb, IPROTO_NONBLOCK);

	//
	// Регистрируем обработчики команд модификации данных
	//
	foreach_op (INSERT, UPDATE_FIELDS, DELETE, DELETE_1_3)
		service_register_iproto (s, *op, box_cb, IPROTO_ON_MASTER);

	//
	// Регистрируем обработчики команд модификации мета-информации
	//
	foreach_op (CREATE_OBJECT_SPACE, CREATE_INDEX, DROP_OBJECT_SPACE, DROP_INDEX, TRUNCATE)
		service_register_iproto (s, *op, box_meta_cb, IPROTO_ON_MASTER|IPROTO_WLOCK);

#if CFG_lua_path || CFG_caml_path
	//
	// allow select only lua procedures updates are blocked by luaT_box_dispatch()
	//
	service_register_iproto (s, EXEC_LUA, box_proc_cb, 0);
#endif
}

/**
 * @brief Обработчик изменений БД для сервиса только-чтение
 */
static void
box_roerr (struct netmsg_head* _h __attribute__((unused)), struct iproto* _request __attribute__((unused)))
{
	iproto_raise (ERR_CODE_NONMASTER, "updates are forbidden");
}

/**
 * @brief Инициализация обработчиков запросов для систем только-чтение
 */
static void
box_service_ro (struct iproto_service* _s)
{
	service_register_iproto (_s, SELECT, box_select_cb, IPROTO_NONBLOCK);
	service_register_iproto (_s, SELECT_LIMIT, box_select_cb, IPROTO_NONBLOCK);

	foreach_op (INSERT, UPDATE_FIELDS, DELETE, DELETE_1_3, PAXOS_LEADER,
				CREATE_OBJECT_SPACE, CREATE_INDEX, DROP_OBJECT_SPACE, DROP_INDEX, TRUNCATE)
		service_register_iproto (_s, *op, box_roerr, IPROTO_NONBLOCK);

#if CFG_lua_path || CFG_caml_path
	//
	// allow select only lua procedures updates are blocked by luaT_box_dispatch()
	//
	service_register_iproto (_s, EXEC_LUA, box_proc_cb, 0);
#endif
}

/**
 * @brief Инициализация базовых служб и сопроцедур
 */
static void
initialize_primary_service ()
{
	say_info ("box %s (%i workers)", __func__, cfg.wal_writer_inbox_size);

	//
	// Инициализация службы приёма и диспетчеризации запросов
	//
	iproto_service (&box_primary, cfg.primary_addr);
	box_primary.options = SERVICE_SHARDED;

	//
	// Настройка сервиса обработки запросов
	//
	box_service (&box_primary);
	//
	// Настройка сервиса репликации
	//
	feeder_service (&box_primary);

	//
	// Запускаем сопроцедуры записи журнала на диск
	//
	for (int i = 0; i < MAX (1, cfg.wal_writer_inbox_size); ++i)
		fiber_create ("box_worker", iproto_worker, &box_primary);
}

/**
 * @brief Инициализация дополнительных служб и сопроцедур
 */
static void
initialize_secondary_service ()
{
	//
	// Если задан дополнительный адрес обработки запросов и он не совпадает с первичным
	//
	if ((cfg.secondary_addr != NULL) && (strcmp (cfg.secondary_addr, cfg.primary_addr) != 0))
	{
		say_info ("box %s", __func__);

		iproto_service (&box_secondary, cfg.secondary_addr);
		box_secondary.options = SERVICE_SHARDED;

		box_service_ro (&box_secondary);
		fiber_create   ("box_secondary_worker", iproto_worker, &box_secondary);
	}
}

/**
 * @brief Вторая часть инициализации
 */
static void
init_second_stage (va_list _ap __attribute__((unused)))
{
#if CFG_lua_path
	//
	// Инициализация подсистемы запуска Lua-скриптов
	//
	luaT_openbox (root_L);
	luaO_require_or_panic ("box_init", false, NULL);
#endif

#if CFG_caml_path
	//
	// Инициализация подсистемы запуска Ocaml-скриптов
	//
	extern void oct_caml_plugins ();
	oct_caml_plugins ();
#endif

	if (box_primary.name != NULL)
		[recovery simple:&box_primary];
	else
		[recovery simple:NULL];
}

/**
 * @brief Статистика по использованию памяти
 */
static void
stat_mem_callback (int _base _unused_)
{
	char cbuf[64];
	struct tbuf buf = TBUF_BUF (cbuf);

	for (int i = 0; i < MAX_SHARD; ++i)
	{
		id<Shard> shard = [recovery shard:i];
		if (shard == nil)
			continue;

		id<Executor> exec = [shard executor];
		if (![(id)exec isKindOf: [Box class]])
			continue;

		Box* box = exec;
		for (uint32_t n = 0; n < nelem(box->object_space_registry); ++n)
		{
			if (box->object_space_registry[n] == NULL)
				continue;

			struct object_space* osp = box->object_space_registry[n];

			//
			// Общая часть имён
			//
			tbuf_reset      (&buf);
			tbuf_append_lit (&buf, "space_");
			tbuf_putu       (&buf, n);
			tbuf_putc       (&buf, '@');
			tbuf_putu       (&buf, i);
			size_t len = tbuf_len (&buf);

			tbuf_append_lit (&buf, "_objects");
			stat_report_gauge (buf.ptr, tbuf_len(&buf), [osp->index[0] size]);

			tbuf_reset_to   (&buf, len);
			tbuf_append_lit (&buf, "_obj_bytes");
			stat_report_gauge (buf.ptr, tbuf_len(&buf), osp->obj_bytes);

			tbuf_reset_to   (&buf, len);
			tbuf_append_lit (&buf, "_slab_bytes");
			stat_report_gauge(buf.ptr, tbuf_len(&buf), osp->slab_bytes);

			//
			// Общая часть имён для вывода индексов
			//
			tbuf_reset_to   (&buf, len);
			tbuf_append_lit (&buf, "_ix_");
			len = tbuf_len (&buf);

			foreach_index (index, osp)
			{
				tbuf_reset_to   (&buf, len);
				tbuf_putu       (&buf, index->conf.n);
				tbuf_append_lit (&buf, "_bytes");
				stat_report_gauge (buf.ptr, tbuf_len(&buf), [index bytes]);
			}
		}
	}
}

/**
 * @brief Первая часть инициализации, которая непосредственно
 *        вызывается для инициализации модуля
 */
static void
init (void)
{
	title ("loading");

	recovery = [[Recovery alloc] init];
	recovery->default_exec_class = [Box class];

	//
	// Если задана начальная инициализация хранилища
	//
	if (init_storage)
	{
		if (cfg.object_space)
			[recovery shard_create_dummy:NULL];

		return;
	}

	//
	// Инициализация системы управления памятью для box_phi и box_phi_cell
	//
	phi_cache_init ();

	//
	// Инициализация сбора статистики
	//
	box_stat_init ();

	//
	// Регистрация процедуры сбора статистики использования памяти
	//
	stat_register_callback ("box_mem", stat_mem_callback);

	//
	// Инициализация сервисов
	//
	if (cfg.object_space == NULL)
	{
		initialize_primary_service ();

		initialize_secondary_service ();
	}

	//
	// Второй этап инициализации должен выполняться в сопроцедуре для
	// нормальной работы получения данных с удалённого сервера
	//
	fiber_create ("box_init", init_second_stage);
}

/**
 * @brief Вывод информации о сервисе
 */
static void
info (struct tbuf* _out, const char* _what)
{
	extern Recovery* recovery;

	if (_what == NULL)
	{
		tbuf_printf (_out, "info:" CRLF);
		tbuf_printf (_out, "  version: \"%s\"" CRLF, octopus_version ());
		tbuf_printf (_out, "  uptime: %i" CRLF, tnt_uptime ());
		tbuf_printf (_out, "  pid: %i" CRLF, getpid ());
		tbuf_printf (_out, "  lsn: %" PRIi64 CRLF, [recovery lsn]);
		tbuf_printf (_out, "  shards:" CRLF);

		for (int i = 0; i < MAX_SHARD; ++i)
		{
			id<Shard> shard = [recovery shard:i];
			if (shard == nil)
				continue;

			tbuf_printf (_out, "  - shard_id: %i" CRLF, i);
			tbuf_printf (_out, "    scn: %" PRIi64 CRLF, [shard scn]);
			tbuf_printf (_out, "    status: %s%s%s" CRLF, [shard status],
							cfg.custom_proc_title ? "@" : "", cfg.custom_proc_title ?: "");

			if ([shard is_replica])
			{
				tbuf_printf (_out, "    recovery_lag: %.3f" CRLF, [shard lag]);
				tbuf_printf (_out, "    recovery_last_update: %.3f" CRLF, [shard last_update_tstamp]);
				if (!cfg.ignore_run_crc)
					tbuf_printf (_out, "    recovery_run_crc_status: %s" CRLF, [shard run_crc_status]);
			}

			Box* box = [shard executor];
			tbuf_printf (_out, "    namespaces:" CRLF);
			for (uint32_t n = 0; n < nelem(box->object_space_registry); ++n)
			{
				if (box->object_space_registry[n] == NULL)
					continue;

				struct object_space* osp = box->object_space_registry[n];
				tbuf_printf (_out, "    - n: %i"CRLF, n);
				tbuf_printf (_out, "      objects: %i"CRLF, [osp->index[0] size]);
				tbuf_printf (_out, "      obj_bytes: %zi"CRLF, osp->obj_bytes);
				tbuf_printf (_out, "      slab_bytes: %zi"CRLF, osp->slab_bytes);
				tbuf_printf (_out, "      indexes:"CRLF);

				foreach_index (index, osp)
					tbuf_printf (_out, "      - { index: %i, slots: %i, bytes: %zi }" CRLF,
									index->conf.n, [index slots], [index bytes]);
			}
		}

		tbuf_printf (_out, "  config: \"%s\""CRLF, cfg_filename);
	}

	else if (strcmp (_what, "net") == 0)
	{
		if (box_primary.name != NULL)
			iproto_service_info (_out, &box_primary);

		if (box_secondary.name != NULL)
			iproto_service_info (_out, &box_secondary);
	}
}

/**
 * @brief Проверка конфигурации
 */
static int
check_config (struct octopus_cfg* _new)
{
	extern void out_warning (int _v, char* _format, ...);

	struct feeder_param feeder;
	enum feeder_cfg_e e = feeder_param_fill_from_cfg (&feeder, _new);

	bool errors = false;
	if (e)
	{
		out_warning (0, "wal_feeder config is wrong");
		errors = true;
	}

	if (_new->object_space != NULL)
	{
		for (int i = 0; i < OBJECT_SPACE_MAX; ++i)
		{
			if (_new->object_space[i] == NULL)
				break;

			if (!CNF_STRUCT_DEFINED (_new->object_space[i]))
				continue;

			if (_new->object_space[i]->index == NULL)
			{
				out_warning (0, "(object_space = %" PRIu32 ") at least one index must be defined", i);
				errors = true;
			}

			for (int j = 0; j < MAX_IDX; ++j)
			{
				if (_new->object_space[i]->index[j] == NULL)
					break;

				if (!CNF_STRUCT_DEFINED (_new->object_space[i]->index[j]))
					continue;

				struct index_conf* ic = cfg_box2index_conf (_new->object_space[i]->index[j], i, j, 0);
				if (ic == NULL)
					errors = true;
				else
					free (ic);
			}
		}
	}

	if (_new->on_snapshot_duplicates)
	{
		for (int i = 0; _new->on_snapshot_duplicates[i]; ++i)
		{
			if (!CNF_STRUCT_DEFINED (_new->on_snapshot_duplicates[i]))
				continue;

			__typeof__ (_new->on_snapshot_duplicates[i]->index) indexes = _new->on_snapshot_duplicates[i]->index;
			if (indexes == NULL)
			{
				out_warning (0, "no indexes for on_snapshot_duplicates[%d]", i);
				errors = true;
			}
			else
			{
				for (int j = 0; indexes[j]; ++j)
				{
					if (!CNF_STRUCT_DEFINED (indexes[j]))
						continue;

#define eq(t, s) (strcmp((t),(s)) == 0)
					const char* action = indexes[j]->action;
					if (!action || !(eq (action, "DELETE") || !eq (action, "IGNORE")))
						out_warning (0, "on_snapshot_duplicates[%d].index[%d].action unknown (=\"%s\")",
										i, j, action ?: "<null>");
#undef eq
				}
			}
		}
	}

	return errors ? -1 : 0;
}

/**
 * @brief Перезагрузка конфигурации
 */
static void
reload_config (struct octopus_cfg* _old __attribute__((unused)), struct octopus_cfg* _new __attribute__((unused)))
{
	Shard<Shard>* shard = [recovery shard:0];
	if ((shard == NULL) || !shard->dummy)
	{
		say_error ("ignoring legacy configuration request");
		return;
	}

	if ([(id)shard respondsTo:@selector (adjust_route)])
		[(id)shard perform:@selector (adjust_route)];
	else
		say_error ("ignoring unsupported configuration request");
}

Box*
shard_box ()
{
	return ((struct box_txn*)fiber->txn)->box;
}

int
shard_box_id ()
{
	return shard_box ()->shard->id;
}

int
box_version ()
{
	return shard_box ()->version;
}

int
box_shard_id (Box* _box)
{
	if (_box == NULL)
		return -1;

	return _box->shard->id;
}

struct object_space*
object_space (Box* _box, int _n)
{
	//
	// Проверяем выход индекса таблицы за границы допустимого диапазона
	//
	if ((_n < 0) || (_n > nelem (_box->object_space_registry) - 1))
		iproto_raise_fmt (ERR_CODE_ILLEGAL_PARAMS, "bad namespace number %i", _n);

	//
	// Проверяем наличие заданной таблицы
	//
	if (!_box->object_space_registry[_n])
		iproto_raise_fmt (ERR_CODE_ILLEGAL_PARAMS, "object_space %i is not enabled", _n);

	return _box->object_space_registry[_n];
}

/**
 * @brief Создание индекса
 */
static Index*
configure_index (int _i, int _j, Index* _pk)
{
	struct index_conf* ic = cfg_box2index_conf (cfg.object_space[_i]->index[_j], _i, _j, 1);

	//
	// Конфигурацию индекса загрузить не удалось
	//
	if (ic == NULL)
		panic ("(object_space = %" PRIu32 " index = %" PRIu32 ") "
			   "unknown index type `%s'", _i, _j, cfg.object_space[_i]->index[_j]->type);

	ic->n = _j;

	//
	// Первичный индекс должен быть уникальным
	//
	if ((_j == 0) && !ic->unique)
		panic ("(object_space = %" PRIu32 ") object_space PK index must be unique", _i);

	//
	// ... и не может быть частичным
	//
	if ((_j == 0) && ic->notnull)
		panic ("(object_space = %" PRIu32 ") object_space PK index must include all objects and can't be notnull", _i);

	//
	// Для остальных неуникальных индексов добавляем в них поля
	// первичного индекса для того, чтобы сделать их уникальными
	//
	if ((_j > 0) && !ic->unique)
	{
		assert (_pk != NULL);

		index_conf_merge_unique (ic, &_pk->conf);
	}

	//
	// Создаём индекс с заданной конфигурацией и удаляем её
	//
	Index* index = [Index new_conf:ic dtor:box_tuple_dtor ()];
	free (ic);

	//
	// Если индекс по каким-то причинам создать не удалось
	//
	if (index == NULL)
		panic ("(object_space = %" PRIu32 " index = %" PRIu32 ") "
			   "XXX unknown index type `%s'", _i, _j, cfg.object_space[_i]->index[_j]->type);

	//
	// Если индекс поддерживает резервирование, то резервируем место для
	// данных либо по предположительному количеству записей, либо по размеру
	// первичного индекса
	//
	// FIXME: only reasonable for HASH indexes
	//
	if ([index respondsTo:@selector(resize:)])
	{
		if (_pk == NULL)
			[(id)index resize:cfg.object_space[_i]->estimated_rows];
		else
			[(id)index resize:[_pk size]];
	}

	return index;
}

/**
 * @brief Создание таблиц
 */
static void
configure_pk (Box* _box)
{
	//
	// Для всех возможных таблиц
	//
	for (int i = 0; i < nelem (_box->object_space_registry); ++i)
	{
		//
		// Если конфигурация таблицы не задана, то завершаем
		//
		if (cfg.object_space[i] == NULL)
			break;

		//
		// Пропускаем не определённые конфигурации
		//
		if (!CNF_STRUCT_DEFINED (cfg.object_space[i]))
			continue;

		//
		// Создаём таблицу
		//
		struct object_space* osp = _box->object_space_registry[i] = xcalloc (1, sizeof (struct object_space));
		//
		// Копируем конфигурацию
		//
		osp->n           = i;
		osp->ignored     = !!cfg.object_space[i]->ignored;
		osp->snap        = !!cfg.object_space[i]->snap;
		osp->wal         = osp->snap && !!cfg.object_space[i]->wal;
		osp->cardinality = cfg.object_space[i]->cardinality;

		osp->statbase = -1;
		if (cfg.box_extended_stat)
			object_space_fill_stat_names (osp);

		//
		// Если конфигурация индекса для таблицы не задана
		//
		if (cfg.object_space[i]->index == NULL)
			panic ("(object_space = %" PRIu32 ") at least one index must be defined", i);

		//
		// Создаём первичный индекс для таблицы
		//
		osp->index[0] = configure_index (i, 0, NULL);

		say_info ("object space %i PK %i:%s ", i, osp->index[0]->conf.n, [[osp->index[0] class] name]);
	}
}

void
box_idx_print_dups (void* _varg, struct index_node* _a, struct index_node* _b, uint32_t _pos)
{
	struct print_dups_arg* arg = _varg;

	struct tbuf out = TBUF (NULL, 0, fiber->pool);
	tbuf_printf (&out, "Duplicate values space %d index %d : ", arg->space, arg->index);
	tuple_print (&out, tuple_cardinality (_a->obj), tuple_data (_a->obj));
	tbuf_printf (&out, " ");
	tuple_print (&out, tuple_cardinality (_b->obj), tuple_data (_b->obj));

	say_error ("%.*s", (int)tbuf_len (&out), (char*)out.ptr);

	//
	// Выводим позицию найденного дубликата в буфер
	//
	if (arg->positions)
		write_i32 (arg->positions, _pos);
}

enum dup_action
{
	DUP_PANIC,
	DUP_IGNORE,
	DUP_DELETE
};

struct dup_conf
{
	int spaceno;
	int indexno;

	enum dup_action action;
} *dup_conf = NULL;

/**
 * @brief Определить необходимое действие для заданной таблицы и индекса
 */
static enum dup_action
on_duplicate_action (int _spaceno, int _indexno)
{
	if (dup_conf != NULL)
	{
		for (int i = 0; dup_conf[i].spaceno >= 0; ++i)
		{
			if ((dup_conf[i].spaceno == _spaceno) && (dup_conf[i].indexno == _indexno))
				return dup_conf[i].action;
		}
	}

	return cfg.no_panic_on_snapshot_duplicates ? DUP_IGNORE : DUP_PANIC;
}

/**
 * @brief Удаление из таблицы найденных дубликатов
 */
static void
delete_duplicates (struct object_space* _osp, size_t _node_size, uint32_t* _indexes, uint32_t _icount, void* _nodes)
{
	//
	// ВНИМАНИЕ: indexes[icount] должен быть равен общему числу записей в массиве _nodes
	//

	//
	// Проходим по всем найденным дубликатам и удаляем их из индексов
	//
	struct tbuf out = TBUF (NULL, 0, fiber->pool);
	for (int i = 0; i < _icount; ++i)
	{
		struct index_node* node = _nodes + _node_size*_indexes[i];

		tuple_print (&out, tuple_cardinality (node->obj), tuple_data (node->obj));
		say_warn ("space %d delete duplicate %.*s", _osp->n, (int)tbuf_len (&out), (char*)out.ptr);
		tbuf_reset (&out);

		foreach_index (index, _osp)
			[index remove: node->obj];
	}

	//
	// Проходим по всем найденным дубликатам и удаляем их из массива переданных узлов
	//
	for (int i = 0; i < _icount; ++i)
	{
		//
		// Позиция узла, следующего за дубликатом
		//
		uint32_t olda = _indexes[i] + 1;
		//
		// Позиция следующего дубликата или общее число узлов в массиве _nodes
		//
		uint32_t oldb = _indexes[i + 1];
		//
		// Позиция, следующая за последним перемещённым недубликатом
		//
		uint32_t newa = _indexes[i] - i;

		//
		// Смещаем следующий недубликат в позицию, следующую за последним недубликатом
		//
		memmove (_nodes + newa*_node_size, _nodes + olda*_node_size, (oldb - olda)*_node_size);
	}
}

/**
 * @brief Построение вторичных индексов для таблицы
 */
static void
build_secondary (struct object_space* _osp)
{
	//
	// Первичный индекс и общее количество объектов в таблице
	//
	Index<BasicIndex>* pk = _osp->index[0];
	size_t ntuples = [pk size];

	//
	// Древовидные индексы
	//
	Tree* trees[MAX_IDX] = {nil,};
	int tree_count = 0;

	//
	// Прочие индексы, для которых нам нужна только поддержка метода
	// replace:
	//
	// Поскольку в конфигурации таблицы жёстко прошито, что все индексы
	// должны поддерживать интерфейс Index с протоколом BasicIndex, то
	// каких-то дополнительных проверок здесь не делаем
	//
	// FIXME: PHash не поддерживает протокол BasicIndex в полном объёме,
	//        я не знаю, проблема это или нет
	//
	Index<BasicIndex>* others[MAX_IDX] = {nil,};
	int other_count = 0;

	//
	// Разделяем все вторичные индексы на древовидные и остальные, поскольку
	// для древовидных индексов можно использовать более широкий функционал
	// (исключение дубликатов с предварительной сортировкой узлов)
	//
	for (int i = 1; i < MAX_IDX; ++i)
	{
		if (_osp->index[i])
		{
			if ([_osp->index[i] isKindOf:[Tree class]])
				trees[tree_count++] = (Tree*)_osp->index[i];
			else
				others[other_count++] = _osp->index[i];
		}
	}

	//
	// Если вторичных индексов нет, то завершаем работу, перестраивать нечего
	//
	if ((tree_count == 0) && (other_count == 0))
		return;

	say_info ("Building secondary indexes of object space %i", _osp->n);

	//
	// При наличии записей заполняем конфигурацию dup_conf, которая является
	// глобальной и используется затем в функции on_duplicate_action при
	// определении действия, выполняемого для найденных дубликатов
	//
	if ((ntuples > 0) && cfg.on_snapshot_duplicates)
	{
		int cnt = 0;
		for (int i = 0; cfg.on_snapshot_duplicates[i]; ++i)
		{
			if (CNF_STRUCT_DEFINED (cfg.on_snapshot_duplicates[i]))
				++cnt;
		}

		dup_conf = xcalloc (cnt + 1, sizeof (struct dup_conf));
		dup_conf[cnt].spaceno = -1;

		for (int k = 0, i = 0; cfg.on_snapshot_duplicates[i]; ++i)
		{
			if (!CNF_STRUCT_DEFINED (cfg.on_snapshot_duplicates[i]))
				continue;

			__typeof__ (cfg.on_snapshot_duplicates[i]->index) indexes = cfg.on_snapshot_duplicates[i]->index;
			for (int j = 0; indexes[j]; ++j)
			{
				if (!CNF_STRUCT_DEFINED (indexes[j]))
					continue;

				dup_conf[k].spaceno = i;
				dup_conf[k].indexno = j;

				if (strcmp (indexes[j]->action, "IGNORE") == 0)
					dup_conf[k].action = DUP_IGNORE;
				else if (strcmp (indexes[j]->action, "DELETE") == 0)
					dup_conf[k].action = DUP_DELETE;
				else
					abort ();

				say_debug2 ("dup_conf[%d]={.spaceno=%d, .indexno=%d, .action=%d}", k, i, j, dup_conf[k].action);

				++k;
			}
		}
	}

	//
	// Если есть что индексировать
	//
	if (ntuples > 0)
	{
		title ("building_indexes/object_space: %i", _osp->n);

		struct tnt_object* obj = NULL;

		//
		// Перестраиваем недревовидные индексы
		//
		[pk iterator_init];
		while ((obj = [pk iterator_next]))
		{
			for (int i = 0; i < other_count; ++i)
			{
				//
				// Учитываем, что индекс может быть частичным
				//
				if (tuple_match (&others[i]->conf, obj))
				{
					@try
					{
						[others[i] replace:obj];
					}
					@catch (id e)
					{
						say_error ("can't insert object into osp:%i index:%i, try to replace this index with non unique TREE index",
								   _osp->n, others[i]->conf.n);
					}
				}
			}
		}

		//
		// Перестраиваем древовидные индексы с возможным удалением дубликатов
		//
		for (int i = 0; i < tree_count; ++i)
		{
			say_info ("    %i: %s", trees[i]->conf.n, [[trees[i] class] name]);

			//
			// Распределяем память под узлы древовидного индекса
			//
			// Резервируем память под все объекты таблицы даже для частичных индексов, так
			// проще. Подсчёт реального количества объектов, подходящих под условие включения
			// в частичный индекс сделаем одновременно с инициализацией соответствующих узлов
			//
			void* nodes = xmalloc (ntuples*trees[i]->node_size);

			//
			// Инициализируем массив узлов индекса
			//
			u32 t = 0;
			[pk iterator_init];
			while ((obj = [pk iterator_next]))
			{
				//
				// Учитываем, что индекс может быть частичным
				//
				if (tuple_match (&trees[i]->conf, obj))
				{
					//
					// Узел индекса для инициализации
					//
					struct index_node* node = nodes + t*trees[i]->node_size;

					//
					// Инициализация узла
					//
					trees[i]->dtor (obj, node, trees[i]->dtor_arg);

					//
					// Число инициализированных узлов
					//
					++t;
				}
			}

			//
			// Арументы операции сортировки
			//
			struct print_dups_arg arg = {.space = _osp->n, .index = trees[i]->conf.n, NULL};
			//
			// Действие при нахождении дубликатов для индексов, которые отмечены
			// как уникальные
			//
			enum dup_action action = on_duplicate_action (arg.space, arg.index);
			//
			// Если задано удаление дубликатов, то создаём буфер для записи
			// индексов дубликатов для удаления
			//
			if (action == DUP_DELETE)
				arg.positions = tbuf_alloc (fiber->pool);

			//
			// Сортируем массив инициализированных узлов индекса по возрастанию. При этом
			// если индекс сконфигурирован как уникальный, то для всех найденных дубликатов
			// будет вызываться функция box_idx_print_dups и если дубликаты были найдены,
			// то будет возвращён признак false
			//
			if (![trees[i] sort_nodes:nodes count:t onduplicate:box_idx_print_dups arg:(void*)&arg])
			{
				say_debug ("space %d index %d FOUND DUPS!!! action is %s", arg.space, arg.index,
							((action == DUP_PANIC) ? "PANIC" : (action == DUP_IGNORE) ? "IGNORE" : "DELETE"));

				if (action != DUP_IGNORE)
					say_error ("if you want to ignore this duplicates, add " \
								"on_snapshot_duplicates[%d].index[%d].action=\"IGNORE\"",
									arg.space, arg.index);

				if (action != DUP_DELETE)
					say_error ("if you want to delete duplicates rows, add " \
								"on_snapshot_duplicates[%d].index[%d].action=\"DELETE\"",
									arg.space, arg.index);

				if (action == DUP_PANIC)
					panic ("duplicate tuples");

				//
				// В случае если задано удаление дубликатов
				//
				if (action == DUP_DELETE)
				{
					//
					// Общее число позиций узлов в массиве дубликатов (используем тот факт,
					// что все 32x битные целые выводятся в буфер как есть)
					//
					uint32_t npos = tbuf_len (arg.positions)/sizeof (uint32_t);

					//
					// Если дубликаты были найдены
					//
					if (npos > 0)
					{
						//
						// Пишем в конец общее количество инициализированных узлов в массиве nodes
						//
						write_i32 (arg.positions, t);

						//
						// Удаляем дубликаты из массива инициализированных узлов индекса
						//
						delete_duplicates (_osp, trees[i]->node_size, arg.positions->ptr, npos, nodes);

						//
						// Уменьшаем общее число инициализированных узлов на количество удалённых
						// дубликатов
						//
						t -= npos;

						say_error ("DON'T FORGET TO SAVE SNAPSHOT AS SOON AS POSSIBLE!!!!!!!!");
					}
				}
			}

			//
			// После загрузки данных в индекс массив nodes будет удалён
			//
			[trees[i] set_sorted_nodes:nodes count:t];
		}
	}

	if (dup_conf != NULL)
	{
		free (dup_conf);
		dup_conf = NULL;
	}

	title (NULL);
}

/**
 * @brief Создание вторичных индексов в соответствии с конфигурацией
 */
static void
configure_secondary (Box* _box)
{
	//
	// Проходим по всем возможным таблицам
	//
	for (int i = 0; i < nelem (_box->object_space_registry); ++i)
	{
		//
		// Таблица для конфигурирования
		//
		struct object_space* osp = _box->object_space_registry[i];
		//
		// Пропускаем не заданные таблицы
		//
		if (osp == NULL)
			continue;

		say_info ("object space %i", i);

		//
		// Первичный индекс
		//
		Index* pk = osp->index[0];
		//
		// Переменная pv используется для связываения индексов в список
		//
		Index* pv = osp->index[0];
		//
		// Проходим по всем возможным индексам
		//
		for (int j = 1; j < nelem (osp->index); ++j)
		{
			//
			// Пропускаем индексы, для которых не задана конфигурация
			//
			if ((cfg.object_space[i]->index[j] == NULL) || !CNF_STRUCT_DEFINED (cfg.object_space[i]->index[j]))
				break;

			//
			// Создаём индекс
			//
			osp->index[j] = configure_index (i, j, pk);

			//
			// Привязываем созданный индекс к предыдущему
			//
			pv->next = osp->index[j];
			pv       = osp->index[j];

			say_info ("    index %i: %s", j, [[osp->index[j] class] name]);
		}

		//
		// Индексируем данные
		//
		build_secondary (osp);
	}
}

/**
 * @brief Распаковка команд и их выполнение
 */
static void
prepare_tlv (struct box_txn* _tx, const struct tlv* _tlv)
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
				const struct tlv* nested = (struct tlv*)val;

				//
				// Рекурсивно вызываем сами себя для обработки вложенной tlv-структуры
				//
				prepare_tlv (_tx, nested);

				//
				// Переходим к следующей tlv-структуре
				//
				val += sizeof (*nested) + nested->len;
			}

			break;
		}

		//
		// В случае, если tlv-структура содержит одну команду
		//
		case BOX_OP:
			//
			// Вызываем обработку операции с передачей ей кода операции (первые два байта
			// блока данных) и данных операции (следующие за кодом операции данные блока)
			//
			box_prepare (_tx, *(u16*)_tlv->val, _tlv->val + sizeof (u16), _tlv->len - sizeof (u16));
			break;

		//
		// Неизвестные команды просто пропускаем
		//
		default:
			say_error ("Unknown command in prepare_tlv: %d", _tlv->tag);
			break;
	}
}

/**
 * @brief Проверяем вторичные индексы на эквивалентность по количеству
 *        записей в них
 *
 * Случай, когда во вторичном индексе присутствует объект отсутствующий в первичном
 * индексе, не проверяем, так как данная функция вызывается после индексации данных
 * по первичному индексу. Так что такая проверка только зря потратит процессорное
 * время.
 */
static void
verify_indexes (struct object_space* _osp)
{
	title ("snap_dump/check indexes");

	//
	// Число проверенных объектов
	//
	size_t pk_rows = 0;

	//
	// Объект из первичного индекса
	//
	struct tnt_object* obj = NULL;

	//
	// Проходим по всем объектам первичного индекса
	//
	Index<BasicIndex>* pk = _osp->index[0];
	[pk iterator_init];
	while ((obj = [pk iterator_next]))
	{
		//
		// Для каждого объекта проходим по всем вторичным индексам
		//
		foreach_indexi (1, index, _osp)
		{
			//
			// Проверяем должен ли объект находится в индексе
			//
			bool m = tuple_match (&index->conf, obj);

			//
			// Находим объект в индексе
			//
			struct tnt_object* index_obj = [index find_obj:obj];

			//
			// Если объект должен находиться в индексе, но не находится там,
			// то это ошибка
			//
			if (m && !index_obj)
				say_error ("index %i of object space %i violation found at position %zi (object not found in index)", index->conf.n, _osp->n, pk_rows);

			//
			// Если объект не должен находиться в индексе, но находится там,
			// то это ошибка
			//
			if (!m && index_obj)
				say_error ("index %i of object space %i violation found at position %zi (object found in index)", index->conf.n, _osp->n, pk_rows);
		}

		//
		// Временно засыпаем через заданное количество проверенных объектов
		//
		if ((cfg.snap_dump_check_rows > 0) && ((++pk_rows % cfg.snap_dump_check_rows) == 0))
		{
			struct timespec duration;
			//set cfg.snap_dump_check_sleep in milliseconds
			duration.tv_sec  = (cfg.snap_dump_check_sleep >= 1000) ? cfg.snap_dump_check_sleep/1000 : 0;
			duration.tv_nsec = (cfg.snap_dump_check_sleep%1000)*1000000;
			nanosleep (&duration, NULL);
		}
	}
}

@implementation Box
-(void)
set_shard:(Shard<Shard>*)_shard
{
	shard = _shard;

	if (cfg.object_space != NULL)
	{
		if (shard->dummy)
			configure_pk (self);
		else
			say_warn ("cfg.object_space ignored");
	}
}

-(void)
apply:(struct tbuf*)_data tag:(u16)_tag
{
	say_debug2 ("%s tag:%s", __func__, xlog_tag_to_a (_tag));
	say_debug3 ("%s row:%s", __func__, box_row_to_a  (_tag, _data));

	int tag_type = _tag & ~TAG_MASK;

	_tag &= TAG_MASK;

	//
	// Обработка команд по изменению структуры таблиц
	//
	if (_tag >= (CREATE_OBJECT_SPACE<<5))
	{
		struct box_meta_txn tx = {.op = _tag >> 5, .box = self};

		@try
		{
			box_meta_prepare (&tx, _data);
			box_meta_commit  (&tx);
		}
		@catch (id e)
		{
			box_meta_rollback (&tx);
			@throw;
		}

		return;
	}

	//
	// Обработка команд по загрузке данных в таблицу
	//
	switch (tag_type)
	{
		case TAG_WAL:
		{
			struct box_txn tx = {.box = self, .mode = RW, .ops = TAILQ_HEAD_INITIALIZER (tx.ops)};

			fiber->txn = &tx;
			@try
			{
				//
				// Данные из журнала
				//
				if (_tag == wal_data)
				{
					//
					// Код операции
					//
					int op = read_u16 (_data);

					//
					// Выполнение операции
					//
					box_prepare (&tx, op, _data->ptr, tbuf_len (_data));
				}
				//
				// Tag Length Value запись
				//
				else if (_tag == tlv)
				{
					//
					// Пока буфер не пуст
					//
					while (tbuf_len (_data) > 0)
					{
						//
						// Заголовок TLV-данных из буфера
						//
						struct tlv *tlv = read_bytes (_data, sizeof (*tlv));

						//
						// Пропускаем TLV-данные в буфере
						//
						tbuf_ltrim  (_data, tlv->len);

						//
						// Загружаем данные в базу
						//
						prepare_tlv (&tx, tlv);
					}
				}
				//
				// Пользовательские операции
				//
				else if (_tag >= user_tag)
				{
					//
					// Выполняем операцию, закодированную в тэге
					//
					box_prepare (&tx, _tag >> 5, _data->ptr, tbuf_len (_data));
				}
				else
				{
					return;
				}

				//
				// Фиксируем выполнение операции
				//
				box_commit (&tx);
			}
			@catch (id e)
			{
				//
				// В случае ошибки откатываем операцию
				//
				box_rollback (&tx);
				@throw;
			}

			break;
		}

		case TAG_SNAP:
		{
			//
			// Если это не данные снапшота
			//
			if (_tag != snap_data)
				return;

			//
			// Восстанавливаемые данные
			//
			const struct box_snap_row* snap = box_snap_row (_data);

			//
			// Таблица, для которой восстанавливаются данные
			//
			struct object_space* osp = object_space_registry[snap->object_space];
			if (osp == NULL)
				raise_fmt ("object_space %i is not configured", snap->object_space);

			//
			// Данные при загрузке таблицы из снапшота игнорируются
			//
			if (osp->ignored)
				break;

			//
			// Проверяем наличие у таблицы первичного индекса
			//
			assert (osp->index[0] != NULL);

			//
			// Добавляем объект в таблицу (данные добавляются только в первичный
			// индекс, а после загрузки всего снапшота вызывается функция перестроения
			// вторичных индексов сразу для всех таблиц и объектов)
			//
			snap_insert_row (osp, snap->tuple_size, snap->data, snap->data_size);
			break;
		}

		case TAG_SYS:
			abort ();
	}
}

-(void)
snap_final_row
{
	//
	// Вызывается только в режиме legacy
	//
	configure_secondary (self);
}

-(void)
wal_final_row
{
	for (u32 n = 0; n < nelem (object_space_registry); ++n)
	{
		struct object_space* osp = object_space_registry[n];
		if (osp == NULL)
			continue;

		say_info ("Object space %i", n);
		foreach_index (index, osp)
			say_info ("    index[%i]: %s", index->conf.n, [index info]);
	}
}

-(void)
status_changed
{
	if (cfg.object_space != NULL)
	{
		enum box_status
		{
			NOTHING = 0,
			PRIMARY,
			LOCAL_STANDBY,
			REMOTE_STANDBY
		};

		enum box_status cur_status = NOTHING;
		const char*     status     = [shard status];

		if (strcmp (status, "primary") == 0)
			cur_status = PRIMARY;
		else if (strncmp (status, "hot_standby/local", 17) == 0)
			cur_status = LOCAL_STANDBY;
		else if ((strncmp (status, "hot_standby/", 12) == 0) && (strstr (status, "/ok") != NULL))
			cur_status = REMOTE_STANDBY;

		if (((cur_status == PRIMARY) || (cur_status == REMOTE_STANDBY)) && (box_primary.name == NULL))
		{
			initialize_primary_service ();
			set_recovery_service (&box_primary);
		}

		if ((cur_status != NOTHING) && (box_secondary.name == NULL))
			initialize_secondary_service ();
	}
}

-(void)
print:(const struct row_v12*)_row into:(struct tbuf*)_buf
{
	print_row (_buf, _row, box_print_row);
}

-(int)
snapshot_fold
{
	struct tnt_object* obj = NULL;

	u32 crc = 0;

#ifdef FOLD_DEBUG
	int count = 0;
#endif

	for (int n = 0; n < nelem (object_space_registry); ++n)
	{
		if ((object_space_registry[n] == NULL) || (!object_space_registry[n]->snap))
			continue;

		//
		// Первичный индекс таблицы
		//
		id pk = object_space_registry[n]->index[0];
		//
		// Инициализируем итератор обхода объектов таблицы в зависимости
		// от поддержки упорядоченного обхода (для фиксации порядка обхода,
		// чтобы вычисляемая CRC32-сумма не зависила от размещения данных
		// в памяти)
		//
		if ([pk respondsTo:@selector (ordered_iterator_init)])
			[pk ordered_iterator_init];
		else
			[pk iterator_init];

		//
		// Проходим по всем объектам таблицы
		//
		while ((obj = [pk iterator_next]))
		{
#ifdef FOLD_DEBUG
			struct tbuf* b = tbuf_alloc (fiber->pool);
			tuple_print (b, tuple->cardinality, tuple->data);
			say_info ("row %i: %.*s", count++, tbuf_len (b), (char*)b->ptr);
#endif
			//
			// Приводим заголовки SMALL_TUPLE и TUPLE к единому виду и вычисляем
			// для записи CRC32 контрольную сумму
			//
			u32 header[2] = {tuple_bsize (obj), tuple_cardinality (obj)};
			crc = crc32c (crc, (unsigned char*)header, sizeof (header));
			crc = crc32c (crc, tuple_data (obj), header[0]/*bsize*/);
		}
	}

	printf ("CRC: 0x%08x\n", crc);
	return 0;
}

-(u32)
snapshot_estimate
{
	//
	// Подсчитываем общее количество объектов в базе данных
	//
	size_t total_rows = 0;
	for (int n = 0; n < nelem (object_space_registry); ++n)
	{
		if (object_space_registry[n] && object_space_registry[n]->snap)
			total_rows += [object_space_registry[n]->index[0] size];
	}
	return total_rows;
}

-(int)
snapshot_write_rows:(XLog*)_log
{
	struct box_snap_row header;
	struct tnt_object*  obj = NULL;

	//
	// Пул для управления памятью в процессе работы процедуры
	//
	struct palloc_pool* pool = palloc_create_pool ((struct palloc_config) {.name = __func__});

	//
	// Привязываем буфер к пулу управления памятью. Буфер делаем один на все
	// операции, чтобы не забивать память постоянными перераспределениями
	//
	struct tbuf* buf = tbuf_alloc (pool);

	//
	// Общее число записей
	//
	size_t total_rows = [self snapshot_estimate];
	//
	// Обработанное число записей на текущий момент
	//
	size_t all_rows = 0;

	@try
	{
		//
		// Проходим по всем таблицам
		//
		for (int n = 0; n < nelem (object_space_registry); ++n)
		{
			//
			// Пропускаем несконфигурированные и не сохраняемые таблицы
			//
			if ((object_space_registry[n] == NULL) || !object_space_registry[n]->snap)
				continue;

			//
			// Таблица для анализа
			//
			struct object_space* osp = object_space_registry[n];
			assert (n == osp->n);

			//
			// Первичный ключ таблицы, по которому будем выполнять сканирование записей
			//
			Index<BasicIndex>* pk = osp->index[0];

			//
			// Пишем в журнал конфигурацию таблицы и первичного индекса
			//
			if (!shard->dummy)
			{
				int flags = (osp->snap ? 1 : 0) | (osp->wal ? 2 : 0);

				tbuf_reset (buf);
				write_i32  (buf, n);
				write_i32  (buf, flags);
				write_i8   (buf, osp->cardinality);
				index_conf_write (buf, &pk->conf);

				if ([_log append_row:buf->ptr len:tbuf_len (buf) shard:shard tag:(CREATE_OBJECT_SPACE<<5)|TAG_SNAP] == NULL)
					@throw [Error with_reason:"can't write object space configuration into snapshot"];
			}

			//
			// Проходим по всем объектам таблицы
			//
			[pk iterator_init];
			while ((obj = [pk iterator_next]))
			{
				//
				// объект для вывода в журнал
				//
				obj = tuple_visible_left (obj);
				if (obj == NULL)
					continue;

				//
				// Проверяем валидность объекта
				//
				if (!tuple_valid (obj))
				{
					errno = EINVAL;

					@throw [Error with_format:"heap invariant violation: n:%i invalid tuple %p", n, obj];
				}

				//
				// Проверяем счётчик ссылок
				//
				if ((obj->type == BOX_TUPLE) && (container_of (obj, struct gc_oct_object, obj)->refs <= 0))
				{
					errno = EINVAL;

					@throw [Error with_format:"heap invariant violation: n:%i obj->refs == %i", n,
												container_of (obj, struct gc_oct_object, obj)->refs];
				}

				//
				// Формируем заголовок для записи в лог
				//
				header.object_space = n;
				header.tuple_size   = tuple_cardinality (obj);
				header.data_size    = tuple_bsize (obj);

				//
				// Пишем объект в буфер, который затем выводим в лог
				//
				tbuf_reset  (buf);
				tbuf_append (buf, &header, sizeof (header));
				tbuf_append (buf, tuple_data (obj), header.data_size);

				if ([_log append_row:buf->ptr len:tbuf_len (buf) shard:shard tag:snap_data|TAG_SNAP] == NULL)
					@throw [Error with_reason:"can't write tuple into WAL"];

				//
				// Индикация прогресса записи данных
				//
				if (++all_rows%100000 == 0)
				{
					float pct = 100.0*all_rows/total_rows;

					say_info ("%.1fM/%.2f%% rows written", all_rows/1000000.0, pct);

					title ("snap_dump %.2f%%", pct);
				}
			}

			//
			// Пишем конфигурацию индексов
			//
			if (!shard->dummy)
			{
				//
				// Здесь первичный индекс не пишем, так как он был сохранён ранее
				// как часть конфигурации таблицы
				//
				foreach_indexi (1, index, osp)
				{
					tbuf_reset (buf);
					write_i32  (buf, n);
					write_i32  (buf, 0); // flags
					write_i8   (buf, index->conf.n);
					index_conf_write (buf, &index->conf);

					if ([_log append_row:buf->ptr len:tbuf_len(buf) shard:shard tag:(CREATE_INDEX<<5)|TAG_SNAP] == NULL)
						@throw [Error with_reason:"can't write index configuration into snapshot"];
				}
			}

			//
			// Проверяем, что все данные правильно проиндексированы
			//
			verify_indexes (osp);
		}
	}
	@catch (Error* e)
	{
		say_error ("%s: %s", __func__, [e reason]);

		return -1;
	}
	@finally
	{
		//
		// Удаляем всю память, распределённую в процессе работы процедуры
		// и при успешном и при неуспешном её завершении
		//
		palloc_destroy_pool (pool);
	}

	return 0;
}
@end

static struct tnt_module box_mod =
{
	.name          = "box",
	.version       = box_version_string,
	.depend_on     = (const char*[]) {"onlineconf", NULL},
	.init          = init,
	.check_config  = check_config,
	.reload_config = reload_config,
	.cat           = box_cat,
	.cat_scn       = box_cat_scn,
	.info          = info,
};

register_module (box_mod);
register_source ();
