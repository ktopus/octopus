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
#include <stdlib.h>
#include <stdio.h>

#import <stat.h>
#import <say.h>

#import <mod/box/common.h>

static int g_stat_base;
static int g_stat_named_base;

/**
 * @brief Список символических имён для операций
 */
static char const* const g_box_ops[] = ENUM_STR_INITIALIZER(MESSAGES);

const char*
box_op_name (u16 _op)
{
	return g_box_ops[_op];
}

void
object_space_fill_stat_names (struct object_space* _osp)
{
	char buf[128];
	char const** names;

	//
	// Создаём временную таблицу имён
	//
	names = xcalloc (BSS_MAX, sizeof (char const*));

	//
	// Заполняем временную таблицу
	//
	sprintf (buf, "OP_INSERT:%d", _osp->n);
	names[BSS_INSERT] = xstrdup (buf);

	sprintf (buf, "OP_UPDATE:%d", _osp->n);
	names[BSS_UPDATE] = xstrdup (buf);

	sprintf (buf, "OP_DELETE:%d", _osp->n);
	names[BSS_DELETE] = xstrdup (buf);

	for (int i = 0; i < MAX_IDX; ++i)
	{
		sprintf (buf, "SELECT_%d_%d", _osp->n, i);
		names[BSS_SELECT_IDX0 + i] = xstrdup (buf);

		sprintf (buf, "SELECT_TIME_%d_%d", _osp->n, i);
		names[BSS_SELECT_TIME_IDX0 + i] = xstrdup (buf);

		sprintf (buf, "SELECT_KEYS_%d_%d", _osp->n, i);
		names[BSS_SELECT_KEYS_IDX0 + i] = xstrdup (buf);

		sprintf (buf, "SELECT_TUPLES_%d_%d", _osp->n, i);
		names[BSS_SELECT_TUPLES_IDX0 + i] = xstrdup (buf);
	}

	//
	// Регистрируем имена в базе данных
	//
	_osp->statbase = stat_register_static ("box", names, BSS_MAX);

	//
	// Освобождаем временные таблицы
	//
	for (int i = 0; i < BSS_MAX; ++i)
		free ((void*)names[i]);
	free (names);
}

void
object_space_clear_stat_names (struct object_space* _osp)
{
	stat_unregister (_osp->statbase);

	_osp->statbase = -1;
}

void
box_stat_collect (int _name, i64 _v)
{
	stat_collect (g_stat_base, _name, _v);
}

void
box_stat_collect_double (int _name, double _v)
{
	stat_collect_double (g_stat_base, _name, _v);
}

void
box_stat_sum_named (const char* _name, int _n, double _v)
{
	stat_sum_named (g_stat_named_base, _name, _n, _v);
}

void
box_stat_aggregate_named (const char* _name, int _n, double _v)
{
	stat_aggregate_named (g_stat_named_base, _name, _n, _v);
}

void
box_stat_init (void)
{
	g_stat_base       = stat_register (g_box_ops, nelem (g_box_ops));
	g_stat_named_base = stat_register_named ("box");
}

register_source ();
