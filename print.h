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
#ifndef __BOX_PRINT_H
#define __BOX_PRINT_H

#include <index.h>

#import <iproto.h>
#import <net_io.h>
#import <objc.h>
#import <log_io.h>

/**
 * @brief Печать в буфер записи в соответствии с форматом
 */
void tuple_print (struct tbuf* _b, u32 _c, void* _f);

/**
 * @brief Печать в буфер записей журналов
 */
void box_print_row (struct tbuf* out, u16 tag, struct tbuf* r);

/**
 * @brief Преобразование записи журналов в текстовое представление
 *
 * Память под текстовое представление распределяется в том же пуле
 * сопроцедуры
 */
const char* box_row_to_a (u16 tag, struct tbuf* r);

/**
 * @brief Распечатать журнал с заданным номером
 */
int box_cat_scn (i64 stop_scn);

/**
 * @brief Распечатать заданный файл журнала
 *
 * Параметры данной процедуре передаются через переменные окружения:
 *     * BOX_CAT_FMT        - строка формата печати записей;
 *     * BOX_CAT_SNAP_SPACE - таблица, для которой будет выполняться печать
 */
int box_cat (const char* _fname);

#endif // __BOX_PRINT_H
