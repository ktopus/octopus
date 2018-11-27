local box = require ("box")
local ffi = require ("ffi")

local tonumber = tonumber
local print    = print

user_proc = user_box or {}
local user_proc = user_proc

module (...)

ffi.cdef [[
	int box_meta_truncate (int _id, int _n);
]]

--
-- Удалить все записи из заданной таблицы заданного шарда
--
-- @param[in] _id числовой идентификатор шарда
-- @param[in] _n числовой идентификатор таблицы
--
-- @return -1 - выполняется транзакция модификации данных;
--         -2 - идентификатор шарда вне диапазона;
--         -3 - шарда с заданным идентификатором не существует;
--         -4 - с заданным шардом не связан никакой модуль;
--         -5 - шард является репликой;
--         -6 - в шарде отсутствует таблица с заданным индексом;
--         -7 - не удалось сохранить запись об изменениях в журнал;
--         -8 - идентификатор шарда не является числом;
--         -9 - номер таблицы не является числом
--         0...n - число удалённых из таблицы записей
--
-- Данную функцию можно вызывать только из административного LUA-интерфейса,
-- вне транзакции изменения данных
--
function meta_truncate (_id, _n)
	print ("truncate from " .. _id .. "/" .. _n)

	_id = tonumber (_id)
	if _id == nil then
		return -8
	end

	_n = tonumber (_n)
	if _n == nil then
		return -9
	end

	return ffi.C.box_meta_truncate (_id, _n)
end

--
-- Запоминаем функцию в глобальной таблице для её вызова через интерфейс
--
user_proc.meta_truncate = meta_truncate
