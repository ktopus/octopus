#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

RunEnv.env_eval do
	start

	c = connect

	#
	# Проверяем работу LUA-функции полного удаления данных
	#
	# Данная функция работает только в рамках транзакции изменения
	# данных, которая автоматически запускается при выполнении LUA-функций
	# с использованием BOX/IProto-интерфейса
	#
	c.insert [1, 2, "abc", "def"]
	c.select 1, 2
	c.lua "user_proc.truncate", "0"
	c.select 1, 2

	#
	# Проверяем работу специальной команды полного удаления данных,
	# которая относится к классу BOX/IProto-команд модификации
	# мета-информации
	#
	c.insert [2, 3, "klm", "xyz"]
	c.select 1, 2
	c.truncate
	c.select 1, 2
end
