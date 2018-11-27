#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class NormalEnv < RunEnv
	def config
		super + <<EOD

admin_exec_lua = 0
EOD
	end
end

class AdminEnv < RunEnv
	def config
		super + <<EOD

admin_exec_lua = 1
EOD
	end
end

NormalEnv.env_eval do
	start

	c = connect

	#
	# Проверяем работу LUA-функции полного удаления данных
	#
	# Данная функция работает только в рамках транзакции изменения
	# данных, которая автоматически запускается при выполнении LUA-функций
	# с использованием BOX/IProto-интерфейса
	#
	c.insert [1, "abc"]
	c.select 1, 2, 3
	c.lua "user_proc.truncate", "0"
	c.select 1, 2, 3

	#
	# Проверяем работу специальной команды полного удаления данных,
	# которая относится к классу BOX/IProto-команд модификации
	# мета-информации
	#
	c.insert [2, "klm"]
	c.select 1, 2, 3
	c.truncate
	c.select 1, 2, 3

	#
	# Проверяем работу LUA-процедуры полного удаления данных через админский
	# интерфейс. В данном случае процедура сработать не должна, так как
	# запуск LUA-скриптов заблокирован
	#
	c.insert [3, "xyz"]
	c.select 1, 2, 3
	print "run truncate over admin telnet interface\n"
	system ("echo 'exec lua user_proc.meta_truncate (0, 0)' | nc -N 127.0.0.1 33015")
	c.select 1, 2, 3
end

AdminEnv.env_eval do
	start

	c = connect

	#
	# Проверяем работу LUA-процедуры полного удаления данных через админский
	# интерфейс. В данном случае процедура должна сработать
	#
	c.insert [3, "xyz"]
	c.select 1, 2, 3
	print "run truncate over admin telnet interface\n"
	system ("echo 'exec lua user_proc.meta_truncate (0, 0)' | nc -N 127.0.0.1 33015")
	c.select 1, 2, 3
end
