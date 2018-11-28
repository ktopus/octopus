#!/usr/bin/ruby

$: << File.dirname($0) + '/lib'
require 'run_env'

class NormalEnv < RunEnv
	def config
		super + <<EOD

object_space[0].index[1].type = "TREE"
object_space[0].index[1].unique = 1
object_space[0].index[1].key_field[0].fieldno = 1
object_space[0].index[1].key_field[0].type = "STR"
EOD
	end
end

NormalEnv.env_eval do
	start

	c = connect

	30.times do |i|
		c.insert [i + 0, "abc#{i} + 0"]
		c.insert [i + 1, "abc#{i} + 1"]
		c.insert [i + 2, "abc#{i} + 2"]
		c.select i + 0, i + 1, i + 2

		snapshot
		sleep 0.1
		stop
		sleep 0.1
		start
		sleep 0.1
		c.reconnect

		c.select i + 0, i + 1, i + 2
		c.delete i + 0
		c.select i + 0, i + 1, i + 2
		c.delete i + 1
		c.select i + 0, i + 1, i + 2
		c.delete i + 2
		c.select i + 0, i + 1, i + 2
	end

	3000.times do |i|
		c.select i
		c.insert [i, "abc#{i}"]
		c.select i
	end

	snapshot
	sleep 0.1
	stop
	sleep 0.1
	start
	sleep 0.1
	c.reconnect

	3000.times do |i|
		c.select i
		c.delete i
		c.select i
	end
end
