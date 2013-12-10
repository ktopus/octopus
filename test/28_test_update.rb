#!/usr/bin/ruby1.9.1

$:.push 'test/lib'
require 'standalone_env'

class Env < StandAloneEnv
  def config
    super + <<EOD
object_space[0].enabled = 1
object_space[0].index[0].type = "HASH"
object_space[0].index[0].unique = 1
object_space[0].index[0].key_field[0].fieldno = 0
object_space[0].index[0].key_field[0].type = "STR"
EOD
  end
end

Env.clean.with_server do
  insert [1, "\0\1", "\0\0\0\1", "\0\0\0\0\0\0\0\1"]
  update_fields 1, [1, :add, "aa"] , [2, :add, "aaaa"], [3, :add, "aaaaaaaa"]
  select 1
end