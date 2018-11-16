local ffi = require ('ffi')

module (...)

ffi.cdef [[
	int box_meta_truncate (int _shard_id, int _n);
]]

function truncate (_id, _n)
	return ffi.C.box_meta_truncate (tonumber (_id), tonumber (_n));
end
