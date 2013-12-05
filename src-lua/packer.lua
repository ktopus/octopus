local ffi = require'ffi'
local bit = require'bit'
local C = ffi.C
local setmetatable = setmetatable
local type, tonumber = type, tonumber

local i8p = ffi.typeof('int8_t*')
local i16p = ffi.typeof('int16_t*')
local i32p = ffi.typeof('int32_t*')
local i64p = ffi.typeof('int64_t*')
local u8p = ffi.typeof('uint8_t*')
local u16p = ffi.typeof('uint16_t*')
local u32p = ffi.typeof('uint32_t*')
local u64p = ffi.typeof('uint64_t*')
local pack_meths = {
    willneed = C.tbuf_willneed,
    need = function(self, i)
        self:willneed(i)
        return self:_need(i)
    end,
    u8 = function(self, i) C.write_i8(self, tonumber(i)) end,
    u16 = function(self, i) C.write_i16(self, tonumber(i)) end,
    u32 = function(self, i) C.write_i32(self, tonumber(i)) end,
    u64 = function(self, i)
        if type(i) == "string" then
            i = C.atoll(i)
        end
        C.write_i64(self, i)
    end,
    ber = function(self, i) C.write_varint32(self, tonumber(i)) end,
    varint32 = function(self, i) C.write_varint32(self, tonumber(i)) end,
    raw = C.tbuf_append,
    string = function(self, s) C.tbuf_append(self, s, #s) end,
    field = function(self, s) C.write_field_s(self, s, #s) end,
    field_raw = function(self, s, ln) C.write_field_s(self, s, ln) end,
    field_u8 = function(self, i) C.write_field_i8(self, tonumber(i)) end,
    field_u16 = function(self, i) C.write_field_i16(self, tonumber(i)) end,
    field_u32 = function(self, i) C.write_field_i32(self, tonumber(i)) end,
    field_u64 = function(self, i)
        if type(i) == "string" then
            i = C.atoll(i)
        end
        C.write_field_i64(self, i)
    end,
    bersize = function(self, i)
        if type(i) == "string" then
            i = tonumber(i)
        end
        if i < 128 then
            return 1
        elseif i < 128*128 then
            return 2
        else
            local c = bit.rshift(i, 21)
            for n=3,5 do
                if c == 0 then return n end
                c = bit.rshift(c, 7)
            end
        end
    end,
    _need = function(self, i)
        local ptr = self.stop
        self.stop = self.stop + i
        return ptr
    end,
    _u8 = function(self, i)
        self:_need(1)[0] = tonumber(i)
    end,
    _u16 = function(self, i)
        ffi.cast(i16p, self:_need(2))[0] = tonumber(i)
    end,
    _u32 = function(self, i)
        ffi.cast(i32p, self:_need(4))[0] = tonumber(i)
    end,
    _u64 = function(self, i)
        if type(i) == "string" then
            i = C.atoll(i)
        end
        ffi.cast(i64p, self:_need(8))[0] = i
    end,
    _ber = function(self, i)
        if type(i) == "string" then
            i = tonumber(i)
        end
        if i < 128 then
            self:_u8(i)
        elseif i < 128*128 then
            local p = self:_need(2)
            p[0] = bit.bor(bit.rshift(i, 7), 0x80)
            p[1] = bit.band(i, 0x7f)
        else
            local n = 3
            local c = bit.rshift(i, 21)
            while c > 0 do
                c = bit.rshift(c, 7)
                n = n + 1
            end
            local ptr = self:_need(n)
            ptr[n - 1] = bit.band(i, 0x7f)
            i = bit.rshift(i, 7)
            for j = n - 2, 0, -1 do
                ptr[j] = bit.bor(bit.band(i, 0x7f), 0x80)
                i = bit.rshift(i, 7)
            end
        end
    end,
    _string = function(self, s)
        ffi.copy(self:_need(#s), s, #s)
    end,
    _field = function(self, s)
        self:_ber(#s)
        self:_string(s)
    end,
    _raw = function(self, v, l)
        ffi.copy(self:_need(l), v, l)
    end,
    len = function(self)
        return self.stop - self.ptr
    end,
    pack = function(self)
        local ptr = self.ptr
        local len = self.stop - ptr
        self.ptr = self.stop
        return ptr, len
    end,
}
pack_meths._varint32 = pack_meths._ber
ffi.metatype('struct tbuf', {__index=pack_meths})

function packer ()
    return ffi.new('struct tbuf', nil, nil, 0, C.fiber.pool)
end

ffi.cdef [[
    struct tread {
        u8 const * const beg;
        u8 const * ptr;
        i32 len;
        i32 haserror;
    };
]]
local unpack_t = ffi.typeof('struct tread')
local unpack_errors = setmetatable({}, {__mode="k"})

function unpacker(ptr, len)
    if type(ptr) == "string" and len == nil then
        len = #ptr
    end
    ptr = ffi.cast(u8p, ptr)
    return ffi.new(unpack_t, ptr, ptr, len, 0)
end

local function __line__()
    local d = debug.getinfo(2, 'lS')
    return d.source..':'..d.currentline
end

local function reader_func(name)
    local type = ffi.typeof(name.."*")
    local bytes = ffi.sizeof(ffi.typeof(name))
    local conv
    if name == 'u8' then
        conv = 'self.ptr[0]'
    else
        conv = 'ffi.cast(type, self.ptr)[0]'
    end
    local fab = loadstring(([[
        local ffi, type = ...
        return function (self)
            if self.haserror == 1 then
                return 0
            end
            if self.len < %d then
                self:set_error("Not enough data for %s at pos %%d", self:pos())
                return 0
            end
            local v = %s
            self:_inc(%d)
            return v
        end
    ]]):format(bytes, name, conv, bytes), __line__())
    return fab(ffi, type)
end

local function reader_func_n(name, conv)
    local fab = loadstring(([[
        local ffi, unpacker = ...
        return function (self, n)
            if self.haserror == 1 then
                return nil
            end
            if self.len < n then
                self:set_error("Not enough data for %s[%%d] at pos %%d", n, self:pos())
                return nil
            end
            local v = %s
            self:_inc(n)
            return v
        end
    ]]):format(name, conv), __line__())
    return fab(ffi, unpacker)
end

local function reader_field(name)
    local type = ffi.typeof(name.."*")
    local bytes = ffi.sizeof(ffi.typeof(name))
    local conv
    if name == 'u8' then
        conv = 'self.ptr[1]'
    else
        conv = 'ffi.cast(type, self.ptr+1)[0]'
    end
    local fab = loadstring(([[
        local ffi, type = ...
        return function (self)
            if self.haserror == 1 then
                return 0
            end
            if self.len < %d then
                self:set_error("Not enough data for field %s")
                return 0
            end
            if self.ptr[0] ~= %d then
                self:set_error("Size should be %d for field %s")
                return 0
            end
            local v = %s
            self:_inc(%d)
            return v
        end
    ]]):format(bytes+1, name, bytes, bytes, name, conv, bytes+1), __line__())
    return fab(ffi, type)
end

local function reader_field_n(name, conv)
    local fab = loadstring(([[
        local ffi, unpacker = ...
        return function (self)
            local n = self:ber()
            if self.haserror == 1 then
                return nil
            end
            if self.len < n then
                self:set_error("Not enough data for %s[%%d]", n)
                return nil
            end
            local v = %s
            self:_inc(n)
            return v
        end
    ]]):format(name, conv), __line__())
    return fab(ffi, unpacker)
end

unpack_meths = {
    _inc = function(self, n)
        self.ptr = self.ptr + n
        self.len = self.len - n
    end,
    set_error = function(self, err, ...)
        if self.haserror == 0 or self.haserror == 2 then
            local pos = ("(cur pos %d)"):format(self:pos())
            if select('#', ...) == 0 then
                err = err .. pos
            else
                err = err:format(...) .. pos
            end
            if self.haserror == 0 then
                self.haserror = 1
                unpack_errors[self] = err:format(...) .. pos
            else
                error(err)
            end
        end
    end,
    raise_on_error = function(self)
        if self.haserror == 0 then
            self.haserror = 2
        elseif self.haserror == 1 then
            error(self:error())
        end
    end,
    error = function(self)
        if self.haserror == 1 then
            return unpack_errors[self]
        end
        return nil
    end,
    pos = function(self)
        return self.ptr - self.beg
    end,
    set_pos = function(self, n)
        self.len = self.len + (self:pos() - n)
        self.ptr = self.beg + n
    end,
    reset = function(self)
        self.len = self.len + (self.ptr - self.beg)
        self.ptr = self.beg
    end,
    u8  = reader_func('u8'),
    u16 = reader_func('u16'),
    u32 = reader_func('u32'),
    u64 = reader_func('u64'),
    i8  = reader_func('i8'),
    i16 = reader_func('i16'),
    i32 = reader_func('i32'),
    i64 = reader_func('i64'),
    string = reader_func_n('string', 'ffi.string(self.ptr, n)'),
    skip = reader_func_n('skip', 'nil'),
    unpacker = reader_func_n('unpacker', 'unpacker(self.ptr, n)'),
    ber = function(self)
        if self.haserror == 1 then
            return 0
        end
        if self.len < 1 then
            self:set_error("Not enough data for ber")
            return 0
        end
        if self.ptr[0] < 128 then
            local v = self.ptr[0]
            self:_inc(1)
            return v
        end
        if self.len < 2 then
            self:set_error("Wrong ber at %d", self:pos())
            return 0
        end
        local v = bit.bor(bit.band(self.ptr[0], 0x7f) * 128, bit.band(self.ptr[1], 0x7f))
        if self.ptr[1] < 128 then
            self:_inc(2)
            return v
        end
        v = bit.bor(v * 128, bit.band(self.ptr[2], 0x7f))
        if self.ptr[2] < 128 then
            self:_inc(3)
            return v
        end
        local i = 3
        while i < self.len do
            v = v * 128 + self.ptr[i] % 128
            if self.ptr[i] < 128 then
                self:_inc(i+1)
                return v
            end
            i = i + 1
        end
        self:set_error("Wrong ber at %d", self:pos())
        return 0
    end,
    berconst = function(self, n)
        if self.haserror == 1 then
            return
        end
        if n < 128 then
            local v = self.ptr[0]
            if v ~= n then
                self:set_error("Ber doesn't match: want %d, but first byte is %d", n, v)
                return
            end
            self:_inc(i+1)
            return
        end
        local ptr = self.ptr
        local v = self:ber()
        if v ~= n then
            self.ptr = ptr
            self:set_error("Ber doesn't match: want %d, got %d", n, v)
        end
    end,
    field_u8 = reader_field('u8'),
    field_u16 = reader_field('u16'),
    field_u32 = reader_field('u32'),
    field_u64 = reader_field('u64'),
    field_i8 = reader_field('i8'),
    field_i16 = reader_field('i16'),
    field_i32 = reader_field('i32'),
    field_i64 = reader_field('i64'),
    field_ber = function(self)
        if self.haserror == 1 then
            return 0
        end
        if self.len < 2 then
            self:set_error("Not enough data for field u8")
            return 0
        end
        local bs = self.ptr[0]
        local ptr = self.ptr
        local v
        if bs > 5 then
            self:set_error("Could not parse ber with size %d > 5", bs)
            return 0
        end
        self:_inc(bs)
        if bs == 1 then
            v = ptr[1]
        elseif bs == 2 then
            return (ptr[1]-128)*128 + ptr[2]
        end
        local midv = ptr[1]*128*128 + ptr[2]*128 - (128^3 + 128^2) + ptr[3]
        if bs == 3 then
            return midv
        elseif bs == 4 then
            return (midv-128)*128 + ptr[4]
        elseif bs == 5 then
            return (midv-128)*128*128 + (ptr[4]-128)*128 + ptr[5]
        end
    end,
    field_string = reader_field_n('string', 'ffi.string(self.ptr, n)'),
    field_skip = reader_field_n('skip', 'nil'),
    field_unpacker = reader_field_n('unpacker', 'unpacker(self.ptr, n)'),
}
local unpack_mt = {
    __index = unpack_meths,
    __len = function(self) return self.stop - self.ptr end
}
ffi.metatype(unpack_t, unpack_mt)
