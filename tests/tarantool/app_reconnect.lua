ffi = require('ffi')

ffi.cdef[[
  void close(int)
]]

function self_close()
  ffi.C.close(box.session.fd())
end

box.cfg{listen=3301}
