ffi = require('ffi')

ffi.cdef[[
  int shutdown(int, int);
]]

function self_close()
  local SHUT_RDWR = 2
  ffi.C.shutdown(box.session.fd(), SHUT_RDWR)
end

box.cfg{listen=3301}
