-- LSP base protocol: `Content-Length` framed JSON bodies, and the file URIs
-- the messages name documents by.

local M = {}

local HEADER_END = "\r\n\r\n"
local LENGTH_PATTERN = "[Cc]ontent%-[Ll]ength:%s*(%d+)"
local FILE_SCHEME = "file://"
local URI_SAFE = "[^%w%-%._~/:]"

function M.encode(message)
  local body = assert(maki.json.encode(message))
  return "Content-Length: " .. #body .. HEADER_END .. body
end

-- Returns a function that takes stdout chunks as they arrive and returns the
-- bodies they completed, plus an error once the stream stops making sense.
function M.decoder()
  local pending = ""
  return function(chunk)
    pending ..= chunk
    local bodies = {}
    while true do
      local header_end = pending:find(HEADER_END, 1, true)
      if not header_end then
        return bodies
      end
      local length = tonumber(pending:sub(1, header_end - 1):match(LENGTH_PATTERN))
      if not length then
        return bodies, "message header without Content-Length"
      end
      local body_start = header_end + #HEADER_END
      local body_end = body_start + length - 1
      if #pending < body_end then
        return bodies
      end
      table.insert(bodies, pending:sub(body_start, body_end))
      pending = pending:sub(body_end + 1)
    end
  end
end

function M.uri(path)
  local unix = path:gsub("\\", "/")
  local encoded = unix:gsub(URI_SAFE, function(c)
    return string.format("%%%02X", c:byte())
  end)
  return FILE_SCHEME .. (encoded:sub(1, 1) == "/" and "" or "/") .. encoded
end

-- Servers spell the same file with their own percent-encoding, so documents
-- are keyed by the decoded path rather than by URI.
function M.path(uri)
  local path = uri:sub(#FILE_SCHEME + 1):gsub("%%(%x%x)", function(hex)
    return string.char(tonumber(hex, 16))
  end)
  return path:match("^/%a:") and path:sub(2) or path
end

return M
