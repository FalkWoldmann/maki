-- Runs a plain `jq [-r|-c|-s] FILTER FILE...` in-process through maki.jq, so
-- the model's habit of reaching for jq works without the binary and without a
-- process. Anything outside that shape (pipes, expansions, globs, redirects,
-- other flags, stdin) returns nil and runs through bash as before.

local M = {}

M.ERROR_EXIT = 5

local NAMES = { jq = true, jaq = true }
local SHORT_FLAGS = { r = "raw", c = "compact", s = "slurp" }
local LONG_FLAGS = { ["--raw-output"] = "raw", ["--compact-output"] = "compact", ["--slurp"] = "slurp" }
-- A bare word holding any of these would be expanded by bash first.
local EXPANDING = "[%$`\\%*%?%[~{}]"
local DQUOTE_SPECIAL = "[%$`\\]"

local function literal(node, source)
  local kind, text = node:type(), maki.treesitter.get_node_text(node, source)
  if kind == "word" then
    return not text:find(EXPANDING) and text or nil
  elseif kind == "raw_string" then
    return text:sub(2, -2)
  elseif kind == "string" then
    local inner = text:sub(2, -2)
    return not inner:find(DQUOTE_SPECIAL) and inner or nil
  end
  return nil
end

local function only_command(root)
  local found
  for child in root:iter_children() do
    if child:named() and child:type() ~= "comment" then
      if found then
        return nil
      end
      found = child
    end
  end
  return found and found:type() == "command" and found or nil
end

local function words(command)
  local parser = maki.treesitter.get_parser(command, "bash")
  if not parser then
    return nil
  end
  local root = parser:parse()[1]:root()
  if root:has_error() then
    return nil
  end
  local cmd = only_command(root)
  if not cmd then
    return nil
  end
  local out = {}
  for child in cmd:iter_children() do
    if child:named() then
      local kind = child:type()
      if #out == 0 and kind ~= "command_name" then
        return nil
      end
      local value = kind == "command_name" and maki.treesitter.get_node_text(child, command)
        or literal(child, command)
      if not value then
        return nil
      end
      out[#out + 1] = value
    end
  end
  return out
end

local function apply_flag(parsed, arg)
  local long = LONG_FLAGS[arg]
  if long then
    parsed[long] = true
    return true
  end
  if not arg:match("^%-[rcs]+$") then
    return false
  end
  for flag in arg:sub(2):gmatch(".") do
    parsed[SHORT_FLAGS[flag]] = true
  end
  return true
end

-- The pieces of a command this module can run, or nil.
function M.parse(command)
  local args = words(command)
  if not args or not NAMES[args[1]] then
    return nil
  end
  local parsed = { files = {} }
  for i = 2, #args do
    local arg = args[i]
    local is_flag = arg:sub(1, 1) == "-" and arg ~= "-"
    if parsed.filter == nil and is_flag then
      if not apply_flag(parsed, arg) then
        return nil
      end
    elseif parsed.filter == nil then
      parsed.filter = arg
    elseif is_flag or arg == "-" then
      return nil
    else
      parsed.files[#parsed.files + 1] = arg
    end
  end
  if not parsed.filter or #parsed.files == 0 then
    return nil
  end
  return parsed
end

-- Output as jq would print it (compact, since the reader is the model), or
-- nil plus a jq-style error. Several files form one input stream, as in jq.
function M.run(parsed, dir, read)
  local inputs = {}
  for _, file in ipairs(parsed.files) do
    local path = file:sub(1, 1) == "/" and file or (dir .. "/" .. file)
    local content, err = read(path)
    if not content then
      return nil, "jq: error: Could not open " .. file .. ": " .. tostring(err)
    end
    inputs[#inputs + 1] = content
  end
  local out, err = maki.jq.run(parsed.filter, table.concat(inputs, "\n"), {
    to = parsed.raw and "raw" or "json",
    slurp = parsed.slurp,
  })
  if not out then
    return nil, "jq: error: " .. tostring(err)
  end
  return out
end

return M
