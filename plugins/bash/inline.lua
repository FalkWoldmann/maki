-- Commands answered in-process instead of by a process: a plain `cat FILE...`,
-- and jq through maki.jq, fed by files, by `cat FILE... |`, or by the stdout of
-- whatever runs before the last `|`. Only literal arguments qualify; anything
-- bash would expand, redirect, or treat specially returns nil and runs as usual.

local M = {}

M.JQ_ERROR_EXIT = 5
M.CAT_ERROR_EXIT = 1

local JQ_NAMES = { jq = true, jaq = true }
local CAT = "cat"
local SHORT_FLAGS = { r = "raw", c = "compact", s = "slurp" }
local LONG_FLAGS = { ["--raw-output"] = "raw", ["--compact-output"] = "compact", ["--slurp"] = "slurp" }
-- A bare word holding any of these would be expanded by bash first.
local EXPANDING = "[%$`\\%*%?%[~{}]"
local DQUOTE_SPECIAL = "[%$`\\]"
local PIPE = "|"

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

local function named_children(node)
  local out = {}
  for child in node:iter_children() do
    if child:named() and child:type() ~= "comment" then
      out[#out + 1] = child
    end
  end
  return out
end

-- Command name and literal arguments of a `command` node, or nil.
local function words(node, source)
  if node:type() ~= "command" then
    return nil
  end
  local out = {}
  for _, child in ipairs(named_children(node)) do
    local kind = child:type()
    if #out == 0 and kind ~= "command_name" then
      return nil
    end
    local value = kind == "command_name" and maki.treesitter.get_node_text(child, source) or literal(child, source)
    if not value then
      return nil
    end
    out[#out + 1] = value
  end
  return out
end

local function apply_flag(jq, arg)
  local long = LONG_FLAGS[arg]
  if long then
    jq[long] = true
    return true
  end
  if not arg:match("^%-[rcs]+$") then
    return false
  end
  for flag in arg:sub(2):gmatch(".") do
    jq[SHORT_FLAGS[flag]] = true
  end
  return true
end

local function is_flag(arg)
  return arg:sub(1, 1) == "-"
end

local function parse_jq(args)
  if not args or not JQ_NAMES[args[1]] then
    return nil
  end
  local jq, files = {}, {}
  for i = 2, #args do
    local arg = args[i]
    if jq.filter == nil and is_flag(arg) and arg ~= "-" then
      if not apply_flag(jq, arg) then
        return nil
      end
    elseif jq.filter == nil then
      jq.filter = arg
    elseif is_flag(arg) then
      return nil
    else
      files[#files + 1] = arg
    end
  end
  return jq.filter and jq, files or nil
end

local function parse_cat(args)
  if not args or args[1] ~= CAT or #args < 2 then
    return nil
  end
  local files = {}
  for i = 2, #args do
    if is_flag(args[i]) then
      return nil
    end
    files[#files + 1] = args[i]
  end
  return files
end

-- `a | b | jq ...`: the jq stage and the text of everything before it. `|&`
-- also pipes stderr, which the in-process path cannot, so it does not qualify.
local function parse_pipeline(pipeline, source)
  local stages = named_children(pipeline)
  for child in pipeline:iter_children() do
    if not child:named() and child:type() ~= PIPE then
      return nil
    end
  end
  local jq, files = parse_jq(words(stages[#stages], source))
  if not jq or #files > 0 then
    return nil
  end
  if #stages == 2 then
    local cat_files = parse_cat(words(stages[1], source))
    if cat_files then
      return { kind = "jq", jq = jq, files = cat_files }
    end
  end
  local _, _, from = stages[1]:start()
  local _, _, to = stages[#stages - 1]:end_()
  return { kind = "jq", jq = jq, upstream = source:sub(from + 1, to) }
end

-- What to run in-process for `command`, or nil to run it through bash.
function M.parse(command)
  local parser = maki.treesitter.get_parser(command, "bash")
  if not parser then
    return nil
  end
  local root = parser:parse()[1]:root()
  if root:has_error() then
    return nil
  end
  local top = named_children(root)
  if #top ~= 1 then
    return nil
  end
  local node = top[1]
  if node:type() == "pipeline" then
    return parse_pipeline(node, command)
  end
  local args = words(node, command)
  local jq, files = parse_jq(args)
  if jq then
    return #files > 0 and { kind = "jq", jq = jq, files = files } or nil
  end
  local cat_files = parse_cat(args)
  return cat_files and { kind = "cat", files = cat_files } or nil
end

local function read_all(plan, dir, read, prefix)
  local contents = {}
  for _, file in ipairs(plan.files) do
    local path = file:sub(1, 1) == "/" and file or (dir .. "/" .. file)
    local content, err = read(path)
    if not content then
      return nil, prefix .. " " .. file .. ": " .. tostring(err)
    end
    contents[#contents + 1] = content
  end
  return contents
end

-- jq output for `text`, compact since the reader is the model, or nil plus a
-- jq-style error.
function M.filter(plan, text)
  local out, err = maki.jq.run(plan.jq.filter, text, {
    to = plan.jq.raw and "raw" or "json",
    slurp = plan.jq.slurp,
  })
  if not out then
    return nil, "jq: error: " .. tostring(err)
  end
  return out
end

-- Output and exit code for a plan that needs no process (no `upstream`).
function M.run(plan, dir, read)
  if plan.kind == "cat" then
    local contents, err = read_all(plan, dir, read, CAT .. ":")
    if not contents then
      return err, M.CAT_ERROR_EXIT
    end
    return table.concat(contents), 0
  end
  local contents, err = read_all(plan, dir, read, "jq: error: Could not open")
  if not contents then
    return err, M.JQ_ERROR_EXIT
  end
  local out, filter_err = M.filter(plan, table.concat(contents, "\n"))
  if not out then
    return filter_err, M.JQ_ERROR_EXIT
  end
  return out, 0
end

return M
