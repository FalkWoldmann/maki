-- Adds the flags that make build, test and package commands print only what
-- the model acts on: failures, diagnostics and totals, never the per-test and
-- per-crate progress lines. The tool's own flags do the filtering, so there is
-- nothing to parse and the output still streams.
--
-- A command is left alone when its output goes somewhere other than the model
-- (a pipe, a file, a substitution), when the model already chose a verbosity
-- or format flag, or when the command does not parse.

local M = {}

-- `words` are matched against the command name and its leading arguments: a
-- string exactly, a table as alternatives. `skip` is a pattern for arguments
-- ignored while matching, such as cargo's `+nightly`. `unless` are patterns
-- over every other argument, including those after `--`, that keep the
-- command as the model wrote it.
local RULES = {
  {
    words = { "cargo", { "test", "build", "check", "clippy", "run", "doc" } },
    skip = "^%+",
    flags = "-q",
    unless = {
      "^%-q$",
      "^%-%-quiet$",
      "^%-v+$",
      "^%-%-verbose$",
      "^%-%-message%-format",
      "^%-%-no%-run$",
      "^%-%-nocapture$",
      "^%-%-show%-output$",
    },
  },
  {
    words = { "cargo", "nextest", "run" },
    skip = "^%+",
    flags = "--status-level fail --final-status-level fail --hide-progress-bar",
    unless = {
      "^%-%-status%-level",
      "^%-%-final%-status%-level",
      "^%-%-hide%-progress%-bar$",
      "^%-v+$",
      "^%-%-verbose$",
      "^%-%-cargo%-verbose$",
      "^%-%-cargo%-quiet$",
      "^%-%-no%-capture$",
      "^%-%-nocapture$",
    },
  },
  {
    words = { "git", "log" },
    flags = "--oneline",
    unless = { "^%-%-oneline$", "^%-%-format", "^%-%-pretty", "^%-%-date", "^%-p$", "^%-%-patch$", "^%-%-stat$" },
  },
  {
    words = { "pytest" },
    flags = "-q",
    unless = { "^%-q+$", "^%-%-quiet$", "^%-v+$", "^%-%-verbose$" },
  },
  {
    words = { { "pip", "pip3" }, "install" },
    flags = "-q",
    unless = { "^%-q+$", "^%-%-quiet$", "^%-v+$", "^%-%-verbose$", "^%-%-dry%-run$", "^%-%-report" },
  },
  {
    words = { "npm", { "install", "i", "ci" } },
    flags = "--no-fund",
    unless = { "^%-%-fund$", "^%-%-no%-fund$" },
  },
}

-- Redirects that leave stdout on its way to the model: stderr only, input,
-- and descriptor duplication such as `2>&1`.
local PASSTHROUGH_REDIRECTS = { "^2>", "^<", "^%d*>>?&%d$" }

local CAPTURING_TYPES = {
  command_substitution = true,
  process_substitution = true,
}

local function text(node, source)
  return maki.treesitter.get_node_text(node, source)
end

local function matches_any(s, patterns)
  for _, pattern in ipairs(patterns) do
    if s:match(pattern) then
      return true
    end
  end
  return false
end

local function word_matches(expected, actual)
  if type(expected) == "string" then
    return actual == expected
  end
  for _, alternative in ipairs(expected) do
    if actual == alternative then
      return true
    end
  end
  return false
end

-- Returns the byte offset the flags go after, or nil when the rule does not
-- apply. The offset is the end of the last matched word, which always comes
-- before `--` and before any argument the flags could be mistaken for.
local function match_rule(rule, name, args, source)
  if not word_matches(rule.words[1], text(name, source)) then
    return nil
  end
  local offset = select(3, name:end_())
  local next_word = 2
  for _, arg in ipairs(args) do
    local arg_text = text(arg, source)
    if next_word > #rule.words then
      if matches_any(arg_text, rule.unless) then
        return nil
      end
    elseif word_matches(rule.words[next_word], arg_text) then
      offset = select(3, arg:end_())
      next_word = next_word + 1
    elseif not (rule.skip and arg_text:match(rule.skip)) then
      return nil
    end
  end
  if next_word <= #rule.words then
    return nil
  end
  return offset
end

local function redirect_captures(node, source)
  for child in node:iter_children() do
    if child:type() == "file_redirect" then
      local redirect = text(child, source):gsub("%s+", "")
      if not matches_any(redirect, PASSTHROUGH_REDIRECTS) then
        return true
      end
    end
  end
  return false
end

local function command_insertion(node, source)
  local name = node:field("name")[1]
  if not name or redirect_captures(node, source) then
    return nil
  end
  local args = {}
  for child, field in node:iter_children() do
    if field == "argument" then
      args[#args + 1] = child
    end
  end
  for _, rule in ipairs(RULES) do
    local offset = match_rule(rule, name, args, source)
    if offset then
      return { offset = offset, flags = rule.flags }
    end
  end
  return nil
end

local function collect(node, source, captured, out)
  local kind = node:type()
  if kind == "command" then
    if not captured then
      out[#out + 1] = command_insertion(node, source)
    end
    return
  end

  local child_captured = captured or CAPTURING_TYPES[kind] or false
  if kind == "redirected_statement" and redirect_captures(node, source) then
    child_captured = true
  end

  local children = node:named_children()
  for i, child in ipairs(children) do
    local piped = kind == "pipeline" and i < #children
    collect(child, source, child_captured or piped, out)
  end
end

function M.rewrite(command)
  local parser = maki.treesitter.get_parser(command, "bash")
  if not parser then
    return nil
  end
  local root = parser:parse()[1]:root()
  if root:has_error() then
    return nil
  end

  local insertions = {}
  collect(root, command, false, insertions)
  if #insertions == 0 then
    return nil
  end

  table.sort(insertions, function(a, b)
    return a.offset > b.offset
  end)
  local rewritten = command
  for _, insertion in ipairs(insertions) do
    rewritten = rewritten:sub(1, insertion.offset) .. " " .. insertion.flags .. rewritten:sub(insertion.offset + 1)
  end
  return rewritten
end

return M
