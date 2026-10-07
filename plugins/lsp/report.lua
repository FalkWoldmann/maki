-- Turns published diagnostics into the few lines the model reads after an
-- edit: errors in the file it touched, and errors it just caused elsewhere.

local M = {}

local SEVERITY_ERROR = 1
local MAX_MESSAGE_CHARS = 200
local TRUNCATED = "..."

M.MAX_REPORTED = 20
M.NO_ERRORS = "rust-analyzer: no errors"

local function errors(diagnostics)
  local out = {}
  for _, diagnostic in diagnostics or {} do
    if diagnostic.severity == SEVERITY_ERROR then
      table.insert(out, diagnostic)
    end
  end
  return out
end

local function key(path, diagnostic)
  return `{path}\n{diagnostic.code or ""}\n{diagnostic.message}`
end

local function inside(root, path)
  return path:sub(1, #root + 1) == root .. "/"
end

-- Errors in other files under {root}, keyed so a later report can tell which
-- ones the edit introduced. Line numbers stay out of the key: an edit above an
-- old error moves it without making it new.
function M.snapshot(by_path, root, edited)
  local seen = {}
  for path, diagnostics in by_path do
    if path ~= edited and inside(root, path) then
      for _, diagnostic in errors(diagnostics) do
        seen[key(path, diagnostic)] = true
      end
    end
  end
  return seen
end

local function one_line(message)
  local flat = message:gsub("%s*\n%s*", "; ")
  if #flat <= MAX_MESSAGE_CHARS then
    return flat
  end
  return flat:sub(1, MAX_MESSAGE_CHARS - #TRUNCATED) .. TRUNCATED
end

local function format(root, entry)
  local start = entry.diagnostic.range.start
  local code = entry.diagnostic.code and (tostring(entry.diagnostic.code) .. " ") or ""
  local rel = maki.fs.relpath(root, entry.path)
  return `{rel}:{start.line + 1}:{start.character + 1} {code}{one_line(entry.diagnostic.message)}`
end

local function by_position(a, b)
  if a.edited ~= b.edited then
    return a.edited
  end
  if a.path ~= b.path then
    return a.path < b.path
  end
  local pa, pb = a.diagnostic.range.start, b.diagnostic.range.start
  if pa.line ~= pb.line then
    return pa.line < pb.line
  end
  return pa.character < pb.character
end

-- {baseline} is the `snapshot` taken before the edit was checked.
function M.build(by_path, root, edited, baseline)
  local entries = {}
  local older = 0
  for path, diagnostics in by_path do
    if path == edited or inside(root, path) then
      for _, diagnostic in errors(diagnostics) do
        if path == edited or not baseline[key(path, diagnostic)] then
          table.insert(entries, { path = path, diagnostic = diagnostic, edited = path == edited })
        else
          older += 1
        end
      end
    end
  end

  if #entries == 0 then
    return older == 0 and M.NO_ERRORS or `rust-analyzer: no new errors ({older} already in other files)`
  end

  table.sort(entries, by_position)
  local lines = { `rust-analyzer: {#entries} error{#entries == 1 and "" or "s"}` }
  for i = 1, math.min(#entries, M.MAX_REPORTED) do
    table.insert(lines, format(root, entries[i]))
  end
  if #entries > M.MAX_REPORTED then
    table.insert(lines, `... and {#entries - M.MAX_REPORTED} more`)
  end
  return table.concat(lines, "\n")
end

return M
