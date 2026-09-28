local M = {}

M.FORMATS = { "json", "yaml", "toml", "xml", "cbor", "csv", "tsv", "raw" }
M.NO_OUTPUT = "(no output)"
M.ONE_SOURCE_ERR = "error: give exactly one of `path` or `input`"

local EXTENSION_FORMATS = { json = "json", yaml = "yaml", yml = "yaml", toml = "toml", xml = "xml", csv = "csv", tsv = "tsv" }
local DEFAULT_FORMAT = "json"

function M.format_for(path)
  local ext = path and path:match("%.(%w+)$")
  return ext and EXTENSION_FORMATS[ext:lower()] or DEFAULT_FORMAT
end

-- Runs one tool call. `read` loads a file and records it as read; it is
-- passed in so the spec can stand in for the filesystem.
function M.run(input, read)
  if (input.path == nil) == (input.input == nil) then
    return { llm_output = M.ONE_SOURCE_ERR, is_error = true }
  end

  local text = input.input
  if input.path then
    local content, err = read(input.path)
    if not content then
      return { llm_output = "read error: " .. tostring(err), is_error = true }
    end
    text = content
  end

  local out, err = maki.jq.run(input.filter, text, {
    from = input.from_format or M.format_for(input.path),
    to = input.to_format,
    slurp = input.slurp,
  })
  if not out then
    return { llm_output = "jq error: " .. tostring(err), is_error = true }
  end
  return { llm_output = out == "" and M.NO_OUTPUT or out }
end

return M
