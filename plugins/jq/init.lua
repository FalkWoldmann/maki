local truncate = require("maki.truncate")
local shorten_path = require("maki.shorten_path")
local output_limits = require("maki.output_limits")
local helpers = require("jq_helpers")

local DESCRIPTION = [[Run a jq filter over structured data and return only what the filter selects.

- Reads JSON, YAML, TOML, XML, CSV, TSV, CBOR, or raw lines (`from`), writes any of them (`to`, default json).
- Give either `path` (format guessed from the extension) or `input` text, e.g. JSON printed by another tool.
- Output is compact, one result per line. Filter before reading: `.dependencies | keys`, `.[] | select(.size > 1000) | .name`.]]

local opts = maki.api.register_options(output_limits.extend({}))

maki.api.register_prompt_hint({
  slot = "tool_usage",
  content = [[
- Use the **jq** tool to pull a few fields out of JSON, YAML, or TOML instead of reading the whole file or output.]],
})

maki.api.register_tool({
  name = "jq",
  kind = "read",
  description = DESCRIPTION,

  schema = {
    type = "object",
    properties = {
      filter = { type = "string", description = "jq filter, e.g. `.items[] | .name`", required = true },
      path = { type = "string", description = "Absolute path of a file to filter" },
      input = { type = "string", description = "Text to filter when there is no file" },
      from = {
        type = "string",
        enum = helpers.FORMATS,
        description = "Input format; defaults from the path's extension, else json",
      },
      to = { type = "string", enum = helpers.FORMATS, description = "Output format (default json)" },
      slurp = { type = "boolean", description = "Collect all input values into one array first" },
    },
  },

  header = function(input)
    local buf = maki.ui.buf()
    local target = input.path and shorten_path(input.path) or "input"
    buf:line({ { input.filter or "", "command" }, { "  " .. target, "path" } })
    return buf
  end,

  handler = function(input, ctx)
    local result = helpers.run(input, function(path)
      local content, err = maki.fs.read(path)
      if content then
        ctx:record_read(path)
      end
      return content, err
    end)
    if not result.is_error then
      local max_lines, max_bytes = output_limits.resolve(opts, ctx)
      result.llm_output = truncate(result.llm_output, max_lines, max_bytes)
    end
    return result
  end,
})
