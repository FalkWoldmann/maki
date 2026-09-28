local helpers = require("jq_helpers")

local failures = {}

local function case(name, fn)
  local ok, err = pcall(fn)
  if not ok then
    table.insert(failures, name .. ": " .. tostring(err))
  end
end

local function eq(actual, expected, msg)
  if actual ~= expected then
    error((msg or "") .. "\nexpected: " .. tostring(expected) .. "\n  actual: " .. tostring(actual))
  end
end

local CARGO_TOML = '[package]\nname = "maki"\n\n[dependencies]\nserde = "1"\n'
local TOML_PATH = "/repo/Cargo.toml"

local function fake_read(files)
  return function(path)
    local content = files[path]
    if content then
      return content
    end
    return nil, "no such file"
  end
end

case("a_toml_file_is_read_by_its_extension", function()
  local result = helpers.run({ filter = ".package.name", path = TOML_PATH }, fake_read({ [TOML_PATH] = CARGO_TOML }))
  eq(result.is_error, nil, result.llm_output)
  eq(result.llm_output, '"maki"\n')
end)

case("inline_input_defaults_to_json", function()
  local result = helpers.run({ filter = ".[].name", input = '[{"name":"a"},{"name":"b"}]' }, fake_read({}))
  eq(result.llm_output, '"a"\n"b"\n')
end)

case("output_can_be_another_format", function()
  local result = helpers.run({ filter = ".package", input = CARGO_TOML, from_format = "toml", to_format = "yaml" }, fake_read({}))
  eq(result.is_error, nil, result.llm_output)
  assert(result.llm_output:find("name:maki", 1, true), result.llm_output)
end)

case("exactly_one_source_is_required", function()
  eq(helpers.run({ filter = "." }, fake_read({})).llm_output, helpers.ONE_SOURCE_ERR)
  eq(helpers.run({ filter = ".", path = TOML_PATH, input = "{}" }, fake_read({})).llm_output, helpers.ONE_SOURCE_ERR)
end)

case("errors_come_back_as_tool_errors", function()
  local bad_filter = helpers.run({ filter = ".[", input = "{}" }, fake_read({}))
  eq(bad_filter.is_error, true)
  assert(bad_filter.llm_output:find("jq error", 1, true), bad_filter.llm_output)
  local missing = helpers.run({ filter = ".", path = "/nope.json" }, fake_read({}))
  eq(missing.is_error, true)
  local bad_format = helpers.run({ filter = ".", input = "{}", from_format = "ini" }, fake_read({}))
  eq(bad_format.is_error, true)
end)

case("an_empty_result_says_so", function()
  eq(helpers.run({ filter = "empty", input = "{}" }, fake_read({})).llm_output, helpers.NO_OUTPUT)
end)

case("format_for_knows_common_extensions", function()
  eq(helpers.format_for("/a/b.YML"), "yaml")
  eq(helpers.format_for("/a/b.toml"), "toml")
  eq(helpers.format_for("/a/b.unknown"), "json")
  eq(helpers.format_for(nil), "json")
end)

if #failures > 0 then
  error(#failures .. " case(s) failed:\n\n" .. table.concat(failures, "\n\n"))
end
