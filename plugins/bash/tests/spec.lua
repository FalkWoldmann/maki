local jq_inline = require("jq_inline")

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

local DIR = "/repo"
local FILES = {
  ["/repo/package.json"] = '{"name":"maki","deps":{"a":"1","b":"2"}}',
  ["/repo/one.json"] = '{"n":1}',
  ["/repo/two.json"] = '{"n":2}',
}

local function read(path)
  local content = FILES[path]
  if content then
    return content
  end
  return nil, "No such file or directory"
end

local function run(command)
  local parsed = jq_inline.parse(command)
  assert(parsed, "expected an inline jq command: " .. command)
  return jq_inline.run(parsed, DIR, read)
end

case("plain_jq_runs_in_process", function()
  eq(run("jq '.deps | keys' package.json"), '["a","b"]\n')
end)

case("raw_output_drops_the_quotes", function()
  eq(run("jq -r .name package.json"), "maki\n")
  eq(run('jq --raw-output ".name" package.json'), "maki\n")
end)

case("combined_flags_and_several_files", function()
  eq(run("jq -sc 'map(.n) | add' one.json two.json"), "3\n")
  eq(run("jq .n /repo/one.json two.json"), "1\n2\n")
end)

case("errors_read_like_jq", function()
  local out, err = run("jq . missing.json")
  eq(out, nil)
  assert(err:find("Could not open missing.json", 1, true), err)
  local _, bad = run("jq '.[' package.json")
  assert(bad:find("jq: error", 1, true), bad)
end)

case("anything_else_falls_through_to_bash", function()
  for _, command in ipairs({
    "cat package.json | jq .name",
    "jq .name package.json > out.txt",
    "jq .name $FILE",
    "jq .name *.json",
    'jq ".$key" package.json',
    "jq --arg x 1 .name package.json",
    "jq .name",
    "jq . -",
    "jq .name package.json && echo done",
    "FOO=1 jq .name package.json",
    "yq .name package.json",
    "jq .name $(ls)",
  }) do
    eq(jq_inline.parse(command), nil, command)
  end
end)

if #failures > 0 then
  error(#failures .. " case(s) failed:\n\n" .. table.concat(failures, "\n\n"))
end
