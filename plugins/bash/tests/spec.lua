local inline = require("inline")

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
  ["/repo/notes.txt"] = "first\n\nthird\n",
}

local function read(path)
  local content = FILES[path]
  if content then
    return content
  end
  return nil, "No such file or directory"
end

local function plan_of(command)
  local plan = inline.parse(command)
  assert(plan, "expected an in-process plan: " .. command)
  return plan
end

local function run(command)
  return inline.run(plan_of(command), DIR, read)
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

case("cat_into_jq_reads_the_files_itself", function()
  local plan = plan_of("cat one.json two.json | jq .n")
  eq(plan.upstream, nil)
  eq(inline.run(plan, DIR, read), "1\n2\n")
end)

case("plain_cat_reads_without_a_process", function()
  local out, code = run("cat notes.txt")
  eq(out, "first\n\nthird\n")
  eq(code, 0)
  local both = run("cat one.json two.json")
  eq(both, '{"n":1}{"n":2}')
end)

case("anything_else_upstream_runs_then_filters", function()
  local plan = plan_of("gh pr list --json title | jq -r '.[].title'")
  eq(plan.upstream, "gh pr list --json title")
  eq(inline.filter(plan, '[{"title":"a"},{"title":"b"}]'), "a\nb\n")
  eq(plan_of("curl -s $URL 2>/dev/null | grep x | jq .").upstream, "curl -s $URL 2>/dev/null | grep x")
end)

case("errors_read_like_the_real_tools", function()
  local out, code = run("jq . missing.json")
  eq(code, inline.JQ_ERROR_EXIT)
  assert(out:find("Could not open missing.json", 1, true), out)
  local bad, bad_code = run("jq '.[' package.json")
  eq(bad_code, inline.JQ_ERROR_EXIT)
  assert(bad:find("jq: error", 1, true), bad)
  local cat_err, cat_code = run("cat missing.txt")
  eq(cat_code, inline.CAT_ERROR_EXIT)
  assert(cat_err:find("cat: missing.txt", 1, true), cat_err)
end)

case("anything_else_falls_through_to_bash", function()
  for _, command in ipairs({
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
    "gh api x |& jq .",
    "gh api x | jq . | head -1",
    "gh api x | jq --arg a 1 .",
    "gh api x | jq . extra.json",
    "gh api x | jq . > out.json",
    "cat -n notes.txt",
    "cat",
    "cat *.txt",
    "cat notes.txt | head",
  }) do
    eq(inline.parse(command), nil, command)
  end
end)

if #failures > 0 then
  error(#failures .. " case(s) failed:\n\n" .. table.concat(failures, "\n\n"))
end
