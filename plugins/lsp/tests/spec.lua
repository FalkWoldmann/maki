local Client = require("client")
local Report = require("report")
local Rpc = require("rpc")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq
local has = th.has

local ROOT = "/work/demo"
local EDITED = ROOT .. "/src/util.rs"
local OTHER = ROOT .. "/src/main.rs"

local function diagnostic(line, message, opts)
  opts = opts or {}
  return {
    severity = opts.severity or 1,
    code = opts.code,
    message = message,
    range = { start = { line = line, character = opts.character or 0 } },
  }
end

-- Stands in for the server process: records what the client sends and lets
-- a case answer from inside `write`, the way the real server would reply.
local function fake_proc(on_message)
  local sent = {}
  local proc = {
    write = function(_, frame)
      local message = maki.json.decode(frame:match("\r\n\r\n(.*)$"))
      table.insert(sent, message)
      if on_message then
        on_message(message)
      end
      return true
    end,
    kill = function() end,
  }
  return proc, sent
end

local function methods(sent)
  local out = {}
  for _, message in sent do
    table.insert(out, message.method or "<response>")
  end
  return table.concat(out, ",")
end

case("decoder_reassembles_messages_split_across_chunks", function()
  local stream = Rpc.encode({ id = 1, text = "héllo" }) .. Rpc.encode({ id = 2 })
  local decode = Rpc.decoder()
  local bodies = {}
  for _, cut in { { 1, 10 }, { 11, 30 }, { 31, #stream } } do
    for _, body in decode(stream:sub(cut[1], cut[2])) do
      table.insert(bodies, (maki.json.decode(body)))
    end
  end
  eq(#bodies, 2, "both messages come out once their last byte arrived")
  eq(bodies[1].text, "héllo", "Content-Length counts bytes, so multi-byte text survives")
  eq(bodies[2].id, 2, "the second message follows the first")
end)

case("decoder_rejects_a_header_without_length", function()
  local bodies, err = Rpc.decoder()("X-Other: 1\r\n\r\n{}")
  eq(#bodies, 0, "nothing decodes without a length")
  has(err, "Content-Length", "the error names the missing header")
end)

case("uri_round_trips_paths_that_need_escaping", function()
  local path = "/tmp/my crate/src/ü.rs"
  has(Rpc.uri(path), "my%20crate", "spaces are percent-encoded")
  eq(Rpc.path(Rpc.uri(path)), path, "decoding gives the path back")
end)

case("path_drops_the_slash_before_a_windows_drive", function()
  eq(Rpc.path("file:///C:/work/main.rs"), "C:/work/main.rs")
end)

case("report_lists_the_edited_file_first_with_one_based_positions", function()
  local report = Report.build({
    [OTHER] = { diagnostic(0, "unresolved import", { code = "E0432" }) },
    [EDITED] = { diagnostic(4, "mismatched types\nexpected `u32`, found `&str`", { code = "E0308", character = 8 }) },
  }, ROOT, EDITED, {})
  eq(
    report,
    "rust-analyzer: 2 errors\n"
      .. "src/util.rs:5:9 E0308 mismatched types; expected `u32`, found `&str`\n"
      .. "src/main.rs:1:1 E0432 unresolved import"
  )
end)

case("report_skips_warnings_and_files_outside_the_workspace", function()
  local report = Report.build({
    [EDITED] = { diagnostic(0, "unused variable", { severity = 2 }) },
    ["/rustlib/src/core/ops.rs"] = { diagnostic(0, "the trait is not implemented") },
  }, ROOT, EDITED, {})
  eq(report, Report.NO_ERRORS)
end)

case("report_leaves_out_errors_elsewhere_that_predate_the_edit", function()
  local before = { [OTHER] = { diagnostic(9, "old error") } }
  local baseline = Report.snapshot(before, ROOT, EDITED)
  local after = { [OTHER] = { diagnostic(12, "old error"), diagnostic(3, "new error") } }
  local report = Report.build(after, ROOT, EDITED, baseline)
  has(report, "src/main.rs:4:1 new error", "an error the edit caused is reported")
  eq(report:find("old error", 1, true), nil, "one that only moved down is not")
  eq(Report.build(before, ROOT, EDITED, baseline), "rust-analyzer: no new errors (1 already in other files)")
end)

case("report_caps_the_list", function()
  local many = {}
  for i = 1, Report.MAX_REPORTED + 3 do
    table.insert(many, diagnostic(i, "e" .. i))
  end
  has(Report.build({ [EDITED] = many }, ROOT, EDITED, {}), "... and 3 more")
end)

case("client_answers_server_requests", function()
  local proc, sent = fake_proc()
  local client = Client.new(proc, ROOT)
  client:handle({ id = "token-1", method = "window/workDoneProgress/create" })
  eq(sent[1].id, "token-1", "the reply carries the request id")
  eq(sent[1].method, nil, "and is a response, not a request")
end)

case("client_finishes_the_handshake_on_the_initialize_response", function()
  local proc, sent = fake_proc()
  local client = Client.new(proc, ROOT)
  client:handle({ id = client.init_id, result = {} })
  eq(client.ready, true)
  eq(methods(sent), "initialized")
end)

local function ready_client(on_message)
  local dir = th.mktmpdir("lsp")
  local path = maki.fs.joinpath(dir, "lib.rs")
  maki.fs.write(path, "pub fn f() {}\n")
  local client
  local proc, sent = fake_proc(function(message)
    if on_message then
      on_message(client, message)
    end
  end)
  client = Client.new(proc, dir)
  client.ready = true
  client.quiescent = true
  return client, path, sent, dir
end

local function run_check_on_save(client, message)
  if message.method == "textDocument/didSave" then
    local token = "rust-analyzer/flycheck/0"
    client:handle({ method = "$/progress", params = { token = token, value = { kind = "begin" } } })
    client:handle({ method = "$/progress", params = { token = token, value = { kind = "end" } } })
  end
end

case("check_waits_for_the_cargo_check_its_save_started", function()
  local client, path, sent, dir = ready_client(run_check_on_save)
  eq(client:check(path, 100), true, "the check that followed the save settled")
  eq(client:check(path, 100), true)
  eq(methods(sent), "textDocument/didOpen,textDocument/didSave,textDocument/didChange,textDocument/didSave")
  eq(sent[3].params.textDocument.version, 2, "each change bumps the version")
  th.rmtree(dir)
end)

case("check_does_not_wait_while_the_workspace_loads", function()
  local client, path, sent, dir = ready_client()
  client.quiescent = false
  eq(client:check(path, 100), false)
  eq(methods(sent), "textDocument/didOpen,textDocument/didSave", "the server still hears about the edit")
  th.rmtree(dir)
end)

case("check_gives_up_when_no_check_starts", function()
  local client, path, _, dir = ready_client()
  eq(client:check(path, 2), false)
  th.rmtree(dir)
end)

th.report()
