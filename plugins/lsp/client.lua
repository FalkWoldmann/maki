-- One language server process: the handshake, the documents we told it
-- about, and what it has published since.

local Rpc = require("rpc")

local Client = {}
Client.__index = Client

local POLL_MS = 50
-- rust-analyzer restarts `cargo check` when another save lands within its
-- debounce (about 100ms), so a run only counts as final once nothing new
-- started for this long.
local SETTLE_POLLS = 4
-- A check starts about 100ms after the save. Past this, none is coming
-- (`checkOnSave` is off, or the server does not know the file).
local START_POLLS = 40
local FLYCHECK_TOKEN = "rust-analyzer/flycheck/"
local LANGUAGE_ID = "rust"

function Client.new(proc, root)
  return setmetatable({
    proc = proc,
    root = root,
    init_id = 1,
    ready = false,
    quiescent = false,
    dead = false,
    versions = {},
    diagnostics = {},
    check_runs = 0,
    checks_running = {},
  }, Client)
end

function Client.start(argv, root)
  local proc, err = maki.fn.spawn(argv, { cwd = root })
  if not proc then
    return nil, err
  end
  local self = Client.new(proc, root)
  maki.async.spawn(function()
    self:read_loop()
  end)
  return self
end

function Client:initialize()
  self:send({
    jsonrpc = "2.0",
    id = self.init_id,
    method = "initialize",
    params = {
      rootUri = Rpc.uri(self.root),
      capabilities = {
        window = { workDoneProgress = true },
        experimental = { serverStatusNotification = true },
      },
    },
  })
end

function Client:send(message)
  if self.dead then
    return
  end
  local ok, err = self.proc:write(Rpc.encode(message))
  if not ok then
    self:stop(err)
  end
end

function Client:notify(method, params)
  self:send({ jsonrpc = "2.0", method = method, params = params })
end

function Client:stop(reason)
  if not self.dead then
    self.dead = true
    self.proc:kill()
    maki.log.warn(`rust-analyzer for {self.root} stopped: {reason}`)
  end
end

function Client:read_loop()
  local decode = Rpc.decoder()
  while not self.dead do
    local chunk, read_err = self.proc:read()
    if not chunk then
      return self:stop(read_err or "exited")
    end
    local bodies, frame_err = decode(chunk)
    for _, body in bodies do
      local message = maki.json.decode(body)
      if message then
        self:handle(message)
      end
    end
    if frame_err then
      return self:stop(frame_err)
    end
  end
end

function Client:handle(message)
  local method, params = message.method, message.params
  if not method then
    if message.id == self.init_id then
      if message.error then
        return self:stop(message.error.message)
      end
      self:notify("initialized", {})
      self.ready = true
    end
  elseif message.id ~= nil then
    -- Server requests (progress tokens, capability registration) only need
    -- an answer, and an empty result accepts them.
    self:send({ jsonrpc = "2.0", id = message.id })
  elseif method == "textDocument/publishDiagnostics" then
    self.diagnostics[Rpc.path(params.uri)] = params.diagnostics
  elseif method == "experimental/serverStatus" then
    self.quiescent = params.quiescent
  elseif method == "$/progress" then
    local token = tostring(params.token)
    if token:sub(1, #FLYCHECK_TOKEN) == FLYCHECK_TOKEN then
      if params.value.kind == "begin" then
        self.check_runs += 1
        self.checks_running[token] = true
      elseif params.value.kind == "end" then
        self.checks_running[token] = nil
      end
    end
  end
end

function Client:sync(path)
  local text = maki.fs.read(path)
  if not text then
    return
  end
  local uri = Rpc.uri(path)
  local version = (self.versions[path] or 0) + 1
  self.versions[path] = version
  if version == 1 then
    self:notify("textDocument/didOpen", {
      textDocument = { uri = uri, languageId = LANGUAGE_ID, version = version, text = text },
    })
  else
    self:notify("textDocument/didChange", {
      textDocument = { uri = uri, version = version },
      contentChanges = { { text = text } },
    })
  end
  self:notify("textDocument/didSave", { textDocument = { uri = uri } })
end

-- Tells the server {path} changed and waits for the `cargo check` that
-- follows. True once that check settled; false while the server is still
-- loading the workspace, when no check started, or after {max_polls}.
function Client:check(path, max_polls)
  if not self.ready then
    return false
  end
  local run_before = self.check_runs
  self:sync(path)
  if not self.quiescent then
    return false
  end
  local polls, settled = 0, 0
  while settled < SETTLE_POLLS do
    if self.dead or polls >= max_polls then
      return false
    end
    maki.async.sleep(POLL_MS)
    polls += 1
    local started = self.check_runs > run_before
    if not started and polls >= START_POLLS then
      return false
    end
    settled = (started and next(self.checks_running) == nil) and settled + 1 or 0
  end
  return true
end

Client.POLL_MS = POLL_MS

return Client
