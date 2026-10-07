-- After a write to a Rust file, waits for rust-analyzer's `cargo check` and
-- appends the errors to what the model reads, so it fixes them without a
-- round trip through `bash`.

local Client = require("client")
local Report = require("report")

local RUST_FILE = "%.rs$"
local MANIFEST = "Cargo.toml"
local REPO_MARKER = ".git"

local opts = maki.api.register_options({
  command = { default = "rust-analyzer", desc = "Language server executable, looked up on `PATH`." },
  wait_ms = {
    default = 15000,
    min = 0,
    desc = "How long an edit waits for `cargo check` before returning without diagnostics.",
  },
})

local servers = {}
local unavailable = {}

-- The outermost `Cargo.toml` inside the repository, so every crate of a
-- workspace shares one server.
local function workspace_root(path)
  local root
  for _, dir in maki.fs.parents(path) do
    if maki.fs.metadata(maki.fs.joinpath(dir, MANIFEST)) then
      root = dir
    end
    if maki.fs.metadata(maki.fs.joinpath(dir, REPO_MARKER)) then
      break
    end
  end
  return root
end

local function server_for(root)
  local server = servers[root]
  if server and not server.dead then
    return server
  end
  if unavailable[root] then
    return nil
  end
  local started, err = Client.start({ opts.command }, root)
  if not started then
    unavailable[root] = true
    maki.log.warn(`cannot start {opts.command} for {root}: {err}`)
    return nil
  end
  -- Registered before the handshake yields, so a concurrent edit finds this
  -- server instead of starting a second one.
  servers[root] = started
  started:initialize()
  return started
end

local function diagnose(prev, path)
  local note = prev(path)
  if not path:match(RUST_FILE) then
    return note
  end
  local root = workspace_root(path)
  local server = root and server_for(root)
  if not server then
    return note
  end
  local baseline = Report.snapshot(server.diagnostics, root, path)
  if not server:check(path, opts.wait_ms // Client.POLL_MS) then
    return note
  end
  local report = Report.build(server.diagnostics, root, path, baseline)
  return note and `{note}\n{report}` or report
end

maki.api.set_slot("edit.feedback", diagnose)
maki.api.set_slot("write.feedback", diagnose)

-- Indexing takes a while, so the first turn starts the server for the
-- project maki runs in, and it indexes while the model reads its way to the
-- first edit. Loading the plugin alone (tests, docgen) starts nothing.
maki.api.create_autocmd("TurnStart", {
  once = true,
  callback = function()
    local root = workspace_root(maki.fs.joinpath(maki.uv.cwd(), MANIFEST))
    if root then
      server_for(root)
    end
  end,
})
