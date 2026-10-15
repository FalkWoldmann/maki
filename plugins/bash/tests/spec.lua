local quiet_flags = require("quiet_flags")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

local CARGO = " -q"
local NEXTEST = " --status-level fail --final-status-level fail --hide-progress-bar"
local GIT_LOG = " --oneline"
local NPM = " --no-fund"

case("cargo_subcommands_get_quiet_flags", function()
  eq(quiet_flags.rewrite("cargo test"), "cargo test" .. CARGO)
  eq(quiet_flags.rewrite("cargo build --release"), "cargo build" .. CARGO .. " --release")
  eq(quiet_flags.rewrite("cargo nextest run -p maki-lua"), "cargo nextest run" .. NEXTEST .. " -p maki-lua")
end)

case("flags_go_before_the_double_dash", function()
  eq(
    quiet_flags.rewrite("cargo test --test basic -- --test-threads=1"),
    "cargo test" .. CARGO .. " --test basic -- --test-threads=1"
  )
end)

case("toolchain_and_env_prefixes_are_kept", function()
  eq(quiet_flags.rewrite("cargo +nightly test"), "cargo +nightly test" .. CARGO)
  eq(quiet_flags.rewrite("RUST_LOG=debug cargo test"), "RUST_LOG=debug cargo test" .. CARGO)
end)

case("explicit_verbosity_or_format_opts_out", function()
  eq(quiet_flags.rewrite("cargo test -q"), nil)
  eq(quiet_flags.rewrite("cargo test --quiet"), nil)
  eq(quiet_flags.rewrite("cargo test -vv"), nil)
  eq(quiet_flags.rewrite("cargo build --message-format=json"), nil)
  eq(quiet_flags.rewrite("cargo build --message-format short"), nil)
  eq(quiet_flags.rewrite("cargo nextest run --status-level all"), nil)
  eq(quiet_flags.rewrite("cargo nextest run -v"), nil)
  eq(quiet_flags.rewrite("git log --pretty=format:%h"), nil)
  eq(quiet_flags.rewrite("git log --date=iso -n 3"), nil)
  eq(quiet_flags.rewrite("pytest -v tests/"), nil)
end)

case("flags_that_change_what_the_output_is_for_opt_out", function()
  eq(quiet_flags.rewrite("cargo test --no-run"), nil)
  eq(quiet_flags.rewrite("cargo test -- --nocapture"), nil)
  eq(quiet_flags.rewrite("cargo test -- --show-output"), nil)
  eq(quiet_flags.rewrite("cargo nextest run --no-capture"), nil)
  eq(quiet_flags.rewrite("pip install --dry-run requests"), nil)
end)

case("output_that_does_not_reach_the_model_is_left_alone", function()
  eq(quiet_flags.rewrite("cargo test 2>&1 | grep FAILED"), nil)
  eq(quiet_flags.rewrite("cargo test > out.txt"), nil)
  eq(quiet_flags.rewrite("> out.txt cargo test"), nil)
  eq(quiet_flags.rewrite("cargo test > /dev/null 2>&1"), nil)
  eq(quiet_flags.rewrite("{ cargo build; cargo test; } > out.txt"), nil)
  eq(quiet_flags.rewrite("echo $(cargo test)"), nil)
  eq(quiet_flags.rewrite("x=$(cargo test); git log"), "x=$(cargo test); git log" .. GIT_LOG)
  eq(quiet_flags.rewrite("diff <(cargo test) expected"), nil)
end)

case("stderr_redirects_and_pipeline_tails_still_rewrite", function()
  eq(quiet_flags.rewrite("cargo test 2>&1"), "cargo test" .. CARGO .. " 2>&1")
  eq(quiet_flags.rewrite("cargo test 2>/dev/null"), "cargo test" .. CARGO .. " 2>/dev/null")
  eq(quiet_flags.rewrite("cargo test >out.txt; git log"), "cargo test >out.txt; git log" .. GIT_LOG)
  eq(quiet_flags.rewrite("echo x | cargo test"), "echo x | cargo test" .. CARGO)
end)

case("redirect_on_a_list_keeps_the_whole_list_as_written", function()
  eq(quiet_flags.rewrite("cargo build && cargo test > out.txt"), nil)
end)

case("every_command_in_a_chain_is_rewritten", function()
  eq(quiet_flags.rewrite("cargo build && cargo test"), "cargo build" .. CARGO .. " && cargo test" .. CARGO)
  eq(quiet_flags.rewrite("ls && cargo test; echo done"), "ls && cargo test" .. CARGO .. "; echo done")
  eq(quiet_flags.rewrite("if cargo test; then echo ok; fi"), "if cargo test" .. CARGO .. "; then echo ok; fi")
end)

case("single_flag_tools_get_their_flag", function()
  eq(quiet_flags.rewrite("git log -n 5"), "git log" .. GIT_LOG .. " -n 5")
  eq(quiet_flags.rewrite("pytest tests/test_a.py"), "pytest -q tests/test_a.py")
  eq(quiet_flags.rewrite("pip install -r requirements.txt"), "pip install -q -r requirements.txt")
  eq(quiet_flags.rewrite("npm ci"), "npm ci" .. NPM)
end)

case("unmatched_or_broken_commands_return_nil", function()
  eq(quiet_flags.rewrite("ls -la"), nil)
  eq(quiet_flags.rewrite("cargo fmt"), nil)
  eq(quiet_flags.rewrite("git status"), nil)
  eq(quiet_flags.rewrite("make check"), nil)
  eq(quiet_flags.rewrite("git -C sub log"), nil)
  eq(quiet_flags.rewrite("cargo test (("), nil)
  eq(quiet_flags.rewrite(""), nil)
end)

th.report()
