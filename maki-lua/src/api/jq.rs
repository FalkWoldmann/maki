use jaq_all::data::{self, Runner};
use jaq_all::fmts::Format;
use jaq_all::fmts::read::read;
use jaq_all::fmts::write::{Writer, write};
use jaq_all::jaq_core::unwrap_valr;
use jaq_all::load::FileReportsDisp;
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Table};

use super::util::pair::{Pair, err_pair, pair};

const DEFAULT_FORMAT: &str = "json";

/// The formats `from` and `to` accept.
pub(crate) const FORMATS: &str = Format::ALL;

pub(crate) struct JqOptions {
    pub from: Format,
    pub to: Format,
    pub slurp: bool,
}

pub(crate) fn parse_format(name: &str) -> Result<Format, String> {
    Format::parse(name)
        .ok_or_else(|| format!("unknown format {name:?}, expected one of: {FORMATS}"))
}

/// Runs `filter` over every value in `input` and writes each result on its
/// own line, compact, since the reader is usually a model paying per token. Compile errors carry jaq's own report, which points at the
/// offending part of the filter.
pub(crate) fn run_filter(filter: &str, input: &str, options: &JqOptions) -> Result<String, String> {
    let filter = data::compile(filter).map_err(|reports| {
        reports
            .iter()
            .map(|report| FileReportsDisp::new(report).to_string())
            .collect::<String>()
    })?;
    let runner = Runner {
        writer: Writer {
            format: options.to,
            ..Default::default()
        },
        ..Default::default()
    };
    let inputs = read(options.from, input.as_bytes(), input, options.slurp);
    let mut out = Vec::new();
    data::run(
        &runner,
        &filter,
        Default::default(),
        inputs,
        |e| e,
        |value| {
            let value = unwrap_valr(value).map_err(|e| e.to_string())?;
            write(&mut out, &runner.writer, &value).map_err(|e| e.to_string())
        },
    )?;
    String::from_utf8(out).map_err(|e| e.to_string())
}

fn options(opts: Option<Table>) -> LuaResult<Result<JqOptions, String>> {
    let from: Option<String> = opts.as_ref().map_or(Ok(None), |opts| opts.get("from"))?;
    let to: Option<String> = opts.as_ref().map_or(Ok(None), |opts| opts.get("to"))?;
    let slurp: Option<bool> = opts.as_ref().map_or(Ok(None), |opts| opts.get("slurp"))?;
    let parse = |name: Option<String>| parse_format(name.as_deref().unwrap_or(DEFAULT_FORMAT));
    Ok(parse(from).and_then(|from| {
        parse(to).map(|to| JqOptions {
            from,
            to,
            slurp: slurp.unwrap_or(false),
        })
    }))
}

/// Run a jq filter over structured text. Input and output can each be JSON,
/// YAML, TOML, XML, CBOR, CSV, TSV, or raw lines, so one filter can read a
/// `Cargo.toml` and answer in JSON. Every result goes on its own line.
///
/// @param filter string A jq filter, e.g. `.dependencies | keys`.
/// @param input string The text to filter.
/// @param opts table? `from` and `to` name the formats (default `"json"`); `slurp = true` collects all input values into one array.
/// @return (string?, string?) The filter's output, or nil plus an error.
/// @example
/// local out, err = maki.jq.run(".package.name", maki.fs.read("Cargo.toml"), { from = "toml" })
/// print(out) -- "maki"
#[lua_fn]
fn run(_lua: &Lua, filter: String, input: String, opts: Option<Table>) -> LuaResult<Pair<String>> {
    Ok(match options(opts)? {
        Ok(options) => pair(run_filter(&filter, &input, &options)),
        Err(e) => err_pair(e),
    })
}

lua_table! {
    /// jq filters over JSON, YAML, TOML, XML, CBOR, CSV, and TSV, run
    /// in-process with jaq.
    ///
    /// ```lua
    /// local names = maki.jq.run(".[].name", json_text)
    /// ```
    "maki.jq" => pub(crate) fn create_jq_table(), DOCS [
        run,
    ]
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    const CARGO_TOML: &str =
        "[package]\nname = \"maki\"\n\n[dependencies]\nserde = \"1\"\nsmol = \"2\"\n";

    fn opts(from: &str, to: &str) -> JqOptions {
        JqOptions {
            from: parse_format(from).unwrap(),
            to: parse_format(to).unwrap(),
            slurp: false,
        }
    }

    #[test_case(".package.name", "toml", "json", "\"maki\"\n" ; "toml_to_json")]
    #[test_case(".dependencies | keys", "toml", "json", "[\"serde\",\"smol\"]\n" ; "toml_keys")]
    #[test_case(".package.name", "toml", "raw", "maki\n" ; "raw_output")]
    fn filters_toml(filter: &str, from: &str, to: &str, expected: &str) {
        assert_eq!(
            run_filter(filter, CARGO_TOML, &opts(from, to)).unwrap(),
            expected
        );
    }

    #[test]
    fn filters_yaml_into_json() {
        let yaml = "jobs:\n  test:\n    runs-on: ubuntu\n  lint:\n    runs-on: macos\n";
        let out = run_filter(
            ".jobs | map_values(.\"runs-on\")",
            yaml,
            &opts("yaml", "json"),
        )
        .unwrap();
        assert_eq!(out, "{\"test\":\"ubuntu\",\"lint\":\"macos\"}\n");
    }

    #[test]
    fn every_input_value_is_filtered() {
        let out = run_filter(".n", "{\"n\":1}\n{\"n\":2}", &opts("json", "json")).unwrap();
        assert_eq!(out, "1\n2\n");
    }

    #[test]
    fn slurp_collects_inputs_into_one_array() {
        let options = JqOptions {
            slurp: true,
            ..opts("json", "json")
        };
        assert_eq!(run_filter("length", "1 2 3", &options).unwrap(), "3\n");
    }

    #[test]
    fn a_bad_filter_is_an_error_not_a_panic() {
        assert!(run_filter(".[", "{}", &opts("json", "json")).is_err());
    }

    #[test]
    fn bad_input_is_an_error() {
        assert!(run_filter(".", "{not json", &opts("json", "json")).is_err());
    }

    #[test]
    fn an_unknown_format_names_the_known_ones() {
        let err = parse_format("ini").err().unwrap();
        assert!(err.contains("toml"), "{err}");
    }
}
