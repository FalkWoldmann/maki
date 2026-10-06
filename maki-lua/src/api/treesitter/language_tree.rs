use std::sync::Arc;

use crate::language::Language;
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{Function, Lua, Result as LuaResult, Table, UserDataRefMut, Value as LuaValue};
use tree_sitter::{Parser, Tree};

use super::tree::LuaTree;

fn parse_source(lang: Language, source: &str) -> LuaResult<Tree> {
    let mut parser = Parser::new();
    parser
        .set_language(&lang.ts_language())
        .map_err(|e| mlua::Error::runtime(format!("failed to set language: {e}")))?;
    parser
        .parse(source.as_bytes(), None)
        .ok_or_else(|| mlua::Error::runtime("parse returned no tree"))
}

pub(crate) struct LuaLanguageTree {
    tree: Option<Arc<Tree>>,
    source: Arc<str>,
    lang_name: Arc<str>,
    lang: Language,
}

impl LuaLanguageTree {
    pub(crate) fn new(source: Arc<str>, lang_name: Arc<str>, lang: Language) -> Self {
        Self {
            tree: None,
            source,
            lang_name,
            lang,
        }
    }

    fn ensure_parsed(&mut self) -> Result<Arc<Tree>, mlua::Error> {
        if let Some(ref tree) = self.tree {
            return Ok(Arc::clone(tree));
        }
        let tree = Arc::new(parse_source(self.lang, &self.source)?);
        self.tree = Some(Arc::clone(&tree));
        Ok(tree)
    }
}

/// Parses the source and returns a table containing the resulting Tree.
/// The tree is cached, so calling this again is cheap.
///
/// Pass {on_parse} for big sources: the parse then runs off the Lua thread,
/// the call yields until it is done, calls `on_parse(nil, trees)` and returns
/// the trees.
///
/// @param range table Unused. Accepted for API compatibility.
/// @param on_parse function? Callback `function(err, trees)`. When given, the parse runs off the Lua thread.
/// @return (table) Array with one Tree element.
/// @example
/// local trees = parser:parse()
/// local root = trees[1]:root()
#[lua_fn]
async fn parse(
    lua: Lua,
    mut this: UserDataRefMut<LuaLanguageTree>,
    range: Option<LuaValue>,
    on_parse: Option<Function>,
) -> LuaResult<Table> {
    let _ = range;
    if this.tree.is_none() && on_parse.is_some() {
        let (lang, source) = (this.lang, Arc::clone(&this.source));
        let tree = smol::unblock(move || parse_source(lang, &source)).await?;
        this.tree = Some(Arc::new(tree));
    }
    let tree = this.ensure_parsed()?;
    drop(this);
    let result = lua.create_table()?;
    result.raw_set(
        1,
        LuaTree {
            inner: Arc::clone(&tree),
        },
    )?;
    if let Some(on_parse) = on_parse {
        on_parse.call_async::<()>((LuaValue::Nil, &result)).await?;
    }
    Ok(result)
}

/// Returns the language name this parser was created with.
///
/// @return (string) Language name, e.g. `"lua"`.
#[lua_fn]
fn lang(_lua: &Lua, this: &LuaLanguageTree) -> LuaResult<String> {
    Ok(this.lang_name.to_string())
}

/// Returns child LanguageTrees for injected languages.
/// Not yet implemented, always returns an empty table.
///
/// @return (table) Empty table.
#[lua_fn]
fn children(lua: &Lua, this: &LuaLanguageTree) -> LuaResult<Table> {
    let _ = this;
    lua.create_table()
}

/// Returns all parsed trees as a table (at most one for now).
/// Returns an empty table if `parse()` has not been called yet.
///
/// @return (table) Array of Tree.
#[lua_fn]
fn trees(lua: &Lua, this: &LuaLanguageTree) -> LuaResult<Table> {
    let result = lua.create_table()?;
    if let Some(ref tree) = this.tree {
        result.raw_set(
            1,
            LuaTree {
                inner: Arc::clone(tree),
            },
        )?;
    }
    Ok(result)
}

/// Returns the source string this parser was created with.
///
/// @return (string) The original source text.
#[lua_fn]
fn source(_lua: &Lua, this: &LuaLanguageTree) -> LuaResult<String> {
    Ok(this.source.to_string())
}

/// Checks whether the parse tree is still valid.
/// Not yet implemented, always returns true.
///
/// @param exclude_children boolean Unused.
/// @param range table Unused.
/// @return (boolean) Always true.
#[lua_fn]
fn is_valid(
    _lua: &Lua,
    this: &LuaLanguageTree,
    exclude_children: Option<LuaValue>,
    range: Option<LuaValue>,
) -> LuaResult<bool> {
    let _ = (this, exclude_children, range);
    Ok(true)
}

/// Calls {fn} with `(tree, nil)` for the parsed tree.
/// Triggers a parse if the tree has not been parsed yet.
///
/// @param fn function Callback receiving `(Tree, nil)`.
/// @return
/// @example
/// parser:for_each_tree(function(tree, _)
///   print(tree:root():type())
/// end)
#[lua_fn]
fn for_each_tree(_lua: &Lua, this: &mut LuaLanguageTree, r#fn: Function) -> LuaResult<()> {
    let tree = this.ensure_parsed()?;
    r#fn.call::<()>((LuaTree { inner: tree }, LuaValue::Nil))?;
    Ok(())
}

/// Returns the regions this parser covers.
/// Not yet implemented, always returns a table with one empty region.
///
/// @return (table) Array with one empty table.
#[lua_fn]
fn included_regions(lua: &Lua, this: &LuaLanguageTree) -> LuaResult<Table> {
    let _ = this;
    let result = lua.create_table()?;
    result.raw_set(1, lua.create_table()?)?;
    Ok(result)
}

/// Checks whether this parser covers the given {range}.
/// Not yet implemented, always returns true.
///
/// @param range table Range to check (currently unused).
/// @return (boolean) Always true.
#[lua_fn]
fn contains(_lua: &Lua, this: &LuaLanguageTree, range: LuaValue) -> LuaResult<bool> {
    let _ = (this, range);
    Ok(true)
}

/// Drops the cached parse tree and frees its memory.
/// After calling this, the next `parse()` will re-parse from scratch.
///
/// @return
#[lua_fn]
fn destroy(_lua: &Lua, this: &mut LuaLanguageTree) -> LuaResult<()> {
    this.tree = None;
    Ok(())
}

lua_class! {
    /// Manages parsing of a source string for a single language.
    ///
    /// Obtained from `maki.treesitter.get_parser()` or `maki.treesitter.get_string_parser()`.
    /// Call `:parse()` to get the syntax tree, then use `:root()` on the tree to start walking nodes.
    ///
    /// ```lua
    /// local parser, err = maki.treesitter.get_parser(source, "lua")
    /// if not err then
    ///   local trees = parser:parse()
    ///   local root = trees[1]:root()
    /// end
    /// ```
    "maki.treesitter.LanguageTree" => LuaLanguageTree, DOCS [parse, lang, children, trees, source, is_valid, for_each_tree, included_regions, contains, destroy]
}
