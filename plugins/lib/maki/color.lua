--!strict
local M = {}

local function mix(from_hex: string?, to_hex: string?, t: number): number
  local a, b = tonumber(from_hex, 16) :: number, tonumber(to_hex, 16) :: number
  return math.floor(a + (b - a) * t + 0.5)
end

function M.lerp(from: string, to: string, t: number): string?
  local fr, fg, fb = from:match("^#(%x%x)(%x%x)(%x%x)$")
  local tr, tg, tb = to:match("^#(%x%x)(%x%x)(%x%x)$")
  if not fr or not tr then
    return nil
  end
  return string.format("#%02x%02x%02x", mix(fr, tr, t), mix(fg, tg, t), mix(fb, tb, t))
end

function M.dim(color: string, factor: number): string?
  local background = maki.ui.theme_style("background")
  local bg = background and background.bg
  return bg and M.lerp(color, bg, factor) or nil
end

return M
