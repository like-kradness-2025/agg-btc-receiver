# Spec: Dashboard Design Refresh (2026-07-07)

## Goal
Refresh the agg-btc-receiver web dashboard design for better visual appeal, readability, and modern aesthetics while keeping the mobile-first approach.

## Current State
- Port 3847, Tailscale Funnel HTTPS enabled
- Dark theme, 480px max-width, mobile-first
- Shows: connection status, trade/depth rates, market ranking, chart, system info
- HTML embedded in dashboard.mjs as template literal

## Design Refresh Goals
1. **Visual polish**: Glassmorphism cards, subtle gradients, better spacing
2. **Larger text**: Increase base font size, better hierarchy
3. **Color refinement**: More nuanced palette, better contrast
4. **Status indicators**: Animated pulse effects, better visual feedback
5. **Market cards**: Card-based layout instead of flat list
6. **System info**: Cleaner presentation with icons
7. **Chart area**: Better framing, loading states
8. **Overall feel**: Professional monitoring dashboard, not just a status page

## Constraints
- Single file (dashboard.mjs) - no external assets
- Must stay mobile-first (480px max-width)
- No external CSS/JS dependencies
- Dark theme mandatory
- Keep all existing functionality intact
- Japanese labels

## Success Criteria
- Visually distinct from current version
- All existing data displayed correctly
- Mobile-friendly
- No functional regression
