# TraumaRaptor Changelog

Changes from upstream [Velociraptor](https://github.com/Velocidex/velociraptor) v0.76.1.

## 2026-03-28

### Bugfix: `notebook_default_new_cell_rows` not applied to non-event notebook cells

The `notebook_default_new_cell_rows` config option only applied to event-type artifact notebooks (client events, server events). For all other artifact types, `LIMIT 50` was hard-coded, ignoring the config.

**Fix:**
- `services/notebook/initial.go` — The `default` case in notebook cell template generation now uses the configured `default_limit` instead of hard-coded `LIMIT 50`, matching the event-type case.
