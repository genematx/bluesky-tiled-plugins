# Search

Tiled provides [built-in search queries][] covering most common use cases:
equality, comparison, full text, and more.

The `bluesky_tiled_plugins.queries` modules adds some additional queries
specific to querying catalog of Bluesky Runs.

- {py:func}`bluesky_tiled_plugins.queries.PartialUID`
- {py:func}`bluesky_tiled_plugins.queries.ScanID`
- {py:class}`bluesky_tiled_plugins.queries.ScanIDRange`
- {py:class}`bluesky_tiled_plugins.queries.TimeRange`

For backward-compatibility with common legacy workflows, item lookup on a
Catalog of Bluesky Runs integrates these queries:

- `catalog[<positive integer>]` searches by scan ID.
- `catalog[<partial uid>]` searches by partial UID.

[built-in search queries]:
  https://blueskyproject.io/tiled/reference/queries.html
