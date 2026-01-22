# Replay Bluesky documents

The bluesky-tiled-plugings package provides a Tiled exporter that produces
Bluesky documents, encoded as [newline-delimited JSON][].

This supports the `run.documents()` method in the Python client.

To use it, include the following in the Tiled server configuration.

```yaml
media_types:
  BlueskyRun:
    application/json-seq: bluesky_tiled_plugins.exporters:json_seq_exporter
```

[newline-delimited JSON]: https://github.com/ndjson/ndjson-spec
