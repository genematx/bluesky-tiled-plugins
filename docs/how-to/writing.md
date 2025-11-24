# Writing Bluesky Runs into Tiled

## Complete Example

A minimal simulated example of using `TiledWriter` in a Bluesky plan is shown
below.

```python
from bluesky import RunEngine
import bluesky.plans as bp
from bluesky_tiled_plugins import TiledWriter
from tiled.server import SimpleTiledServer
from tiled.client import from_uri
from ophyd.sim import det
from ophyd.sim import hw

# Initialize the Tiled server and client
save_path = "/path/to/save/detector_data"
tiled_server = SimpleTiledServer(readable_storage=[save_path])
tiled_client = from_uri(tiled_server.uri)

# Initialize the RunEngine and subscribe TiledWriter
RE = RunEngine()
tw = TiledWriter(tiled_client, batch_size=1)
RE.subscribe(tw)

# Run an experiment collecting internal data
(uid,) = RE(bp.count([det], 3))
data = tiled_client[uid]["primary/det"].read()

# Run an experiment collecting external data
(uid,) = RE(bp.count([hw(save_path=save_path).img], 2))
data = tiled_client[uid]["primary/img"].read()
```

## Details

### Run `SimpleTiledServer`

This starts a tiled server, running on a background thread. This way of running
the server is intended for "first steps" and embedded deployments.

```python
# Initialize the Tiled server and client
save_path = "/path/to/save/detector_data"
tiled_server = SimpleTiledServer(readable_storage=[save_path])
```

### Connect client

This connects to the server.

```python
tiled_client = from_uri(tiled_server.uri)
```

````{note}
If running the server in a separate process, such as via,

```sh
tiled serve catalog --temp
```

connect in the same way, e.g.

```python
tiled_client = from_uri("http://localhost:8000?api_key=...")
```
````

When used with detectors that write data directory to storage, it is necessary
to set the `readable_storage` parameter. This grants the server permission to
serve data at certain file paths(s).

### Subscribe

This configures the RunEngine to publish all Bluesky documents to the
TiledWriter callback.

```python
# Initialize the RunEngine and subscribe TiledWriter
RE = RunEngine()
tw = TiledWriter(tiled_client, batch_size=1)
RE.subscribe(tw)
```

By default `TiledWriter` caches documents into large batches before writing them
to Tiled. For "live" access to data, set `batch_size=1`.

### Acquire Data and Access It

```python
# Run an experiment collecting internal data
(uid,) = RE(bp.count([det], 3))
data = tiled_client[uid]["primary/det"].read()

# Run an experiment collecting external data
(uid,) = RE(bp.count([hw(save_path=save_path).img], 2))
data = tiled_client[uid]["primary/img"].read()
```
