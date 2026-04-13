"""FastAPI router that serves the embedding UI plugin's static assets.

Registered via Tiled's ``routers:`` config and served at ``/custom/embedding-ui/``.
"""

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, HTMLResponse

router = APIRouter(prefix="/embedding-ui", tags=["embedding-ui"])

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@router.get("/embedding-view.js")
async def embedding_view_js():
    return FileResponse(
        STATIC_DIR / "embedding-view.js",
        media_type="application/javascript",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/ws-test")
async def ws_test_page():
    """Minimal page to debug WebSocket connections to projections stream."""
    return HTMLResponse("""<!DOCTYPE html>
<html><body>
<h3>WS Debug</h3>
<pre id="log" style="background:#eee;padding:12px;max-height:80vh;overflow:auto"></pre>
<script>
const log = document.getElementById('log');
function L(msg) { log.textContent += new Date().toISOString().slice(11,23) + ' ' + msg + '\\n'; }

const path = 'test_embed/embeddings/projections';
const wsUrl = (location.protocol==='https:'?'wss:':'ws:') + '//' + location.host
  + '/api/v1/stream/single/' + path + '?envelope_format=json';

L('Connecting to: ' + wsUrl);
const ws = new WebSocket(wsUrl);
ws.onopen = () => L('OPEN');
ws.onclose = (e) => L('CLOSE code=' + e.code + ' reason=' + e.reason);
ws.onerror = (e) => L('ERROR ' + JSON.stringify(e));
ws.onmessage = (e) => {
  const d = JSON.parse(e.data);
  L('MSG type=' + d.type + ' offset=' + JSON.stringify(d.offset) + ' shape=' + JSON.stringify(d.shape)
    + ' payload=' + (d.payload ? JSON.stringify(d.payload).slice(0,200) : 'none'));
};
</script>
</body></html>""")
