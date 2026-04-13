import React from "react";

interface EmbeddingPoint {
  x: number;
  y: number;
  index: number;
  label?: string;
  path?: string;
}

interface TooltipInfo {
  point: EmbeddingPoint;
  screenX: number;
  screenY: number;
  thumbnailUrl: string;
}

interface SelectedPointInfo {
  point: EmbeddingPoint;
  thumbnailUrl: string;
  note: string;
  userLabel: string;
  originalNote: string;
  originalUserLabel: string;
  saving: boolean;
}

interface ViewState {
  offsetX: number;
  offsetY: number;
  scale: number;
}

type WsStatus = "disconnected" | "connecting" | "connected";

const POINT_RADIUS = 4;
const HOVER_RADIUS = 8;
const NOTES_MAX_LEN = 1024;
const USER_LABEL_MAX_LEN = 64;
const LABEL_COLORS: Record<string, string> = {};
const COLOR_PALETTE = [
  "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
  "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
  "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
];
let colorIdx = 0;

function getLabelColor(label: string): string {
  if (!label || label === "") return "#888888";
  if (!LABEL_COLORS[label]) {
    LABEL_COLORS[label] = COLOR_PALETTE[colorIdx % COLOR_PALETTE.length];
    colorIdx++;
  }
  return LABEL_COLORS[label];
}

function encodeUtf32LE(str: string, maxLen: number): ArrayBuffer {
  const buf = new ArrayBuffer(maxLen * 4);
  const view = new Uint32Array(buf);
  for (let i = 0; i < Math.min(str.length, maxLen); i++) {
    view[i] = str.codePointAt(i) || 0;
  }
  return buf;
}

async function patchStringArray(
  apiUrl: string,
  nodePath: string,
  arrayName: string,
  index: number,
  value: string,
  maxLen: number,
  apiKey?: string,
): Promise<Response> {
  const body = encodeUtf32LE(value, maxLen);
  const headers: Record<string, string> = {
    "Content-Type": "application/octet-stream",
  };
  if (apiKey) headers["Authorization"] = `Apikey ${apiKey}`;
  return fetch(
    `${apiUrl}/array/full/${nodePath}/${arrayName}` +
      `?offset=${index}&shape=1`,
    { method: "PATCH", headers, body },
  );
}

async function fetchStringValue(
  apiUrl: string,
  nodePath: string,
  arrayName: string,
  index: number,
): Promise<string> {
  const res = await fetch(
    `${apiUrl}/array/full/${nodePath}/${arrayName}` +
      `?format=application/json&slice=${index}`,
  );
  if (!res.ok) return "";
  const data = await res.json();
  return typeof data === "string" ? data : String(data ?? "");
}

function fitViewToPoints(
  pts: EmbeddingPoint[],
  width: number,
  height: number,
): ViewState {
  if (pts.length === 0) return { offsetX: 0, offsetY: 0, scale: 1 };
  const xs = pts.map((p) => p.x);
  const ys = pts.map((p) => p.y);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const rangeX = maxX - minX || 1;
  const rangeY = maxY - minY || 1;
  const padding = 0.1;
  const scaleX = width / (rangeX * (1 + 2 * padding));
  const scaleY = height / (rangeY * (1 + 2 * padding));
  const scale = Math.min(scaleX, scaleY);
  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  return {
    offsetX: width / 2 - cx * scale,
    offsetY: height / 2 + cy * scale,
    scale,
  };
}

const STATUS_COLORS: Record<WsStatus, string> = {
  disconnected: "#999",
  connecting: "#f0ad4e",
  connected: "#5cb85c",
};

function EmbeddingScatter({
  segments,
  item,
}: {
  segments: string[];
  item: any;
}) {
  const canvasRef = React.useRef<HTMLCanvasElement>(null);
  const containerRef = React.useRef<HTMLDivElement>(null);
  const [points, setPoints] = React.useState<EmbeddingPoint[]>([]);
  const [tooltip, setTooltip] = React.useState<TooltipInfo | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [view, setView] = React.useState<ViewState>({
    offsetX: 0,
    offsetY: 0,
    scale: 1,
  });
  const [wsStatus, setWsStatus] = React.useState<WsStatus>("disconnected");
  const [liveEnabled, setLiveEnabled] = React.useState(true);
  const [selected, setSelected] = React.useState<SelectedPointInfo | null>(
    null,
  );
  const [apiKey, setApiKey] = React.useState("");
  const [saveError, setSaveError] = React.useState<string | null>(null);
  const dragRef = React.useRef<{
    startX: number;
    startY: number;
    startOffsetX: number;
    startOffsetY: number;
  } | null>(null);
  const [containerWidth, setContainerWidth] = React.useState(800);
  const CANVAS_HEIGHT = 500;
  const PANEL_WIDTH = 280;
  const PANEL_GAP = 8;
  // Track how many points we've loaded so far for incremental fetching
  const pointCountRef = React.useRef(0);
  // Track whether initial view fit has been applied
  const initialFitDone = React.useRef(false);
  // Skip the catch-up refreshAll on the first live-effect run after initial load
  const skipCatchupRef = React.useRef(false);

  const apiUrl = React.useMemo(() => {
    return `${window.location.origin}/api/v1`;
  }, []);

  const nodePath = segments.join("/");

  const canvasWidth = selected
    ? containerWidth - Math.round(PANEL_WIDTH / 4)
    : containerWidth;

  // Fetch all current data (projections + index) and merge into state
  const refreshAll = React.useCallback(async () => {
    try {
      const [projRes, indexRes] = await Promise.all([
        fetch(
          `${apiUrl}/array/full/${nodePath}/projections?format=application/json`,
        ),
        fetch(
          `${apiUrl}/table/full/${nodePath}/_index?format=application/json`,
        ),
      ]);
      if (!projRes.ok || !indexRes.ok) return;

      const projData: number[][] = await projRes.json();
      const indexData = await indexRes.json();
      if (projData.length === 0) return;

      const labels: string[] = indexData.label || [];
      const paths: string[] = indexData.path || [];

      const pts: EmbeddingPoint[] = projData.map(
        (coords: number[], i: number) => ({
          x: coords[0],
          y: coords[1],
          index: i,
          label: labels[i] || "",
          path: paths[i] || "",
        }),
      );
      pointCountRef.current = pts.length;
      setPoints(pts);
      return pts;
    } catch {
      return undefined;
    }
  }, [apiUrl, nodePath]);

  // Initial data load
  const canvasWidthRef = React.useRef(canvasWidth);
  canvasWidthRef.current = canvasWidth;
  React.useEffect(() => {
    let cancelled = false;

    async function fetchData() {
      try {
        setLoading(true);
        const pts = await refreshAll();
        if (cancelled) return;
        setError(null);
        if (pts && pts.length > 0) {
          const fitted = fitViewToPoints(pts, canvasWidthRef.current, CANVAS_HEIGHT);
          setView(fitted);
          initialFitDone.current = true;
        }
      } catch (err: any) {
        if (!cancelled) setError(err.message);
      } finally {
        if (!cancelled) {
          skipCatchupRef.current = true;
          setLoading(false);
        }
      }
    }

    fetchData();
    return () => {
      cancelled = true;
    };
  }, [refreshAll]);

  // Live updates via dual WebSocket subscriptions.
  //
  // We subscribe to both the projections array and the _index table.
  // The write order on the server is: arrays first, _index table last.
  // So the projection event (with x/y coords) typically arrives before the
  // table event (with label/path). Points render immediately from the
  // projection payload; when the table event arrives it fills in metadata.
  //
  // A pendingMeta buffer handles the rare case where the table event
  // arrives before the projection event: metadata is buffered and applied
  // once the projection event creates the points.
  //
  // On (re-)connect and on live re-enable we do a full refreshAll() to
  // catch any updates that arrived while disconnected or paused.
  React.useEffect(() => {
    if (loading || !liveEnabled) return;

    let projWs: WebSocket | null = null;
    let indexWs: WebSocket | null = null;
    let projReconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let indexReconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let disposed = false;
    let projSchemaReceived = false;
    let indexSchemaReceived = false;
    let projConnected = false;
    let indexConnected = false;

    // Buffer for table metadata that arrived before projections.
    // Keyed by startIndex → {labels, paths}.
    const pendingMeta: Map<
      number,
      { labels: string[]; paths: string[] }
    > = new Map();

    function updateStatus() {
      if (projConnected && indexConnected) setWsStatus("connected");
      else if (projConnected || indexConnected) setWsStatus("connecting");
      else setWsStatus("disconnected");
    }

    // Catch up on anything missed while live was off or WS was disconnected.
    // Skip on the first run right after initial load (data is already fresh).
    if (skipCatchupRef.current) {
      skipCatchupRef.current = false;
    } else {
      refreshAll();
    }

    function applyMeta(startIdx: number, count: number) {
      const meta = pendingMeta.get(startIdx);
      if (!meta) return;
      if (meta.labels.length !== count) return;
      pendingMeta.delete(startIdx);
      setPoints((prev) =>
        prev.map((p) => {
          const rel = p.index - startIdx;
          if (rel >= 0 && rel < count) {
            return {
              ...p,
              label: meta.labels[rel] || p.label,
              path: meta.paths[rel] || p.path,
            };
          }
          return p;
        }),
      );
    }

    function handleProjectionEvent(msg: any) {
      if (msg.type !== "array-data" && msg.type !== "array-ref") return;
      const offset = msg.offset;
      const payload = msg.payload;
      if (!offset || !Array.isArray(offset) || offset.length === 0) {
        refreshAll();
        return;
      }
      const startIdx = offset[0];
      if (!payload || !Array.isArray(payload) || payload.length === 0) {
        refreshAll();
        return;
      }
      const newPts: EmbeddingPoint[] = payload.map(
        (coords: number[], i: number) => ({
          x: coords[0],
          y: coords[1],
          index: startIdx + i,
          label: "",
          path: "",
        }),
      );
      pointCountRef.current = startIdx + payload.length;
      setPoints((prev) => prev.slice(0, startIdx).concat(newPts));

      // Check if metadata already arrived for this range
      applyMeta(startIdx, payload.length);
    }

    function handleTableEvent(msg: any) {
      if (msg.type !== "table-data") return;
      if (!msg.append) {
        refreshAll();
        return;
      }
      const payload = msg.payload;
      if (!payload) return;
      const labels: string[] = payload.label || [];
      const paths: string[] = payload.path || [];
      const count = labels.length;
      if (count === 0) return;

      // Determine which indices these rows correspond to.
      // If projections already arrived, pointCountRef is updated and the
      // points exist — apply metadata directly. Otherwise buffer it.
      const startIdx = pointCountRef.current - count;
      if (startIdx >= 0) {
        setPoints((prev) => {
          // Verify the points at this range exist
          if (prev.length >= startIdx + count) {
            return prev.map((p) => {
              const rel = p.index - startIdx;
              if (rel >= 0 && rel < count) {
                return {
                  ...p,
                  label: labels[rel] || p.label,
                  path: paths[rel] || p.path,
                };
              }
              return p;
            });
          }
          // Points don't exist yet; buffer metadata
          pendingMeta.set(pointCountRef.current, { labels, paths });
          return prev;
        });
      } else {
        // Projection event hasn't arrived yet; buffer metadata
        pendingMeta.set(pointCountRef.current, { labels, paths });
      }
    }

    function connectProjections() {
      if (disposed) return;
      projSchemaReceived = false;
      const wsScheme =
        window.location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl =
        `${wsScheme}//${window.location.host}/api/v1/stream/single/${nodePath}/projections` +
        `?envelope_format=json`;
      projWs = new WebSocket(wsUrl);

      projWs.onopen = () => {
        if (disposed) { projWs?.close(); return; }
        projConnected = true;
        updateStatus();
      };
      projWs.onmessage = (event) => {
        if (disposed) return;
        try {
          const msg = JSON.parse(event.data);
          if (!projSchemaReceived) {
            if (msg.type && msg.type.endsWith("-schema")) {
              projSchemaReceived = true;
              return;
            }
          }
          handleProjectionEvent(msg);
        } catch { /* ignore parse errors */ }
      };
      projWs.onclose = () => {
        projConnected = false;
        updateStatus();
        if (!disposed) projReconnectTimer = setTimeout(connectProjections, 3000);
      };
      projWs.onerror = () => {};
    }

    function connectIndex() {
      if (disposed) return;
      indexSchemaReceived = false;
      const wsScheme =
        window.location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl =
        `${wsScheme}//${window.location.host}/api/v1/stream/single/${nodePath}/_index` +
        `?envelope_format=json`;
      indexWs = new WebSocket(wsUrl);

      indexWs.onopen = () => {
        if (disposed) { indexWs?.close(); return; }
        indexConnected = true;
        updateStatus();
      };
      indexWs.onmessage = (event) => {
        if (disposed) return;
        try {
          const msg = JSON.parse(event.data);
          if (!indexSchemaReceived) {
            if (msg.type && msg.type.endsWith("-schema")) {
              indexSchemaReceived = true;
              return;
            }
          }
          handleTableEvent(msg);
        } catch { /* ignore parse errors */ }
      };
      indexWs.onclose = () => {
        indexConnected = false;
        updateStatus();
        if (!disposed) indexReconnectTimer = setTimeout(connectIndex, 3000);
      };
      indexWs.onerror = () => {};
    }

    connectProjections();
    connectIndex();

    return () => {
      disposed = true;
      if (projReconnectTimer) clearTimeout(projReconnectTimer);
      if (indexReconnectTimer) clearTimeout(indexReconnectTimer);
      if (projWs) { projWs.onclose = null; projWs.close(); }
      if (indexWs) { indexWs.onclose = null; indexWs.close(); }
      setWsStatus("disconnected");
    };
  }, [loading, liveEnabled, nodePath, apiUrl, refreshAll]);

  // Resize observer — tracks container width
  const selectedRef = React.useRef(selected);
  selectedRef.current = selected;
  React.useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width } = entry.contentRect;
        if (width > 0) setContainerWidth(Math.floor(width));
      }
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  // Draw canvas
  React.useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = canvasWidth * dpr;
    canvas.height = CANVAS_HEIGHT * dpr;
    ctx.scale(dpr, dpr);

    ctx.clearRect(0, 0, canvasWidth, CANVAS_HEIGHT);

    ctx.fillStyle = "#fafafa";
    ctx.fillRect(0, 0, canvasWidth, CANVAS_HEIGHT);

    // Grid
    ctx.strokeStyle = "#e0e0e0";
    ctx.lineWidth = 1;
    const gridStep = 50 * view.scale;
    if (gridStep > 10) {
      const startX = view.offsetX % gridStep;
      for (let x = startX; x < canvasWidth; x += gridStep) {
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, CANVAS_HEIGHT);
        ctx.stroke();
      }
      const startY = view.offsetY % gridStep;
      for (let y = startY; y < CANVAS_HEIGHT; y += gridStep) {
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(canvasWidth, y);
        ctx.stroke();
      }
    }

    // Points
    for (const p of points) {
      const sx = p.x * view.scale + view.offsetX;
      const sy = -p.y * view.scale + view.offsetY;
      if (
        sx < -10 ||
        sx > canvasWidth + 10 ||
        sy < -10 ||
        sy > CANVAS_HEIGHT + 10
      )
        continue;
      const isHovered = tooltip?.point.index === p.index;
      const isSelected = selected?.point.index === p.index;
      const radius = isHovered || isSelected ? HOVER_RADIUS : POINT_RADIUS;
      ctx.beginPath();
      ctx.arc(sx, sy, radius, 0, Math.PI * 2);
      ctx.fillStyle = getLabelColor(p.label || "");
      ctx.globalAlpha = isHovered || isSelected ? 1.0 : 0.7;
      ctx.fill();
      if (isSelected) {
        ctx.strokeStyle = "#1976d2";
        ctx.lineWidth = 2.5;
        ctx.stroke();
      } else if (isHovered) {
        ctx.strokeStyle = "#000";
        ctx.lineWidth = 2;
        ctx.stroke();
      }
    }
    ctx.globalAlpha = 1.0;
  }, [points, view, canvasWidth, tooltip, selected]);

  // Mouse handlers
  const toDataCoords = React.useCallback(
    (clientX: number, clientY: number) => {
      const canvas = canvasRef.current;
      if (!canvas) return null;
      const rect = canvas.getBoundingClientRect();
      return { mx: clientX - rect.left, my: clientY - rect.top };
    },
    [],
  );

  const findPoint = React.useCallback(
    (mx: number, my: number): EmbeddingPoint | null => {
      let closest: EmbeddingPoint | null = null;
      let minDist = Infinity;
      for (const p of points) {
        const sx = p.x * view.scale + view.offsetX;
        const sy = -p.y * view.scale + view.offsetY;
        const d = Math.sqrt((mx - sx) ** 2 + (my - sy) ** 2);
        if (d < HOVER_RADIUS * 2 && d < minDist) {
          minDist = d;
          closest = p;
        }
      }
      return closest;
    },
    [points, view],
  );

  const handleMouseMove = React.useCallback(
    (e: React.MouseEvent) => {
      if (dragRef.current) {
        const dx = e.clientX - dragRef.current.startX;
        const dy = e.clientY - dragRef.current.startY;
        setView((v) => ({
          ...v,
          offsetX: dragRef.current!.startOffsetX + dx,
          offsetY: dragRef.current!.startOffsetY + dy,
        }));
        return;
      }

      const coords = toDataCoords(e.clientX, e.clientY);
      if (!coords) return;
      const p = findPoint(coords.mx, coords.my);
      if (p) {
        const thumbUrl = `${apiUrl}/array/full/${nodePath}/thumbnails?format=image/png&slice=${p.index}`;
        setTooltip({
          point: p,
          screenX: coords.mx,
          screenY: coords.my,
          thumbnailUrl: thumbUrl,
        });
      } else {
        setTooltip(null);
      }
    },
    [toDataCoords, findPoint, apiUrl, nodePath],
  );

  const handleMouseDown = React.useCallback(
    (e: React.MouseEvent) => {
      dragRef.current = {
        startX: e.clientX,
        startY: e.clientY,
        startOffsetX: view.offsetX,
        startOffsetY: view.offsetY,
      };
    },
    [view],
  );

  const handleMouseUp = React.useCallback(() => {
    dragRef.current = null;
  }, []);

  const handleWheel = React.useCallback(
    (e: React.WheelEvent) => {
      e.preventDefault();
      const coords = toDataCoords(e.clientX, e.clientY);
      if (!coords) return;
      const factor = e.deltaY > 0 ? 0.9 : 1.1;
      setView((v) => ({
        scale: v.scale * factor,
        offsetX: coords.mx - (coords.mx - v.offsetX) * factor,
        offsetY: coords.my - (coords.my - v.offsetY) * factor,
      }));
    },
    [toDataCoords],
  );

  const handleClick = React.useCallback(
    (e: React.MouseEvent) => {
      const coords = toDataCoords(e.clientX, e.clientY);
      if (!coords) return;
      const p = findPoint(coords.mx, coords.my);
      if (!p) {
        setSelected(null);
        return;
      }
      const thumbUrl = `${apiUrl}/array/full/${nodePath}/thumbnails?format=image/png&slice=${p.index}`;
      setSelected({
        point: p,
        thumbnailUrl: thumbUrl,
        note: "",
        userLabel: "",
        originalNote: "",
        originalUserLabel: "",
        saving: false,
      });
      // Fetch current note and user_label
      Promise.all([
        fetchStringValue(apiUrl, nodePath, "notes", p.index),
        fetchStringValue(apiUrl, nodePath, "user_labels", p.index),
      ]).then(([note, userLabel]) => {
        setSelected((prev) =>
          prev && prev.point.index === p.index
            ? { ...prev, note, userLabel, originalNote: note, originalUserLabel: userLabel }
            : prev,
        );
      });
      setSaveError(null);
    },
    [toDataCoords, findPoint, apiUrl, nodePath],
  );

  const handleSave = React.useCallback(async () => {
    if (!selected) return;
    setSaveError(null);
    setSelected((prev) => (prev ? { ...prev, saving: true } : prev));
    const key = apiKey || undefined;
    const [noteRes, labelRes] = await Promise.all([
      patchStringArray(
        apiUrl, nodePath, "notes",
        selected.point.index, selected.note, NOTES_MAX_LEN, key,
      ),
      patchStringArray(
        apiUrl, nodePath, "user_labels",
        selected.point.index, selected.userLabel, USER_LABEL_MAX_LEN, key,
      ),
    ]);
    setSelected((prev) => (prev ? { ...prev, saving: false } : prev));
    if (noteRes.status === 401 || labelRes.status === 401) {
      setSaveError("auth");
    } else if (!noteRes.ok || !labelRes.ok) {
      setSaveError("Save failed");
    } else {
      // Save succeeded — update originals so button goes back to gray
      setSelected((prev) =>
        prev
          ? { ...prev, originalNote: prev.note, originalUserLabel: prev.userLabel }
          : prev,
      );
    }
  }, [selected, apiKey, apiUrl, nodePath]);

  const uniqueLabels = React.useMemo(() => {
    const labels = new Set<string>();
    for (const p of points) {
      if (p.label) labels.add(p.label);
    }
    return Array.from(labels).sort();
  }, [points]);

  const meta = item?.data?.attributes?.metadata || {};

  if (loading) {
    return React.createElement(
      "div",
      { style: { padding: 24, color: "#666" } },
      "Loading embedding data...",
    );
  }

  if (error) {
    return React.createElement(
      "div",
      { style: { padding: 24, color: "#c00" } },
      `Error: ${error}`,
    );
  }

  const isDirty =
    selected != null &&
    (selected.note !== selected.originalNote ||
      selected.userLabel !== selected.originalUserLabel);

  return React.createElement(
    "div",
    { ref: containerRef },
    // Header (full width, always above canvas + panel)
    React.createElement(
      "div",
      {
        style: {
          marginBottom: 12,
          fontSize: 14,
          color: "#555",
          display: "flex",
          alignItems: "center",
          gap: 16,
        },
      },
      React.createElement("span", null, `${points.length} embeddings`),
      meta.model_name
        ? React.createElement("span", null, `Model: ${meta.model_name}`)
        : null,
      meta.embedding_dim
        ? React.createElement("span", null, `Dim: ${meta.embedding_dim}`)
        : null,
      // Live status toggle
      React.createElement(
        "button",
        {
          onClick: () => setLiveEnabled((v) => !v),
          style: {
            marginLeft: "auto",
            display: "flex",
            alignItems: "center",
            gap: 4,
            fontSize: 12,
            background: "none",
            border: "1px solid #ccc",
            borderRadius: 12,
            padding: "2px 10px",
            cursor: "pointer",
            color: liveEnabled ? STATUS_COLORS[wsStatus] : "#999",
          },
        },
        React.createElement("span", {
          style: {
            width: 8,
            height: 8,
            borderRadius: "50%",
            backgroundColor: liveEnabled
              ? STATUS_COLORS[wsStatus]
              : "#999",
            display: "inline-block",
          },
        }),
        React.createElement(
          "span",
          null,
          !liveEnabled
            ? "Off"
            : wsStatus === "connected"
              ? "Live"
              : wsStatus === "connecting"
                ? "Connecting..."
                : "Reconnecting...",
        ),
      ),
    ),
    // Content area: canvas + panel side by side
    React.createElement(
      "div",
      { style: { position: "relative" } },
      // Canvas + tooltip wrapper
      React.createElement(
        "div",
        { style: { position: "relative", display: "inline-block" } },
      // Canvas
      React.createElement("canvas", {
        ref: canvasRef,
        width: canvasWidth,
        height: CANVAS_HEIGHT,
        style: {
          width: canvasWidth,
          height: CANVAS_HEIGHT,
          cursor: dragRef.current ? "grabbing" : "crosshair",
          border: "1px solid #ddd",
          borderRadius: 4,
          display: "block",
        },
        onMouseMove: handleMouseMove,
        onMouseDown: handleMouseDown,
        onMouseUp: handleMouseUp,
        onMouseLeave: () => {
          dragRef.current = null;
          setTooltip(null);
        },
        onWheel: handleWheel,
        onClick: handleClick,
      }),
      // Tooltip
      tooltip
        ? React.createElement(
            "div",
            {
              style: {
                position: "absolute",
                left: tooltip.screenX + 16,
                top: tooltip.screenY - 16,
                background: "white",
                border: "1px solid #ccc",
                borderRadius: 6,
                padding: 8,
                boxShadow: "0 2px 8px rgba(0,0,0,0.15)",
                pointerEvents: "none",
                zIndex: 10,
                maxWidth: 240,
                fontSize: 12,
              },
            },
            React.createElement("img", {
              src: tooltip.thumbnailUrl,
              alt: "thumbnail",
              style: {
                width: 96,
                height: 96,
                objectFit: "contain",
                display: "block",
                marginBottom: 4,
                imageRendering: "pixelated",
              },
            }),
            React.createElement("div", null, `#${tooltip.point.index}`),
            tooltip.point.label
              ? React.createElement(
                  "div",
                  null,
                  `Label: ${tooltip.point.label}`,
                )
              : null,
            tooltip.point.path
              ? React.createElement(
                  "div",
                  {
                    style: {
                      color: "#1976d2",
                      fontSize: 11,
                      wordBreak: "break-all",
                    },
                  },
                  tooltip.point.path,
                )
              : null,
          )
        : null,
      ),
      // Detail panel — anchored to right edge of content area
      selected
        ? React.createElement(
            "div",
            {
              style: {
                position: "absolute" as const,
                right: 0,
                top: 0,
                width: PANEL_WIDTH,
                height: CANVAS_HEIGHT,
                background: "white",
              border: "1px solid #ccc",
              borderRadius: 4,
              padding: "12px 16px",
              boxShadow: "0 2px 8px rgba(0,0,0,0.08)",
              overflowY: "auto" as const,
              fontSize: 13,
              boxSizing: "border-box" as const,
            },
          },
            // Close button
            React.createElement(
              "button",
              {
                onClick: () => { setSelected(null); setSaveError(null); },
                style: {
                  position: "absolute" as const,
                  top: 6,
                  right: 8,
                  background: "none",
                  border: "none",
                  fontSize: 18,
                  cursor: "pointer",
                  color: "#999",
                  lineHeight: 1,
                },
              },
              "\u00D7",
            ),
            // Thumbnail
            React.createElement("img", {
              src: selected.thumbnailUrl,
              alt: "thumbnail",
              style: {
                width: "100%",
                maxHeight: 180,
                objectFit: "contain",
                display: "block",
                marginBottom: 10,
                imageRendering: "pixelated" as const,
                background: "#f5f5f5",
                borderRadius: 4,
              },
            }),
            // Point info row
            React.createElement(
              "div",
              {
                style: {
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "baseline",
                  marginBottom: 4,
                  color: "#333",
                },
              },
              React.createElement("span", null, `#${selected.point.index}`),
              selected.point.label
                ? React.createElement(
                    "span",
                    { style: { color: getLabelColor(selected.point.label), fontWeight: 500 } },
                    selected.point.label,
                  )
                : null,
            ),
            // Source link
            selected.point.path
              ? React.createElement(
                  "a",
                  {
                    href: `/ui/browse/${selected.point.path}`,
                    target: "_blank",
                    rel: "noopener",
                    style: {
                      display: "block",
                      color: "#1976d2",
                      fontSize: 11,
                      marginBottom: 12,
                      wordBreak: "break-all" as const,
                    },
                  },
                  "View source \u2192",
                )
              : React.createElement("div", { style: { marginBottom: 12 } }),
            // User label
            React.createElement(
              "label",
              { style: { display: "block", fontSize: 11, color: "#777", marginBottom: 2 } },
              "User label",
            ),
            React.createElement("input", {
              type: "text",
              value: selected.userLabel,
              maxLength: USER_LABEL_MAX_LEN,
              onChange: (e: React.ChangeEvent<HTMLInputElement>) => {
                const val = e.target.value;
                setSelected((prev) => (prev ? { ...prev, userLabel: val } : prev));
              },
              onKeyDown: (e: React.KeyboardEvent) => {
                if (e.key === "Enter") handleSave();
              },
              style: {
                width: "100%",
                padding: "4px 8px",
                border: "1px solid #ccc",
                borderRadius: 4,
                fontSize: 13,
                marginBottom: 10,
                boxSizing: "border-box" as const,
              },
            }),
            // Note
            React.createElement(
              "label",
              { style: { display: "block", fontSize: 11, color: "#777", marginBottom: 2 } },
              "Note",
            ),
            React.createElement("textarea", {
              value: selected.note,
              maxLength: NOTES_MAX_LEN,
              onChange: (e: React.ChangeEvent<HTMLTextAreaElement>) => {
                const val = e.target.value;
                setSelected((prev) => (prev ? { ...prev, note: val } : prev));
              },
              style: {
                width: "100%",
                minHeight: 72,
                padding: "4px 8px",
                border: "1px solid #ccc",
                borderRadius: 4,
                fontSize: 13,
                resize: "vertical" as const,
                boxSizing: "border-box" as const,
              },
            }),
            // Save button (highlighted when dirty)
            React.createElement(
              "button",
              {
                onClick: handleSave,
                disabled: selected.saving || !isDirty,
                style: {
                  marginTop: 8,
                  width: "100%",
                  padding: "6px 0",
                  fontSize: 13,
                  border: isDirty ? "1px solid #1976d2" : "1px solid #ccc",
                  borderRadius: 4,
                  background: selected.saving
                    ? "#eee"
                    : isDirty
                      ? "#1976d2"
                      : "#f5f5f5",
                  color: isDirty && !selected.saving ? "#fff" : "#333",
                  cursor: selected.saving || !isDirty ? "default" : "pointer",
                  transition: "background 0.2s, border-color 0.2s, color 0.2s",
                },
              },
              selected.saving ? "Saving..." : "Save",
            ),
            // Auth error + API key prompt
            saveError === "auth"
              ? React.createElement(
                  "div",
                  { style: { marginTop: 10 } },
                  React.createElement(
                    "div",
                    { style: { fontSize: 11, color: "#c00", marginBottom: 6 } },
                    "Authentication required to save.",
                  ),
                  React.createElement(
                    "label",
                    { style: { display: "block", fontSize: 11, color: "#777", marginBottom: 2 } },
                    "API key",
                  ),
                  React.createElement("input", {
                    type: "password",
                    value: apiKey,
                    placeholder: "Enter API key",
                    onChange: (e: React.ChangeEvent<HTMLInputElement>) =>
                      setApiKey(e.target.value),
                    onKeyDown: (e: React.KeyboardEvent) => {
                      if (e.key === "Enter") handleSave();
                    },
                    style: {
                      width: "100%",
                      padding: "4px 8px",
                      border: "1px solid #e88",
                      borderRadius: 4,
                      fontSize: 12,
                      boxSizing: "border-box" as const,
                    },
                  }),
                  React.createElement(
                    "div",
                    { style: { fontSize: 10, color: "#999", marginTop: 3 } },
                    "Enter your API key, then press Save again.",
                  ),
                )
              : saveError
                ? React.createElement(
                    "div",
                    { style: { marginTop: 6, fontSize: 11, color: "#c00" } },
                    saveError,
                  )
                : null,
          )
        : null,
    ),
    // Legend
    uniqueLabels.length > 0
      ? React.createElement(
          "div",
          {
            style: {
              marginTop: 8,
              display: "flex",
              flexWrap: "wrap",
              gap: 12,
              fontSize: 12,
            },
          },
          ...uniqueLabels.map((label) =>
            React.createElement(
              "div",
              {
                key: label,
                style: { display: "flex", alignItems: "center", gap: 4 },
              },
              React.createElement("div", {
                style: {
                  width: 10,
                  height: 10,
                  borderRadius: "50%",
                  backgroundColor: getLabelColor(label),
                },
              }),
              React.createElement("span", null, label),
            ),
          ),
        )
      : null,
    // Instructions
    React.createElement(
      "div",
      { style: { marginTop: 6, fontSize: 11, color: "#999" } },
      "Scroll to zoom. Drag to pan. Click point to inspect and annotate.",
    ),
  );
}

export default EmbeddingScatter;
