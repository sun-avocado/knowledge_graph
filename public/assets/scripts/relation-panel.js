(function () {
// Relation Manager JS
// ----------------------
const shared = window.kbApp || {};
const state = shared.state || {};
const dom = shared.dom || {};
const byId = dom.byId || ((id) => document.getElementById(id));
const relId = byId("relId");
const relFrom = byId("relFrom");
const relTo = byId("relTo");
const relProp = byId("relProp");
const relPropLabel = byId("relPropLabel");
const relPropHint = byId("relPropHint");
const relMsg = byId("relMsg");
const relModeEl = byId("relMode");
const btnPickFrom = byId("btnPickFrom");
const btnPickTo = byId("btnPickTo");
const btnRelSwap = byId("btnRelSwap");
const btnRelClear = byId("btnRelClear");
const btnRelCreate = byId("btnRelCreate");
const btnRelSave = byId("btnRelSave");
const btnRelDelete = byId("btnRelDelete");
const relPropertyCache = new Map();
if (typeof state.bindAlias === "function") {
  state.bindAlias("kbPickMode", "pickMode", null);
}

function updateRelPropHint() {
  if (!relPropHint) return;
  const code = (relProp?.value || "").trim();
  relPropHint.textContent = code
    ? `属性编号：${code}`
    : "属性编号：保存后自动生成";
}

async function resolvePropertyCodeByName(name) {
  const key = (name || "").trim();
  if (!key) return null;
  if (relPropertyCache.has(key)) return relPropertyCache.get(key);
  try {
    const url = new URL("/api/kb/properties", window.location.origin);
    if (typeof window.appendCurrentDbParam === "function") {
      const scopedUrl = window.appendCurrentDbParam(url);
      if (scopedUrl instanceof URL) {
        url.search = scopedUrl.search;
      }
    }
    url.searchParams.set("q", key);
    const resp = await fetch(url.toString());
    if (!resp.ok) {
      relPropertyCache.set(key, null);
      return null;
    }
    const data = await resp.json();
    const items = Array.isArray(data.items) ? data.items : [];
    const exact = items.find((it) => (it?.label || "").trim() === key);
    if (exact && exact.id) {
      const id = String(exact.id || "");
      const code = id.includes("/") ? id.split("/").pop() : id;
      relPropertyCache.set(key, code || null);
      return code || null;
    }
  } catch {}
  relPropertyCache.set(key, null);
  return null;
}

function fillRelForm(edge) {
  relId.value = edge?.id || "";
  relFrom.value = edge?.source || "";
  relTo.value = edge?.target || "";
  relProp.value = edge?.property || "";
  const label = edge?.property_label_zh || "";
  relPropLabel.value = label || edge?.property || "";
  relMsg.textContent = "";
  updateRelPropHint();
}

function setRelMode(mode) {
  // mode: 'edit' | 'create'
  relModeEl.textContent = mode === "edit" ? "编辑模式" : "新建模式";
  relModeEl.style.color = mode === "edit" ? "#f59e0b" : "var(--muted)";
}

function ensureRelButtonsState() {
  const hasId = !!(relId.value && relId.value.trim());
  btnRelSave.disabled = !hasId;
  btnRelDelete.disabled = !hasId;
}

async function loadRelations(nodeId) {
  // 自动加 entity/ 前缀（如果没有）
  const fullId = nodeId.startsWith("entity/")
    ? nodeId
    : "entity/" + nodeId;
  const url = new URL("/api/kb/node/relations", window.location.origin);
  if (typeof window.appendCurrentDbParam === "function") {
    const scopedUrl = window.appendCurrentDbParam(url);
    if (scopedUrl instanceof URL) {
      url.search = scopedUrl.search;
    }
  }
  url.searchParams.set("id", fullId);
  const resp = await fetch(url.toString());
  if (!resp.ok) return; // silent
  const data = await resp.json();
  const items = Array.isArray(data.items) ? data.items : [];
  // Optionally annotate edges in cy
  try {
    if (window.kbCy) {
      window.kbCy.edges().removeClass("highlight");
      items.forEach((it) => {
        const e = window.kbCy.getElementById(it.id);
        if (e) e.addClass("highlight");
      });
    }
  } catch {}
}

btnRelCreate.addEventListener("click", async () => {
  relMsg.textContent = "";
  const fromId = relFrom.value.trim();
  const toId = relTo.value.trim();
  const propName = relPropLabel.value.trim();
  if (!fromId || !toId || !propName) {
    relMsg.textContent = "请填写起点、终点和属性名称";
    return;
  }
  const currentProp = (relProp.value || "").trim();
  const body = {
    from_id: fromId,
    to_id: toId,
    property_name: propName,
    property_label_zh: propName,
  };
  if (currentProp) body.property = currentProp;
  try {
    const resp = await fetch("/api/kb/relations/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const d = await resp.json();
    relMsg.textContent = "已新增";
    if (d?.property) relProp.value = d.property;
    if (d?.property_label_zh) relPropLabel.value = d.property_label_zh;
    updateRelPropHint();
    // reload graph
    await loadGraph();
    // focus edge
    try {
      if (d.id && window.kbCy) {
        const e = window.kbCy.getElementById(d.id);
        if (e) {
          window.kbCy.center(e);
          window.kbCy.fit(e, 60);
        }
      }
    } catch {}
    // switch to edit mode for the new relation id
    try {
      if (d.id) {
        relId.value = d.id;
        setRelMode("edit");
        ensureRelButtonsState();
      }
    } catch {}
  } catch (e) {
    console.error(e);
    relMsg.textContent = "新增失败";
  }
});

btnRelSave.addEventListener("click", async () => {
  relMsg.textContent = "";
  const id = relId.value.trim();
  if (!id) {
    relMsg.textContent = "请先点击选中一条边";
    return;
  }
  const propName = relPropLabel.value.trim();
  if (!propName) {
    relMsg.textContent = "请填写属性名称";
    return;
  }
  const currentProp = (relProp.value || "").trim();
  const body = {
    id,
    property_name: propName,
    property_label_zh: propName,
  };
  if (currentProp) body.property = currentProp;
  try {
    const resp = await fetch("/api/kb/relations/update", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const d = await resp.json();
    relMsg.textContent = "已保存";
    if (d?.property) relProp.value = d.property || "";
    if (d?.property_label_zh)
      relPropLabel.value = d.property_label_zh || propName;
    updateRelPropHint();
    await loadGraph();
  } catch (e) {
    console.error(e);
    relMsg.textContent = "保存失败";
  }
});

btnRelDelete.addEventListener("click", async () => {
  relMsg.textContent = "";
  const id = relId.value.trim();
  if (!id) {
    relMsg.textContent = "请先点击选中一条边";
    return;
  }
  if (!confirm("确定删除该关系？")) return;
  try {
    const resp = await fetch(
      `/api/kb/relations/${encodeURIComponent(id)}`,
      { method: "DELETE" }
    );
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    relMsg.textContent = "已删除";
    relId.value = "";
    relFrom.value = "";
    relTo.value = "";
    relProp.value = "";
    relPropLabel.value = "";
    updateRelPropHint();
    setRelMode("create");
    ensureRelButtonsState();
    await loadGraph();
  } catch (e) {
    console.error(e);
    relMsg.textContent = "删除失败";
  }
});

// Pick mode controls
window.kbPickMode = null; // 'from' | 'to' | null
function annotatePickMode() {
  try {
    if (window.kbPickMode === "from") {
      relMsg.textContent = "请在图上点击节点作为起点";
    } else if (window.kbPickMode === "to") {
      relMsg.textContent = "请在图上点击节点作为终点";
    } else {
      if (relMsg.textContent.startsWith("请在图上点击"))
        relMsg.textContent = "";
    }
  } catch {}
}
btnPickFrom.addEventListener("click", () => {
  window.kbPickMode = "from";
  annotatePickMode();
});
btnPickTo.addEventListener("click", () => {
  window.kbPickMode = "to";
  annotatePickMode();
});
btnRelSwap.addEventListener("click", () => {
  const a = relFrom.value;
  relFrom.value = relTo.value;
  relTo.value = a;
  relMsg.textContent = "已交换方向";
});
btnRelClear.addEventListener("click", () => {
  relId.value = "";
  relFrom.value = "";
  relTo.value = "";
  relProp.value = "";
  relPropLabel.value = "";
  relMsg.textContent = "";
  updateRelPropHint();
  setRelMode("create");
  ensureRelButtonsState();
});

if (relPropLabel) {
  relPropLabel.addEventListener("input", () => {
    if (relProp.value) relProp.value = "";
    updateRelPropHint();
  });
  relPropLabel.addEventListener("blur", async () => {
    const name = relPropLabel.value.trim();
    if (!name) {
      relProp.value = "";
      updateRelPropHint();
      return;
    }
    const code = await resolvePropertyCodeByName(name);
    if (code) relProp.value = code;
    updateRelPropHint();
  });
}

updateRelPropHint();

// Init default states
setRelMode("create");
ensureRelButtonsState();

window.updateRelPropHint = updateRelPropHint;
window.resolvePropertyCodeByName = resolvePropertyCodeByName;
window.fillRelForm = fillRelForm;
window.setRelMode = setRelMode;
window.ensureRelButtonsState = ensureRelButtonsState;
window.loadRelations = loadRelations;
window.annotatePickMode = annotatePickMode;
})();
