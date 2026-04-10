(function () {
  const shared = window.kbApp || {};
  const state = shared.state || {};
  const dom = shared.dom || {};
  const byId = dom.byId || ((id) => document.getElementById(id));
  const btnDeleteSelected = byId("btnDeleteSelected");
  const tblNodes = byId("tblNodes");

  if (typeof state.bindAlias === "function") {
    state.bindAlias("kbSelectedRowId", "selectedRowId", "");
    state.bindAlias("kbSelectedRowIds", "selectedRowIds", () => new Set());
    state.bindAlias("kbLastAnchorRowId", "lastAnchorRowId", "");
    state.bindAlias("kbSelectionHydrated", "selectionHydrated", false);
  }

  function ensureTableSelectedButtonsState() {
    const count =
      window.kbSelectedRowIds &&
      typeof window.kbSelectedRowIds.size === "number"
        ? window.kbSelectedRowIds.size
        : window.kbSelectedRowId
          ? 1
          : 0;
    try {
      if (btnDeleteSelected) {
        btnDeleteSelected.disabled = count === 0;
      }
    } catch {}
  }

  function updateSelectedRowStyles() {
    try {
      if (!tblNodes) return;
      const rows = tblNodes.querySelectorAll("tbody tr");
      rows.forEach((tr) => {
        const rid = tr.getAttribute("data-id") || "";
        const selected =
          (window.kbSelectedRowIds && window.kbSelectedRowIds.has(rid)) ||
          rid === window.kbSelectedRowId;
        tr.classList.toggle("selected", selected);
      });
    } catch {}
  }

  function syncCheckboxStates() {
    try {
      if (!tblNodes) return;
      const checkboxes = tblNodes.querySelectorAll("tbody .row-checkbox");
      let checkedCount = 0;
      checkboxes.forEach((chk) => {
        const rid = chk.getAttribute("data-id") || "";
        const selected =
          window.kbSelectedRowIds && window.kbSelectedRowIds.has(rid);
        chk.checked = selected;
        if (selected) checkedCount++;
      });
      // 更新全选复选框状态
      const chkSelectAll = document.getElementById("chkSelectAll");
      if (chkSelectAll) {
        const total = checkboxes.length;
        chkSelectAll.checked = total > 0 && checkedCount === total;
        chkSelectAll.indeterminate = checkedCount > 0 && checkedCount < total;
      }
    } catch {}
  }

  function setTableSelection(id, autoEdit = true, options = {}) {
    const opts = options || {};
    const skipDetailRefresh = opts.skipDetailRefresh === true;
    const selectedId = id || "";
    window.kbSelectedRowId = selectedId;
    window.kbSelectedRowIds = new Set(selectedId ? [selectedId] : []);
    window.kbLastAnchorRowId = selectedId;
    window.kbSelectedNodeId = selectedId;
    window.kbCurrentNodeId = selectedId;
    if (id) {
      window.kbActiveVisNodeId =
        (typeof stripEntityIdPrefix === "function" &&
          stripEntityIdPrefix(id)) ||
        id;
      window.kbActiveDetailRouteId = id;
      try {
        if (typeof normalizeEntityIdForApi === "function") {
          window.kbActiveDetailNodeId = normalizeEntityIdForApi(id) || id;
        } else {
          window.kbActiveDetailNodeId = id;
        }
      } catch {
        window.kbActiveDetailNodeId = id;
      }
    } else {
      window.kbActiveVisNodeId = "";
      window.kbActiveDetailRouteId = "";
      window.kbActiveDetailNodeId = "";
    }
    if (!id) {
      window.kbSelectionHydrated = false;
    }
    updateSelectedRowStyles();
    syncCheckboxStates();
    ensureTableSelectedButtonsState();
    if ((window.kbViewMode || "table") === "table") {
      try {
        if (typeof syncHashForView === "function") {
          syncHashForView("table", {
            nodeId: id || "",
            includeNode: true,
          });
        }
      } catch (err) {
        if (window.console && console.warn) {
          console.warn("sync hash for selection failed", err);
        }
      }
    }
    if (autoEdit && id) {
      try {
        if (typeof enterEditById === "function") {
          enterEditById(id);
          window.kbSelectionHydrated = true;
        }
      } catch (e) {
        console.error("enterEditById failed", e);
      }
    }
    try {
      if (!skipDetailRefresh && window.kbViewMode === "detail" && id) {
        if (typeof showNodeDetailInline === "function") {
          showNodeDetailInline(id);
        }
      }
    } catch {}
    try {
      if (window.kbViewMode === "vis" && id) {
        if (typeof focusNode === "function") {
          focusNode(id, { fit: false, duration: 180 });
        }
      }
    } catch {}
  }

  function toggleCtrlSelection(id) {
    if (!id) return;
    if (!window.kbSelectedRowIds) window.kbSelectedRowIds = new Set();
    if (window.kbSelectedRowIds.has(id)) window.kbSelectedRowIds.delete(id);
    else window.kbSelectedRowIds.add(id);
    if (window.kbSelectedRowIds.size === 1) {
      window.kbLastAnchorRowId = Array.from(window.kbSelectedRowIds)[0];
    }
    if (window.kbSelectedRowIds.size === 1) {
      const primaryId = Array.from(window.kbSelectedRowIds)[0] || "";
      window.kbSelectedRowId = primaryId;
      window.kbSelectedNodeId = primaryId;
      window.kbCurrentNodeId = primaryId;
      if (primaryId) {
        window.kbActiveVisNodeId =
          (typeof stripEntityIdPrefix === "function" &&
            stripEntityIdPrefix(primaryId)) ||
          primaryId;
        window.kbActiveDetailRouteId = primaryId;
      }
    }
    updateSelectedRowStyles();
    syncCheckboxStates();
    ensureTableSelectedButtonsState();
  }

  function rangeSelectTo(id) {
    try {
      if (!tblNodes) {
        setTableSelection(id);
        return;
      }
      const rows = Array.from(tblNodes.querySelectorAll("tbody tr"));
      const ids = rows.map((tr) => tr.getAttribute("data-id") || "");
      const a = ids.indexOf(window.kbLastAnchorRowId || "");
      const b = ids.indexOf(id || "");
      if (a === -1 || b === -1) {
        setTableSelection(id);
        return;
      }
      const [start, end] = a < b ? [a, b] : [b, a];
      window.kbSelectedRowIds = new Set(ids.slice(start, end + 1));
      const primaryId = ids[b] || id || "";
      window.kbSelectedRowId = primaryId;
      window.kbSelectedNodeId = primaryId;
      window.kbCurrentNodeId = primaryId;
      if (primaryId) {
        window.kbActiveVisNodeId =
          (typeof stripEntityIdPrefix === "function" &&
            stripEntityIdPrefix(primaryId)) ||
          primaryId;
        window.kbActiveDetailRouteId = primaryId;
      }
      updateSelectedRowStyles();
      syncCheckboxStates();
      ensureTableSelectedButtonsState();
    } catch {
      setTableSelection(id);
    }
  }

  function getTableRows() {
    try {
      if (!tblNodes) return [];
      return Array.from(tblNodes.querySelectorAll("tbody tr")) || [];
    } catch {
      return [];
    }
  }

  function focusRowElement(row) {
    if (!row) return;
    try {
      row.focus({ preventScroll: true });
    } catch {}
  }

  function scrollRowIntoView(row) {
    if (!row) return;
    try {
      row.scrollIntoView({ block: "nearest" });
    } catch {}
  }

  function moveTableSelection(step) {
    if (!step) return;
    const rows = getTableRows();
    if (!rows.length) return;
    let idx = rows.findIndex(
      (tr) => (tr.getAttribute("data-id") || "") === window.kbSelectedRowId,
    );
    if (idx === -1) idx = step > 0 ? -1 : rows.length;
    idx = Math.max(0, Math.min(rows.length - 1, idx + step));
    const target = rows[idx];
    if (!target) return;
    const id = target.getAttribute("data-id") || "";
    if (!id) return;
    if (window.kbSelectedRowId !== id) {
      setTableSelection(id);
    }
    requestAnimationFrame(() => {
      focusRowElement(target);
      scrollRowIntoView(target);
    });
  }

  function openSelectedNodeDetail() {
    const ids =
      window.kbSelectedRowIds && window.kbSelectedRowIds.size
        ? Array.from(window.kbSelectedRowIds)
        : [];
    const primaryId = ids.length ? ids[0] : window.kbSelectedRowId || "";
    if (!primaryId) return false;

    const rows = getTableRows();
    const target = rows.find(
      (tr) => (tr.getAttribute("data-id") || "") === primaryId,
    );
    let href = "";
    if (target) {
      const link = target.querySelector("td:nth-child(2) a");
      if (link && link.href) {
        href = link.href;
      }
    }
    if (!href) {
      const label = target
        ? target.querySelector("td:nth-child(2) a")?.textContent || ""
        : "";
      const params = new URLSearchParams();
      params.set("id", primaryId);
      const trimmed = label.trim();
      if (trimmed) params.set("label", trimmed);
      href = "/kb/detail" + (params.toString() ? "?" + params.toString() : "");
    }
    try {
      window.open(href, "noopener");
      return true;
    } catch {}
    return false;
  }

  function positionTooltip(e, tip) {
    const pad = 12;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const rect = tip.getBoundingClientRect();
    let x = e.clientX + pad;
    let y = e.clientY + pad;
    if (x + rect.width + pad > vw) x = vw - rect.width - pad;
    if (y + rect.height + pad > vh) y = vh - rect.height - pad;
    tip.style.left = x + "px";
    tip.style.top = y + "px";
  }

  function renderTableList() {
    if (!tblNodes) return;

    const tbody = tblNodes.querySelector("tbody");
    if (!tbody) return;

    const rawList = Array.isArray(window.kbTableNodes)
      ? window.kbTableNodes
      : [];
    const frag = document.createDocumentFragment();

    rawList.forEach((n) => {
      const tr = document.createElement("tr");
      tr.tabIndex = -1;
      tr.setAttribute("data-id", n._id || n.id || "");

      const desc = (n.desc_zh || n.description || "").trim();
      if (desc) tr.setAttribute("data-desc", desc);

      // 复选框列
      const tdChk = document.createElement("td");
      tdChk.style.textAlign = "center";
      const chk = document.createElement("input");
      chk.type = "checkbox";
      chk.className = "row-checkbox";
      chk.style.cursor = "pointer";
      chk.setAttribute("data-id", n._id || n.id || "");
      chk.addEventListener("click", (e) => {
        e.stopPropagation();
        const rid = chk.getAttribute("data-id") || "";
        if (!rid) return;
        if (chk.checked) {
          if (!window.kbSelectedRowIds) window.kbSelectedRowIds = new Set();
          window.kbSelectedRowIds.add(rid);
          window.kbLastAnchorRowId = rid;
        } else {
          if (window.kbSelectedRowIds) window.kbSelectedRowIds.delete(rid);
        }
        updateSelectedRowStyles();
        syncCheckboxStates();
        ensureTableSelectedButtonsState();
      });
      tdChk.appendChild(chk);
      tr.appendChild(tdChk);

      const tdIdx = document.createElement("td");
      tdIdx.textContent = String(n.id);
      tr.appendChild(tdIdx);

      const tdName = document.createElement("td");
      const nameLink = document.createElement("a");
      nameLink.textContent = n.label_zh || n.label || "";
      nameLink.style.color = "var(--link)";
      nameLink.style.textDecoration = "none";
      nameLink.addEventListener("mouseenter", () => {
        nameLink.style.textDecoration = "underline";
      });
      nameLink.addEventListener("mouseleave", () => {
        nameLink.style.textDecoration = "none";
      });

      try {
        const id = n._id || n.id || "";
        const label = n.label_zh || n.label || "";
        const params = new URLSearchParams();
        if (id) params.set("id", id);
        else if (label) params.set("label", label);
        nameLink.href =
          "/kb/detail" + (params.toString() ? "?" + params.toString() : "");
        nameLink.rel = "noreferrer noopener";
      } catch {}

      nameLink.addEventListener("click", (e) => {
        try {
          if (e.ctrlKey || e.metaKey || e.shiftKey || e.button !== 0) return;
          e.preventDefault();
          const rid = tr.getAttribute("data-id") || n._id || n.id || "";
          if (!rid) return;
          setTableSelection(rid, false);
          if (typeof setViewMode === "function") {
            setViewMode("detail", { targetNodeId: rid });
          }
        } catch {}
      });

      tdName.appendChild(nameLink);
      tr.appendChild(tdName);

      tr.addEventListener("click", (e) => {
        const target = e.target;
        if (
          target &&
          (target.closest("button") ||
            (target.closest("a") && target.closest("a") !== nameLink))
        ) {
          return;
        }
        const rid = tr.getAttribute("data-id") || "";
        if (!rid) return;
        if (e.shiftKey && window.kbLastAnchorRowId) {
          rangeSelectTo(rid);
        } else if (e.ctrlKey || e.metaKey) {
          toggleCtrlSelection(rid);
        } else {
          setTableSelection(rid, true);
        }
        focusRowElement(tr);
      });

      tr.addEventListener("dblclick", async () => {
        const rid = tr.getAttribute("data-id") || "";
        if (!rid) return;
        setTableSelection(rid);
        if (typeof setViewMode === "function") {
          setViewMode("vis", { targetNodeId: rid });
        }
        try {
          if (!window.kbCy && typeof loadGraph === "function")
            await loadGraph();
          if (typeof focusNode === "function") focusNode(rid);
        } catch {}
      });

      tr.addEventListener("mouseenter", (e) => {
        const d = tr.getAttribute("data-desc") || "";
        if (!d) return;
        let tip = document.querySelector(".kb-tooltip");
        if (!tip) {
          tip = document.createElement("div");
          tip.className = "kb-tooltip";
          document.body.appendChild(tip);
        }
        tip.textContent = d.length > 180 ? d.slice(0, 180) + "…" : d;
        tip.style.display = "block";
        positionTooltip(e, tip);
      });

      tr.addEventListener("mousemove", (e) => {
        const tip = document.querySelector(".kb-tooltip");
        if (tip && tip.style.display === "block") positionTooltip(e, tip);
      });

      tr.addEventListener("mouseleave", () => {
        const tip = document.querySelector(".kb-tooltip");
        if (tip) tip.style.display = "none";
      });

      frag.appendChild(tr);
    });

    tbody.innerHTML = "";
    tbody.appendChild(frag);

    try {
      const rows = Array.from(tblNodes.querySelectorAll("tbody tr"));
      const has = rows.some(
        (tr) => (tr.getAttribute("data-id") || "") === window.kbSelectedRowId,
      );
      const hasExistingSelection =
        (window.kbSelectedRowId && window.kbSelectedRowId.trim()) ||
        (window.kbSelectedRowIds && window.kbSelectedRowIds.size > 0);
      const isTableViewActive = (window.kbViewMode || "table") === "table";
      if (!has) {
        if (!hasExistingSelection) {
          if (isTableViewActive && rows.length) {
            const firstId = rows[0].getAttribute("data-id") || "";
            if (firstId) {
              setTableSelection(firstId, true);
              requestAnimationFrame(() => {
                try {
                  rows[0].focus({ preventScroll: true });
                  rows[0].scrollIntoView({ block: "nearest" });
                } catch {}
              });
            }
          } else if (!rows.length) {
            setTableSelection("");
          }
        }
      } else {
        updateSelectedRowStyles();
        syncCheckboxStates();
      }
    } catch {}

    syncCheckboxStates();
    ensureTableSelectedButtonsState();
  }

  async function deleteSelectedRows() {
    const ids = Array.from(window.kbSelectedRowIds || []);
    if (!ids.length) return;
    if (
      !confirm(
        `确定删除选中 ${ids.length} 个节点及其所有关系？此操作不可恢复。`,
      )
    ) {
      return;
    }

    let okCount = 0;
    for (const id of ids) {
      try {
        const resp = await fetch("/api/kb/nodes?id=" + encodeURIComponent(id), {
          method: "DELETE",
        });
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        const data = await resp.json();
        if (data && data.ok) okCount++;
        else console.warn("删除失败", id);
      } catch (e) {
        console.error("删除失败", id, e);
      }
    }

    alert(`删除完成：成功 ${okCount} / 总计 ${ids.length}`);
    window.kbSelectedRowIds = new Set();
    window.kbSelectedRowId = "";
    window.kbLastAnchorRowId = "";
    if (typeof loadGraph === "function") {
      await loadGraph();
    }
    if (typeof loadTablePage === "function") {
      await loadTablePage({ resetPage: true });
    }
  }

  function bindDeleteButton() {
    if (!btnDeleteSelected) return;
    btnDeleteSelected.addEventListener("click", deleteSelectedRows);
    ensureTableSelectedButtonsState();
  }

  function bindSelectAll() {
    const chkSelectAll = document.getElementById("chkSelectAll");
    if (!chkSelectAll) return;
    chkSelectAll.addEventListener("change", () => {
      if (!tblNodes) return;
      const checkboxes = tblNodes.querySelectorAll("tbody .row-checkbox");
      if (chkSelectAll.checked) {
        // 全选
        window.kbSelectedRowIds = new Set();
        checkboxes.forEach((chk) => {
          const rid = chk.getAttribute("data-id") || "";
          if (rid) {
            window.kbSelectedRowIds.add(rid);
            chk.checked = true;
          }
        });
      } else {
        // 取消全选
        window.kbSelectedRowIds = new Set();
        checkboxes.forEach((chk) => {
          chk.checked = false;
        });
      }
      updateSelectedRowStyles();
      ensureTableSelectedButtonsState();
    });
  }

  window.ensureTableSelectedButtonsState = ensureTableSelectedButtonsState;
  window.updateSelectedRowStyles = updateSelectedRowStyles;
  window.syncCheckboxStates = syncCheckboxStates;
  window.setTableSelection = setTableSelection;
  window.toggleCtrlSelection = toggleCtrlSelection;
  window.rangeSelectTo = rangeSelectTo;
  window.getTableRows = getTableRows;
  window.focusRowElement = focusRowElement;
  window.scrollRowIntoView = scrollRowIntoView;
  window.moveTableSelection = moveTableSelection;
  window.openSelectedNodeDetail = openSelectedNodeDetail;
  window.positionTooltip = positionTooltip;
  window.renderTableList = renderTableList;

  bindDeleteButton();
  bindSelectAll();
})();
