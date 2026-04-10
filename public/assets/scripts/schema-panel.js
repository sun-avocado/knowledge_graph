(function () {
  // Classification & Schema Panel JS
  // ----------------------
  const shared = window.kbApp || {};
  const state = shared.state || {};
  const dom = shared.dom || {};
  const byId = dom.byId || ((id) => document.getElementById(id));
  const clsSearch = byId("clsSearch");
  const btnClsAdd = byId("btnClsAdd");
  const btnClsRefresh = byId("btnClsRefresh");
  const clsTree = byId("clsTree");
  const clsForEntity = byId("clsForEntity");
  const btnSetClass = byId("btnSetClass");
  const btnClearClass = byId("btnClearClass");
  const classModal = byId("classModal");
  const classModalTitle = byId("classModalTitle");
  const classForm = byId("classForm");
  const clsNameInput = byId("clsNameInput");
  const clsDescInput = byId("clsDescInput");
  const btnSchemaRefresh = byId("btnSchemaRefresh");
  const schemaList = byId("schemaList");
  const schemaPropId = byId("schemaPropId");
  const btnAddSchema = byId("btnAddSchema");
  const btnRemoveSchemaSelected = byId("btnRemoveSchemaSelected");
  // Subclass editing removed per request
  // Create class/property controls
  // New-class selection element (only select; no free-text)
  const propSearchInput = byId("propSearchInput");
  const propSearchResults = byId("propSearchResults");
  const propSearchResultsWrap = byId("propSearchResultsWrap");
  const btnCreateProperty = byId("btnCreateProperty");
  const propCreateMsg = byId("propCreateMsg");
  const propRecommendContainer = byId("propRecommendContainer");
  const propRecommendStatus = byId("propRecommendStatus");
  const propRecommendList = byId("propRecommendList");
  const btnPropRecommendRefresh = byId("btnPropRecommendRefresh");
  const btnOpenPropPanel = byId("btnOpenPropPanel");
  const propPanelModal = byId("propPanelModal");
  const btnPropPanelClose = byId("btnPropPanelClose");

  const PROPERTY_RECOMMEND_SAMPLE = 100;
  const PROPERTY_RECOMMEND_TTL = 5 * 60 * 1000;
  if (typeof state.bindAlias === "function") {
    state.bindAlias(
      "kbPropertyRecommendationsCache",
      "propertyRecommendationsCache",
      () => new Map(),
    );
    state.bindAlias("kbSelectedClassId", "selectedClassId", null);
    state.bindAlias("kbClasses", "classes", () => []);
    state.bindAlias("kbEntityClasses", "entityClasses", () => []);
    state.bindAlias("kbSchemaByClassId", "schemaByClassId", () =>
      Object.create(null),
    );
    state.bindAlias("kbSelectedSchemaPropId", "selectedSchemaPropId", "");
    state.bindAlias("kbSelectedSchemaPropLabel", "selectedSchemaPropLabel", "");
    state.bindAlias(
      "kbSchemaRemovalSelection",
      "schemaRemovalSelection",
      () => new Set(),
    );
    state.bindAlias("kbSchemaRemovalLastIndex", "schemaRemovalLastIndex", -1);
    state.bindAlias(
      "kbCollapsedClassIds",
      "collapsedClassIds",
      () => new Set(),
    );
    state.bindAlias(
      "kbClassTreeInitiallyCollapsed",
      "classTreeInitiallyCollapsed",
      true,
    );
    state.bindAlias("kbClassMeta", "classMeta", null);
    state.bindAlias("kbClassDragSourceId", "classDragSourceId", null);
    state.bindAlias("kbFilteredClassId", "filteredClassId", null);
  }
  // 确保筛选状态变量存在
  if (typeof window.kbFilteredClassId === "undefined") {
    window.kbFilteredClassId = null;
  }
  let propRecommendRequestSeq = 0;
  let propRecommendActiveId = "";

  // Local wrapper for pickUiDatatype (defined in attr-panel.js)
  function pickUiDatatype(item) {
    if (typeof window.pickUiDatatype === "function") {
      return window.pickUiDatatype(item);
    }
    // Fallback implementation
    if (!item || typeof item !== "object") return "";
    if (item.ui_datatype) return item.ui_datatype;
    const dt = (item.datatype || "").toString().toLowerCase();
    if (dt === "commonsmedia") return "commonsMedia";
    return item.datatype || "";
  }

  function openPropPanelModal() {
    if (!propPanelModal) return;
    propPanelModal.style.display = "flex";
    propPanelModal.setAttribute("aria-hidden", "false");
    try {
      propPanelModal.querySelector("input")?.focus();
    } catch {}
  }

  function closePropPanelModal() {
    if (!propPanelModal) return;
    propPanelModal.style.display = "none";
    propPanelModal.setAttribute("aria-hidden", "true");
  }

  if (btnOpenPropPanel && propPanelModal) {
    btnOpenPropPanel.addEventListener("click", () => {
      openPropPanelModal();
    });
  }

  if (btnPropPanelClose) {
    btnPropPanelClose.addEventListener("click", () => {
      closePropPanelModal();
    });
  }

  if (propPanelModal) {
    propPanelModal.addEventListener("click", (event) => {
      if (event.target === propPanelModal) closePropPanelModal();
    });
  }

  function getActiveClassIdForRecommendations() {
    const explicit = (window.kbSelectedClassId || "").trim();
    if (explicit) return explicit;
    if (
      Array.isArray(window.kbEntityClasses) &&
      window.kbEntityClasses.length
    ) {
      const first = window.kbEntityClasses[0];
      const candidate = (first?.id || first?._id || "").trim();
      if (candidate) return candidate;
    }
    return "";
  }

  async function fetchPropertyRecommendationsForClass(classId, options = {}) {
    const force = !!options.force;
    const cache = window.kbPropertyRecommendationsCache;
    const now = Date.now();
    if (!force && cache.has(classId)) {
      const cached = cache.get(classId);
      if (cached && now - cached.ts < PROPERTY_RECOMMEND_TTL) {
        return cached.items;
      }
    }
    const url = new URL("/api/kb/entity_search", window.location.origin);
    if (typeof window.appendCurrentDbParam === "function") {
      const scopedUrl = window.appendCurrentDbParam(url);
      if (scopedUrl instanceof URL) {
        url.search = scopedUrl.search;
      }
    }
    url.searchParams.set("class_id", classId);
    url.searchParams.set("limit", String(PROPERTY_RECOMMEND_SAMPLE));
    url.searchParams.set("offset", "0");
    const resp = await fetch(url.toString());
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const data = await resp.json();
    const nodes = Array.isArray(data?.nodes) ? data.nodes : [];
    if (!nodes.length) {
      cache.set(classId, { ts: now, items: [] });
      return [];
    }
    const ids = nodes
      .map((n) => (n?.id || n?._id || "").toString().trim())
      .filter(Boolean)
      .map((id) => (id.includes("/") ? id : `entity/${id}`));
    const attrLists = await Promise.all(
      ids.map((id) =>
        (() => {
          const url = new URL(
            "/api/kb/node/attributes",
            window.location.origin,
          );
          if (typeof window.appendCurrentDbParam === "function") {
            const scopedUrl = window.appendCurrentDbParam(url);
            if (scopedUrl instanceof URL) {
              url.search = scopedUrl.search;
            }
          }
          url.searchParams.set("id", id);
          return fetch(url.toString());
        })()
          .then((res) => (res.ok ? res.json() : null))
          .then((payload) =>
            payload && Array.isArray(payload.items) ? payload.items : [],
          )
          .catch(() => []),
      ),
    );
    const propMap = new Map();
    for (const list of attrLists) {
      const seen = new Set();
      for (const it of list) {
        const pid = canonicalizePropertyId(it?.property);
        if (!pid) continue;
        seen.add(pid);
        const label =
          typeof it?.property_label_zh === "string"
            ? it.property_label_zh.trim()
            : "";
        const dtype =
          pickUiDatatype(it) || it?.datatype || it?.ui_datatype || "";
        const existing = propMap.get(pid);
        if (existing) {
          if (!existing.label && label) existing.label = label;
          if (!existing.datatype && dtype) existing.datatype = dtype;
        } else {
          propMap.set(pid, {
            id: pid,
            label,
            datatype: dtype,
            count: 0,
          });
        }
      }
      seen.forEach((pid) => {
        const item = propMap.get(pid);
        if (item) item.count += 1;
      });
    }
    const items = Array.from(propMap.values());
    items.sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      const anum = parseInt(a.id.replace(/\D+/g, ""), 10);
      const bnum = parseInt(b.id.replace(/\D+/g, ""), 10);
      if (Number.isFinite(anum) && Number.isFinite(bnum)) {
        return anum - bnum;
      }
      return a.id.localeCompare(b.id);
    });
    cache.set(classId, { ts: now, items });
    return items;
  }

  function handleRecommendedPropertyClick(item, button) {
    if (!item || !button || !propRecommendList) return;
    const pid = (item.id || "").trim();
    if (!pid) return;
    propRecommendActiveId = pid;
    const buttons = propRecommendList.querySelectorAll(".prop-rec-item");
    buttons.forEach((el) => {
      el.classList.toggle("active", el === button);
    });
    try {
      schemaPropId.value = pid;
    } catch {}
    if (propCreateMsg) {
      const label = item.label || pid;
      const hasClass = !!getActiveClassIdForRecommendations();
      propCreateMsg.textContent = hasClass
        ? `推荐属性已选择：${label} (${pid})，点击“添加属性”加入分类`
        : `推荐属性：${label} (${pid})，请先选择分类后再添加`;
    }
  }

  function renderPropertyRecommendations(items) {
    if (!propRecommendContainer || !propRecommendStatus || !propRecommendList)
      return;
    propRecommendActiveId = "";
    propRecommendContainer.style.display = "flex";
    propRecommendList.innerHTML = "";
    const sourceItems = Array.isArray(items) ? items : [];
    const existingSchemaItems = getCurrentSchemaItems();
    const existingPropIds = new Set(
      existingSchemaItems
        .map((it) => canonicalizePropertyId(it?.id || it?.property || ""))
        .filter(Boolean),
    );
    const filteredItems = sourceItems.filter((item) => {
      const pid = canonicalizePropertyId(item?.id || "");
      return pid && !existingPropIds.has(pid);
    });
    if (!filteredItems.length) {
      propRecommendStatus.textContent = sourceItems.length
        ? "推荐属性已全部存在于当前模型"
        : "暂无推荐属性";
      return;
    }
    const frag = document.createDocumentFragment();
    const maxItems = Math.min(filteredItems.length, 20);
    for (let i = 0; i < maxItems; i += 1) {
      const item = filteredItems[i];
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "prop-rec-item";
      const labelSpan = document.createElement("span");
      labelSpan.className = "prop-rec-label";
      labelSpan.textContent = item.label || item.id || "";
      const idSpan = document.createElement("span");
      idSpan.className = "prop-rec-id";
      idSpan.textContent = `(${item.id})`;
      btn.appendChild(labelSpan);
      btn.appendChild(idSpan);
      if (item.count) {
        const countSpan = document.createElement("span");
        countSpan.className = "prop-rec-count";
        countSpan.textContent = `×${item.count}`;
        btn.appendChild(countSpan);
      }
      btn.title = item.count
        ? `约 ${item.count} 个同类实体使用`
        : `属性 ${item.id}`;
      btn.addEventListener("click", () =>
        handleRecommendedPropertyClick(item, btn),
      );
      frag.appendChild(btn);
    }
    propRecommendList.appendChild(frag);
    const skipped = sourceItems.length - filteredItems.length;
    if (skipped > 0) {
      propRecommendStatus.textContent = `已排除 ${skipped} 项已存在属性`;
    } else {
      propRecommendStatus.textContent = "";
    }
  }

  async function updatePropertyRecommendations(options = {}) {
    if (!propRecommendContainer || !propRecommendStatus || !propRecommendList)
      return;
    const classId = getActiveClassIdForRecommendations();
    if (!classId) {
      propRecommendContainer.style.display = "flex";
      propRecommendList.innerHTML = "";
      propRecommendStatus.textContent = "请选择分类以查看推荐属性";
      propRecommendActiveId = "";
      return;
    }
    const token = ++propRecommendRequestSeq;
    propRecommendContainer.style.display = "flex";
    propRecommendStatus.textContent = "加载推荐属性…";
    propRecommendList.innerHTML = "";
    try {
      const items = await fetchPropertyRecommendationsForClass(
        classId,
        options,
      );
      if (token !== propRecommendRequestSeq) return;
      renderPropertyRecommendations(items);
    } catch (err) {
      if (token !== propRecommendRequestSeq) return;
      console.error("updatePropertyRecommendations failed", err);
      propRecommendStatus.textContent = "推荐加载失败";
    }
  }

  if (btnPropRecommendRefresh) {
    btnPropRecommendRefresh.addEventListener("click", () =>
      updatePropertyRecommendations({ force: true }),
    );
  }

  window.kbSelectedClassId = null;
  window.kbClasses = [];
  window.kbEntityClasses = [];
  window.kbSchemaByClassId = Object.create(null);
  window.kbSelectedSchemaPropId = "";
  window.kbSelectedSchemaPropLabel = "";
  window.kbSchemaRemovalSelection = new Set();
  window.kbSchemaRemovalLastIndex = -1;
  window.kbClassCreateSubmitting = false;
  // Collapsed state for class tree
  if (!window.kbCollapsedClassIds) {
    window.kbCollapsedClassIds = new Set();
    window.kbClassTreeInitiallyCollapsed = true;
  }
  const CLASS_TREE_ROOT_KEY = "__kb_class_root__";
  window.kbClassMeta = null;
  window.kbClassDragSourceId = null;

  async function apiGet(url) {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    return await resp.json();
  }

  function appendCurrentDbToUrl(url) {
    if (typeof window.appendCurrentDbParam === "function") {
      const scopedUrl = window.appendCurrentDbParam(url);
      if (scopedUrl instanceof URL) return scopedUrl;
    }
    return url;
  }

  function openClassModal() {
    if (!classModal) return;
    if (classModalTitle) classModalTitle.textContent = "新增分类";
    if (classForm) {
      try {
        classForm.dataset.mode = "create";
        classForm.dataset.parentId = "";
      } catch {}
    }
    if (clsNameInput) clsNameInput.value = "";
    if (clsDescInput) clsDescInput.value = "";
    classModal.style.display = "flex";
    classModal.setAttribute("aria-hidden", "false");
    try {
      clsNameInput?.focus();
      clsNameInput?.select?.();
    } catch {}
  }

  function closeClassModal() {
    if (!classModal) return;
    classModal.style.display = "none";
    classModal.setAttribute("aria-hidden", "true");
  }

  async function createClass(payload) {
    const url = appendCurrentDbToUrl(
      new URL("/api/kb/classes", window.location.origin),
    );
    const resp = await fetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {}),
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    return await resp.json();
  }

  async function loadClasses(q) {
    try {
      const url = appendCurrentDbToUrl(
        new URL("/api/kb/classes", window.location.origin),
      );
      if (q && q.trim()) url.searchParams.set("q", q.trim());
      const data = await apiGet(url.toString());
      // API returns array directly now, but handle {items: []} just in case
      let items = Array.isArray(data)
        ? data
        : Array.isArray(data?.items)
          ? data.items
          : [];

      items = items.map((it, index) => {
        const rawOrder = Number(it?.sort_order);
        return {
          ...it,
          parent: it?.parent ?? null,
          sort_order: Number.isFinite(rawOrder) ? rawOrder : index + 1,
        };
      });
      window.kbClasses = items;
      renderClassTree(items);
    } catch (e) {
      console.error("loadClasses", e);
      clsTree.innerHTML = '<div class="muted">加载失败</div>';
    }
  }

  function buildClassTree(items) {
    const byId = new Map();
    const parentMap = new Map();
    const childrenMap = new Map();
    const sortFn = (a, b) => {
      const ao = Number.isFinite(a.sort_order)
        ? a.sort_order
        : Number.MAX_SAFE_INTEGER;
      const bo = Number.isFinite(b.sort_order)
        ? b.sort_order
        : Number.MAX_SAFE_INTEGER;
      if (ao !== bo) return ao - bo;
      const al = (a.label || a.name || a.id || "").toString();
      const bl = (b.label || b.name || b.id || "").toString();
      return al.localeCompare(bl, undefined, { numeric: true });
    };
    const ensureChildrenList = (key) => {
      if (!childrenMap.has(key)) childrenMap.set(key, []);
      return childrenMap.get(key);
    };
    (Array.isArray(items) ? items : []).forEach((it, index) => {
      const orderVal = Number(it?.sort_order);
      const node = {
        ...it,
        children: [],
        parent: it?.parent ?? null,
        sort_order: Number.isFinite(orderVal) ? orderVal : index + 1,
      };
      byId.set(node.id, node);
      parentMap.set(node.id, node.parent);
    });
    byId.forEach((node) => {
      const parentId =
        node.parent && byId.has(node.parent) ? node.parent : null;
      node.parent = parentId;
      const key = parentId ?? CLASS_TREE_ROOT_KEY;
      ensureChildrenList(key).push(node);
    });
    if (!childrenMap.has(CLASS_TREE_ROOT_KEY)) {
      childrenMap.set(CLASS_TREE_ROOT_KEY, []);
    }
    childrenMap.forEach((list, key) => {
      list.sort(sortFn);
      if (key !== CLASS_TREE_ROOT_KEY && byId.has(key)) {
        byId.get(key).children = list;
      }
    });
    const roots = childrenMap.get(CLASS_TREE_ROOT_KEY) || [];
    if (window.kbClassTreeInitiallyCollapsed) {
      try {
        const stack = Array.isArray(roots) ? roots.slice() : [];
        while (stack.length) {
          const node = stack.pop();
          if (!node || !node.id) continue;
          window.kbCollapsedClassIds.add(node.id);
          if (Array.isArray(node.children)) stack.push(...node.children);
        }
      } catch {}
      window.kbClassTreeInitiallyCollapsed = false;
    }
    try {
      window.kbClassMeta = {
        byId,
        parentMap,
        childrenMap: new Map(
          Array.from(childrenMap.entries()).map(([key, list]) => [
            key,
            list.map((node) => node.id),
          ]),
        ),
        rootKey: CLASS_TREE_ROOT_KEY,
      };
    } catch {}
    return roots;
  }

  function renderClassTree(items, options = {}) {
    if (!clsTree) return;
    const dragEnabled =
      options.dragEnabled !== undefined
        ? !!options.dragEnabled
        : !(clsSearch?.value || "").trim();
    const roots = buildClassTree(items);
    clsTree.innerHTML = "";
    const frag = document.createDocumentFragment();
    clsTree.dataset.dragEnabled = dragEnabled ? "true" : "false";
    clsTree.classList.toggle("drag-disabled", !dragEnabled);

    function renderNode(n, depth = 0) {
      const row = document.createElement("div");
      row.className = "cls-item";
      row.style.display = "flex";
      row.style.alignItems = "center";
      row.style.gap = "6px";
      row.style.padding = "4px 6px";
      row.style.cursor = "pointer";
      row.setAttribute("data-id", n.id);
      row.setAttribute("data-parent-id", n.parent || "");
      const indent = document.createElement("span");
      indent.style.display = "inline-block";
      indent.style.width = depth * 14 + "px";
      const toggle = document.createElement("span");
      const hasChildren = Array.isArray(n.children) && n.children.length > 0;
      const collapsed = hasChildren && window.kbCollapsedClassIds.has(n.id);
      toggle.textContent = hasChildren ? (collapsed ? "▶" : "▼") : "·";
      toggle.title = hasChildren ? (collapsed ? "展开" : "收起") : "";
      toggle.style.width = "14px";
      toggle.style.display = "inline-block";
      toggle.style.color = "var(--muted)";
      toggle.style.cursor = hasChildren ? "pointer" : "default";
      toggle.addEventListener("click", (e) => {
        e.stopPropagation();
        if (!hasChildren) return;
        if (window.kbCollapsedClassIds.has(n.id)) {
          window.kbCollapsedClassIds.delete(n.id);
        } else {
          window.kbCollapsedClassIds.add(n.id);
        }
        renderClassTree(window.kbClasses || []);
      });
      const labelWrap = document.createElement("span");
      labelWrap.style.display = "inline-flex";
      labelWrap.style.alignItems = "center";
      labelWrap.style.gap = "4px";

      const name = document.createElement("span");
      name.textContent = n.label || n.id;

      const count = document.createElement("span");
      count.className = "cls-count-pill";
      const countValue = Number.isFinite(n.instance_count)
        ? n.instance_count
        : Number(n.instance_count);
      count.textContent = Number.isFinite(countValue)
        ? countValue.toString()
        : "0";
      count.title = "节点数量";

      labelWrap.appendChild(name);
      labelWrap.appendChild(count);

      // 分类图标（如果有）
      const classIcon = document.createElement("span");
      classIcon.style.display = "inline-block";
      classIcon.style.width = "16px";
      classIcon.style.height = "16px";
      classIcon.style.marginRight = "4px";
      classIcon.style.flexShrink = "0";
      if (n.image) {
        classIcon.style.backgroundImage = `url(${n.image})`;
        classIcon.style.backgroundSize = "cover";
        classIcon.style.backgroundPosition = "center";
        classIcon.style.borderRadius = "3px";
      } else {
        classIcon.style.display = "none";
      }

      const colorDot = document.createElement("span");
      colorDot.style.display = "inline-block";
      colorDot.style.width = "8px";
      colorDot.style.height = "8px";
      colorDot.style.borderRadius = "50%";
      colorDot.style.backgroundColor = n.color || "transparent";
      colorDot.style.marginRight = "4px";
      if (n.color) {
        colorDot.style.border = "1px solid rgba(0,0,0,0.1)";
      }

      row.appendChild(indent);
      row.appendChild(toggle);
      row.appendChild(classIcon);
      row.appendChild(colorDot);
      row.appendChild(labelWrap);
      row.addEventListener("click", () => {
        const btnDel = document.getElementById("btnClsDelete");
        const picker = document.getElementById("clsColorPicker");
        const btnImage = document.getElementById("btnClsImage");
        const wasSelected = window.kbSelectedClassId === n.id;
        if (wasSelected) {
          window.kbSelectedClassId = null;
          row.style.background = "";
          const schemaList = document.getElementById("schemaList");
          if (schemaList) schemaList.innerHTML = "";
          if (btnDel) btnDel.style.display = "none";
          if (picker) picker.style.display = "none";
          if (btnImage) btnImage.style.display = "none";
        } else {
          document
            .querySelectorAll("#clsTree .cls-item")
            .forEach((el) => (el.style.background = ""));
          row.style.background = "rgba(79,70,229,0.10)";
          window.kbSelectedClassId = n.id;
          if (btnDel) btnDel.style.display = "inline-flex";
          if (picker) {
            picker.style.display = "inline-block";
            picker.value = n.color || "#000000";
          }
          if (btnImage) btnImage.style.display = "inline-flex";
          if (window.kbSchemaRemovalSelection)
            window.kbSchemaRemovalSelection.clear();
          window.kbSchemaRemovalLastIndex = -1;
          updateSchemaRemoveButtonState();
          try {
            updatePropertyRecommendations();
          } catch {}
          // load schema for this class
          try {
            loadClassSchema(n.id);
          } catch {}
        }
        // 单击只选中，不触发筛选
      });
      row.addEventListener("dblclick", () => {
        // 双击切换筛选
        const label = n.label || n.id;
        const isCurrentlyFiltered = window.kbFilteredClassId === n.id;
        if (isCurrentlyFiltered) {
          // 取消筛选
          window.kbFilteredClassId = null;
          const clearPromise = loadTablePage({
            classId: "",
            classLabel: "",
            resetPage: true,
          });
          if (clearPromise && typeof clearPromise.catch === "function") {
            clearPromise.catch((err) => console.error("取消分类筛选失败", err));
          }
        } else {
          // 筛选该分类
          window.kbFilteredClassId = n.id;
          const fetchPromise = loadInstancesForClass(n.id, {
            classLabel: label,
            resetPage: true,
          });
          if (fetchPromise && typeof fetchPromise.catch === "function") {
            fetchPromise.catch((err) =>
              console.error("loadInstancesForClass failed", err),
            );
          }
        }
        // 重新渲染分类树以更新筛选标记
        renderClassTree(window.kbClasses || []);
      });
      // restore selection highlight
      if (window.kbSelectedClassId === n.id) {
        row.style.background = "rgba(79,70,229,0.10)";
      }
      // 添加筛选标记
      if (window.kbFilteredClassId === n.id) {
        const filterBadge = document.createElement("span");
        filterBadge.className = "cls-filter-badge";
        filterBadge.innerHTML =
          '<i class="fa fa-filter" style="font-size:10px;"></i>';
        filterBadge.title = "已筛选此分类（双击取消）";
        filterBadge.style.cssText =
          "margin-left:auto;color:var(--accent,#4f46e5);display:inline-flex;align-items:center;padding:2px 6px;background:rgba(79,70,229,0.12);border-radius:4px;font-size:11px;gap:4px;";
        row.appendChild(filterBadge);
        row.style.background = "rgba(79,70,229,0.08)";
      }
      attachClassTreeDragHandlers(row, n, dragEnabled);
      frag.appendChild(row);
      if (hasChildren && !window.kbCollapsedClassIds.has(n.id)) {
        n.children.forEach((c) => renderNode(c, depth + 1));
      }
    }

    roots.forEach((r) => renderNode(r, 0));
    clsTree.appendChild(frag);

    const btnDel = document.getElementById("btnClsDelete");
    const picker = document.getElementById("clsColorPicker");
    const btnImage = document.getElementById("btnClsImage");
    if (btnDel) {
      btnDel.style.display = window.kbSelectedClassId ? "inline-flex" : "none";
    }
    if (picker) {
      picker.style.display = window.kbSelectedClassId ? "inline-block" : "none";
      if (window.kbSelectedClassId) {
        const found = window.kbClasses.find(
          (c) => c.id === window.kbSelectedClassId,
        );
        if (found) picker.value = found.color || "#000000";
      }
    }
    if (btnImage) {
      btnImage.style.display = window.kbSelectedClassId
        ? "inline-flex"
        : "none";
    }
  }

  function expandClassAncestors(classId) {
    const targetId = (classId || "").trim();
    if (!targetId) return;
    try {
      const classes = Array.isArray(window.kbClasses) ? window.kbClasses : [];
      const byId = new Map(
        classes.map((item) => [String(item.id || "").trim(), item]),
      );
      let current = byId.get(targetId);
      while (current) {
        if (current.id) window.kbCollapsedClassIds.delete(current.id);
        const parentId = String(current.parent || "").trim();
        if (!parentId) break;
        window.kbCollapsedClassIds.delete(parentId);
        current = byId.get(parentId);
      }
    } catch (err) {
      console.error("expandClassAncestors", err);
    }
  }

  function revealSelectedClassInTree() {
    try {
      const selectedId = (window.kbSelectedClassId || "").trim();
      if (!selectedId || !clsTree) return;
      const selectedEl = clsTree.querySelector(
        `.cls-item[data-id="${CSS.escape(selectedId)}"]`,
      );
      if (selectedEl && typeof selectedEl.scrollIntoView === "function") {
        selectedEl.scrollIntoView({ block: "nearest" });
      }
    } catch {}
  }

  function mergeCreatedClassIntoLocalList(created, fallback = {}) {
    if (!created || !created.id) return;
    const current = Array.isArray(window.kbClasses)
      ? window.kbClasses.slice()
      : [];
    const createdId = String(created.id || "").trim();
    if (!createdId) return;
    const normalized = {
      id: createdId,
      name: created.name || fallback.name || createdId,
      label: created.label || created.name || fallback.name || createdId,
      description: created.description || fallback.description || "",
      parent:
        created.parent !== undefined
          ? created.parent
          : fallback.parent_id || null,
      color:
        created.color !== undefined ? created.color : fallback.color || null,
      sort_order: Number.isFinite(Number(created.sort_order))
        ? Number(created.sort_order)
        : current.length + 1,
      instance_count: Number.isFinite(Number(created.instance_count))
        ? Number(created.instance_count)
        : 0,
    };
    const index = current.findIndex(
      (item) => String(item?.id || "").trim() === createdId,
    );
    if (index >= 0) current[index] = { ...current[index], ...normalized };
    else current.push(normalized);
    window.kbClasses = current;
  }

  function attachClassTreeDragHandlers(row, node, enabled) {
    if (!row) return;
    if (!enabled) {
      row.removeAttribute("draggable");
      return;
    }
    row.setAttribute("draggable", "true");
    row.addEventListener("dragstart", (event) => {
      window.kbClassDragSourceId = node.id;
      try {
        event.dataTransfer?.setData("text/plain", node.id);
        event.dataTransfer?.setDragImage?.(row, 20, 10);
        if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
      } catch {}
      row.classList.add("dragging");
      clearClassDropIndicators();
    });
    row.addEventListener("dragend", () => {
      row.classList.remove("dragging");
      window.kbClassDragSourceId = null;
      clearClassDropIndicators();
    });
    row.addEventListener("dragover", (event) => {
      if (!window.kbClassDragSourceId || window.kbClassDragSourceId === node.id)
        return;
      event.preventDefault();
      const action = computeClassDropAction(event, row);
      setClassDropIndicator(row, action);
      try {
        if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
      } catch {}
    });
    row.addEventListener("dragleave", () => {
      clearClassDropIndicator(row);
    });
    row.addEventListener("drop", async (event) => {
      if (!window.kbClassDragSourceId || window.kbClassDragSourceId === node.id)
        return;
      event.preventDefault();
      const action = computeClassDropAction(event, row);
      clearClassDropIndicators();
      const sourceId = window.kbClassDragSourceId;
      window.kbClassDragSourceId = null;
      await handleClassDrop(sourceId, node.id, action);
    });
  }

  function computeClassDropAction(event, row) {
    const rect = row.getBoundingClientRect();
    const offsetY = event.clientY - rect.top;
    const ratio = rect.height ? offsetY / rect.height : 0.5;
    if (ratio < 0.33) return "before";
    if (ratio > 0.66) return "after";
    return "inside";
  }

  function setClassDropIndicator(row, action) {
    if (!row) return;
    row.classList.remove("drop-before", "drop-after", "drop-inside");
    if (action === "before") row.classList.add("drop-before");
    else if (action === "after") row.classList.add("drop-after");
    else row.classList.add("drop-inside");
  }

  function clearClassDropIndicator(row) {
    if (!row) return;
    row.classList.remove("drop-before", "drop-after", "drop-inside");
  }

  function clearClassDropIndicators() {
    document
      .querySelectorAll(
        "#clsTree .cls-item.drop-before, #clsTree .cls-item.drop-after, #clsTree .cls-item.drop-inside",
      )
      .forEach((el) => clearClassDropIndicator(el));
  }

  function getClassChildrenIds(parentId) {
    const meta = window.kbClassMeta;
    if (!meta) return [];
    const key = parentId ?? meta.rootKey;
    const list = meta.childrenMap?.get(key) || [];
    return Array.isArray(list) ? list.slice() : [];
  }

  function isClassAncestor(ancestorId, nodeId) {
    if (!ancestorId || !nodeId || !window.kbClassMeta) return false;
    let current = window.kbClassMeta.parentMap?.get(nodeId) ?? null;
    while (current) {
      if (current === ancestorId) return true;
      current = window.kbClassMeta.parentMap?.get(current) ?? null;
    }
    return false;
  }

  async function handleClassDrop(sourceId, targetId, action) {
    const meta = window.kbClassMeta;
    if (!meta) return;
    if (!sourceId || !targetId || sourceId === targetId) return;
    if (!meta.byId?.has(sourceId) || !meta.byId?.has(targetId)) return;
    const dropAction = action || "after";
    if (dropAction === "inside" && isClassAncestor(sourceId, targetId)) {
      setStatus(false, "无法将分类移动到其子节点下");
      return;
    }
    const sourceParent = meta.parentMap?.get(sourceId) ?? null;
    let newParent = sourceParent;
    if (dropAction === "inside") newParent = targetId;
    else newParent = meta.parentMap?.get(targetId) ?? null;
    const currentOrder = getClassChildrenIds(sourceParent);
    const rootKey = meta.rootKey;
    const oldKey = sourceParent ?? rootKey;
    const newKey = newParent ?? rootKey;
    const siblingsOld = getClassChildrenIds(sourceParent);
    let siblingsNew =
      newKey === oldKey ? siblingsOld : getClassChildrenIds(newParent);
    const removeId = (arr, id) => {
      const idx = arr.indexOf(id);
      if (idx >= 0) arr.splice(idx, 1);
    };
    removeId(siblingsOld, sourceId);
    if (siblingsNew !== siblingsOld) removeId(siblingsNew, sourceId);
    let insertIndex = siblingsNew.length;
    if (dropAction === "before" || dropAction === "after") {
      const targetIndex = siblingsNew.indexOf(targetId);
      insertIndex = targetIndex >= 0 ? targetIndex : siblingsNew.length;
      if (dropAction === "after") insertIndex += 1;
    }
    siblingsNew.splice(insertIndex, 0, sourceId);
    if (
      sourceParent === newParent &&
      currentOrder.length === siblingsNew.length &&
      currentOrder.every((id, idx) => id === siblingsNew[idx])
    ) {
      return;
    }
    const updates = [];
    const pushUpdates = (ids, parent) => {
      ids.forEach((id, idx) => {
        updates.push({
          id,
          parent_id: parent,
          sort_order: (idx + 1) * 100,
        });
      });
    };
    if (newKey === oldKey) pushUpdates(siblingsNew, newParent);
    else {
      pushUpdates(siblingsOld, sourceParent);
      pushUpdates(siblingsNew, newParent);
    }
    if (!updates.length) return;
    setStatus(true, "正在调整分类顺序…");
    try {
      const url = appendCurrentDbToUrl(
        new URL("/api/kb/classes/reorder", window.location.origin),
      );
      const resp = await fetch(url.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ updates }),
      });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      await resp.json();
      setStatus(false, "分类结构已更新");
      const searchTerm = clsSearch ? (clsSearch.value || "").trim() : "";
      await loadClasses(searchTerm);
    } catch (err) {
      console.error("handleClassDrop", err);
      setStatus(false, "分类调整失败");
    }
  }

  async function removeEntityClass(entityId, classId) {
    try {
      const url = new URL("/api/kb/entity/class", window.location.origin);
      url.searchParams.set("entity_id", entityId);
      url.searchParams.set("class_id", classId);
      const resp = await fetch(url, { method: "DELETE" });
      if (!resp.ok) throw new Error("Failed to remove class");
    } catch (e) {
      console.error(e);
      alert("移除分类失败");
    }
  }

  async function loadEntityClass(entityId) {
    if (!entityId) {
      clsForEntity.textContent = "未选择实体";
      try {
        updatePropertyRecommendations();
      } catch {}
      return;
    }
    // 自动加 entity/ 前缀（如果没有）
    const fullId = entityId.startsWith("entity/")
      ? entityId
      : "entity/" + entityId;
    try {
      const url = appendCurrentDbToUrl(
        new URL("/api/kb/entity/class", window.location.origin),
      );
      url.searchParams.set("id", fullId);
      const data = await apiGet(url.toString());
      const items = Array.isArray(data?.items) ? data.items : [];
      window.kbEntityClasses = items;
      if (!items.length) {
        clsForEntity.textContent = "当前实体未设置分类";
        try {
          updatePropertyRecommendations();
        } catch {}
        return;
      }
      clsForEntity.innerHTML = "";
      const titleDiv = document.createElement("div");
      titleDiv.textContent = "当前实体分类：";
      titleDiv.style.marginBottom = "6px";
      clsForEntity.appendChild(titleDiv);

      const tagsDiv = document.createElement("div");
      tagsDiv.style.display = "flex";
      tagsDiv.style.flexWrap = "wrap";
      tagsDiv.style.gap = "4px";

      items.forEach((cls) => {
        const tag = document.createElement("span");
        tag.className = "tag";
        tag.style.display = "inline-flex";
        tag.style.alignItems = "center";
        tag.style.gap = "4px";
        tag.style.padding = "2px 6px";
        tag.style.borderRadius = "4px";
        tag.style.fontSize = "12px";
        tag.style.backgroundColor = cls.color ? cls.color + "20" : "#f1f5f9";
        tag.style.color = cls.color || "#475569";
        tag.style.border = `1px solid ${cls.color ? cls.color + "40" : "#e2e8f0"}`;

        const label = document.createElement("span");
        label.textContent = cls.label || cls.name || "未命名分类";
        tag.appendChild(label);

        const delBtn = document.createElement("i");
        delBtn.className = "fa-solid fa-xmark";
        delBtn.style.cursor = "pointer";
        delBtn.style.opacity = "0.6";
        delBtn.title = "移除分类";
        delBtn.onmouseover = () => (delBtn.style.opacity = "1");
        delBtn.onmouseout = () => (delBtn.style.opacity = "0.6");
        delBtn.onclick = async (e) => {
          e.stopPropagation();
          if (confirm(`确认移除分类“${cls.label}”吗？`)) {
            await removeEntityClass(fullId, cls.id);
            loadEntityClass(fullId); // Refresh this panel
            // Also refresh detail panel if open
            try {
              if (
                document.getElementById("detailPanel").style.display !== "none"
              ) {
                showNodeDetailInline(fullId);
              }
            } catch {}
          }
        };
        tag.appendChild(delBtn);
        tagsDiv.appendChild(tag);
      });
      clsForEntity.appendChild(tagsDiv);

      // Auto-select the first class of the entity
      try {
        if (items[0]?.id) {
          const targetId = items[0].id;
          window.kbSelectedClassId = targetId;

          // Update tree UI highlight
          const treeItems = document.querySelectorAll("#clsTree .cls-item");
          treeItems.forEach((el) => {
            if (el.getAttribute("data-id") === targetId) {
              el.style.background = "rgba(79,70,229,0.10)";
              try {
                el.scrollIntoView({ block: "nearest" });
              } catch {}
            } else {
              el.style.background = "";
            }
          });

          // Show delete button
          const btnDel = document.getElementById("btnClsDelete");
          if (btnDel) btnDel.style.display = "inline-flex";

          // Load schema
          await loadClassSchema(targetId);
        }
      } catch (e) {
        console.error(e);
      }

      try {
        refreshAttrPropDatalist();
      } catch {}
      try {
        // refresh the class/new-type select with the entity's types
        refreshClsNewOptions(items);
      } catch {}
      try {
        updatePropertyRecommendations();
      } catch {}
    } catch (e) {
      console.error("loadEntityClass", e);
      clsForEntity.textContent = "加载实体分类失败";
      try {
        updatePropertyRecommendations();
      } catch {}
    }
  }

  async function setEntityClass(entityId, classId) {
    const url = appendCurrentDbToUrl(
      new URL("/api/kb/entity/class", window.location.origin),
    );
    const resp = await fetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ entity_id: entityId, class_id: classId }),
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    return await resp.json();
  }

  function getParentClassId(classId) {
    if (!classId) return null;
    const meta = window.kbClassMeta;
    const parentMap = meta?.parentMap;
    if (parentMap && typeof parentMap.get === "function") {
      const pid = parentMap.get(classId);
      return pid ?? null;
    }
    if (Array.isArray(window.kbClasses)) {
      const found = window.kbClasses.find((cls) => cls && cls.id === classId);
      if (found && found.parent) return found.parent;
    }
    return null;
  }

  function getClassAncestryIds(classId) {
    const chain = [];
    const visited = new Set();
    let current = classId;
    while (current && !visited.has(current)) {
      chain.push(current);
      visited.add(current);
      current = getParentClassId(current);
    }
    return chain;
  }

  async function autoAssignSelectedClassToNode(nodeId) {
    const baseClassId = (window.kbSelectedClassId || "").trim();
    const entityId = (nodeId ?? "").toString().trim();
    if (!baseClassId || !entityId) return;
    const targets = getClassAncestryIds(baseClassId);
    if (!targets.length) return;
    for (const cid of targets) {
      try {
        await setEntityClass(entityId, cid);
      } catch (err) {
        console.error("自动设置分类失败", err);
      }
    }
  }

  async function clearEntityClass(entityId, classId) {
    const url = appendCurrentDbToUrl(
      new URL("/api/kb/entity/class", window.location.origin),
    );
    url.searchParams.set("entity_id", entityId);
    if (classId) url.searchParams.set("class_id", classId);
    const resp = await fetch(url.toString(), { method: "DELETE" });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    return await resp.json();
  }

  function updateSchemaRemoveButtonState() {
    const cid = window.kbSelectedClassId;
    const selectionCount = window.kbSchemaRemovalSelection
      ? window.kbSchemaRemovalSelection.size
      : 0;

    if (btnRemoveSchemaSelected) {
      btnRemoveSchemaSelected.disabled = !cid || selectionCount === 0;
      if (selectionCount > 0) {
        btnRemoveSchemaSelected.title = `移除已标记属性（${selectionCount}）`;
        btnRemoveSchemaSelected.setAttribute(
          "aria-label",
          `移除已标记属性（${selectionCount}）`,
        );
      } else {
        btnRemoveSchemaSelected.title = "移除已标记属性";
        btnRemoveSchemaSelected.setAttribute("aria-label", "移除已标记属性");
      }
    }

    if (btnAddSchema) {
      let isQualifierMode = false;
      if (selectionCount === 1) {
        const selectedId = Array.from(window.kbSchemaRemovalSelection)[0];
        if (!selectedId.includes(":")) {
          isQualifierMode = true;
        }
      }

      if (isQualifierMode) {
        btnAddSchema.title = "为选中属性添加限定";
        btnAddSchema.setAttribute("aria-label", "为选中属性添加限定");
        btnAddSchema.innerHTML = '<i class="fa-solid fa-filter"></i>'; // Use filter icon for restriction/qualifier
      } else {
        btnAddSchema.title = "将属性加入模型";
        btnAddSchema.setAttribute("aria-label", "将属性加入模型");
        btnAddSchema.innerHTML = '<i class="fa-solid fa-circle-plus"></i>';
      }
    }
  }

  async function loadClassSchema(classId) {
    window.kbSchemaRemovalLastIndex = -1;
    if (!classId) {
      schemaList.innerHTML = '<div class="muted">未选择分类</div>';
      if (window.kbSchemaRemovalSelection)
        window.kbSchemaRemovalSelection.clear();
      window.kbSchemaRemovalLastIndex = -1;
      updateSchemaRemoveButtonState();
      return;
    }
    try {
      const url = appendCurrentDbToUrl(
        new URL("/api/kb/class/schema", window.location.origin),
      );
      url.searchParams.set("class_id", classId);
      const data = await apiGet(url.toString());
      const items = Array.isArray(data?.items) ? data.items : [];
      if (!window.kbSchemaRemovalSelection)
        window.kbSchemaRemovalSelection = new Set();
      const removalSelection = window.kbSchemaRemovalSelection;
      // cache per-class schema for attribute dropdown
      try {
        if (!window.kbSchemaByClassId)
          window.kbSchemaByClassId = Object.create(null);
        window.kbSchemaByClassId[classId] = items;
      } catch {}
      if (!items.length) {
        schemaList.innerHTML = '<div class="muted">暂无属性模型</div>';
        removalSelection.clear();
        window.kbSchemaRemovalLastIndex = -1;
        updateSchemaRemoveButtonState();
        return;
      }
      window.kbSchemaRemovalLastIndex = -1;

      // Clean up selection (remove IDs that are no longer present)
      // We need to collect all valid IDs (including qualifiers)
      const available = new Set();
      items.forEach((it) => {
        available.add(it.id);
        if (it.qualifiers) {
          it.qualifiers.forEach((q) => available.add(`${it.id}:${q.id}`));
        }
      });
      removalSelection.forEach((pid) => {
        if (!available.has(pid)) removalSelection.delete(pid);
      });

      const frag = document.createDocumentFragment();

      const renderItem = (it, index, parentId = null) => {
        const row = document.createElement("div");
        row.className = "schema-item";
        if (parentId) {
          row.classList.add("schema-qualifier");
          row.style.paddingLeft = "24px";
          row.style.fontSize = "0.95em";
          row.style.borderLeft = "2px solid var(--border)";
        }
        if (!it.is_local && !parentId) {
          // Only mark inherited for top-level properties
          row.classList.add("inherited");
          row.style.opacity = "0.8";
        }

        const rowId = parentId ? `${parentId}:${it.id}` : it.id;
        row.setAttribute("data-pid", rowId);
        row.dataset.index = String(index);

        const uiType = pickUiDatatype(it) || "";
        if (uiType) row.setAttribute("data-dtype", uiType);

        const left = document.createElement("div");
        left.className = "left";
        const labelEl = document.createElement("div");
        labelEl.className = "schema-label";

        let labelHtml = it.label || it.id;
        if (!parentId && !it.is_local) {
          labelHtml +=
            ' <span style="font-size:10px;color:var(--muted);border:1px solid var(--border);border-radius:4px;padding:0 2px;">继承</span>';
        }
        if (parentId) {
          labelHtml = `<span class="muted" style="margin-right:4px;">限定</span> ${labelHtml}`;
        }
        labelEl.innerHTML = labelHtml;
        left.appendChild(labelEl);

        const hintText = [
          it.description_zh,
          it.desc_zh,
          it.description,
          it.desc,
          it.comment,
        ].find((val) => typeof val === "string" && val.trim());
        if (hintText) {
          const hintEl = document.createElement("div");
          hintEl.className = "schema-hint";
          hintEl.textContent = hintText.trim();
          left.appendChild(hintEl);
        }

        const tooltipParts = [it.label || it.id];
        if (it.id) tooltipParts.push(`ID: ${it.id}`);
        if (uiType) tooltipParts.push(`类型: ${uiType}`);
        if (!parentId && !it.is_local) tooltipParts.push("继承属性");
        tooltipParts.push("点击选择");
        left.title = tooltipParts.join(" · ");
        row.title = left.title;

        const setRemovalState = (selected, mutateSet = true) => {
          if (mutateSet) {
            if (selected) {
              removalSelection.add(rowId);
            } else {
              removalSelection.delete(rowId);
            }
          }
          row.classList.toggle("removal-selected", selected);
        };
        setRemovalState(removalSelection.has(rowId), false);

        const selectSchema = () => {
          // If it's a qualifier, we might want to select the parent property for attribute filling?
          // Or just select the qualifier property itself?
          // For now, let's select the property ID.
          setSelectedSchemaProp(it.id, it.label || it.id);
          try {
            const currentType = pickUiDatatype(it) || uiType || "string";
            if (typeof window.updateDatatypeUI === "function") {
              window.updateDatatypeUI(
                currentType,
                it?.valuetype || it?.datavalue_type || "",
              );
            }
            const attrTypeEl = document.getElementById("attrType");
            if (attrTypeEl) attrTypeEl.value = currentType;
          } catch {}
        };

        left.addEventListener("click", (evt) => {
          evt.stopPropagation();
          if (evt.ctrlKey || evt.metaKey) {
            evt.preventDefault();
            const next = !removalSelection.has(rowId);
            setRemovalState(next);
            window.kbSchemaRemovalLastIndex = index;
            updateSchemaRemoveButtonState();
            return;
          }
          if (evt.shiftKey) {
            // Shift selection logic (simplified)
            evt.preventDefault();
            // ... (omitted for brevity, can be added if needed)
            return;
          }

          // Single click: select this one, deselect others
          removalSelection.clear();
          // Also clear visual state of all rows
          const allRows = schemaList.querySelectorAll(".schema-item");
          allRows.forEach((r) => r.classList.remove("removal-selected"));

          setRemovalState(true);
          window.kbSchemaRemovalLastIndex = index;
          updateSchemaRemoveButtonState();

          selectSchema();
        });

        row.appendChild(left);

        // Right side actions (optional)
        const right = document.createElement("div");
        right.className = "right";
        // ...
        row.appendChild(right);

        frag.appendChild(row);

        // Render qualifiers recursively (though we only expect 1 level)
        if (it.qualifiers && it.qualifiers.length) {
          it.qualifiers.forEach((q, qIdx) => {
            renderItem(q, index + 0.1 * (qIdx + 1), it.id);
          });
        }
      };

      items.forEach((it, index) => {
        renderItem(it, index);
      });

      schemaList.innerHTML = "";
      schemaList.appendChild(frag);
      updateSchemaRemoveButtonState();
    } catch (e) {
      console.error(e);
      schemaList.innerHTML = '<div class="muted">加载失败</div>';
    }
  }

  function setSelectedSchemaProp(pid, label) {
    window.kbSelectedSchemaPropId = pid || "";
    window.kbSelectedSchemaPropLabel = label || pid || "";
    try {
      const el = document.getElementById("attrCurrentProp");
      if (el)
        el.textContent = window.kbSelectedSchemaPropId
          ? `当前属性：${window.kbSelectedSchemaPropLabel} (${window.kbSelectedSchemaPropId})`
          : "当前属性：未选择";
    } catch {}
    applySchemaSelectionHighlight();
    try {
      const items = getCurrentSchemaItems();
      const found = items.find((it) => it.id === pid);
      const dtype = pickUiDatatype(found) || found?.datatype || "string";
      // Update datatype UI to show correct input group
      if (typeof window.updateDatatypeUI === "function") {
        window.updateDatatypeUI(
          dtype,
          found?.valuetype || found?.datavalue_type || "",
        );
      }
      const attrTypeEl = document.getElementById("attrType");
      if (attrTypeEl) attrTypeEl.value = dtype;
      if (dtype === "wikibase-entityid") {
        loadEntitySuggestionsForProperty(pid).catch((err) =>
          console.error("loadEntitySuggestionsForProperty", err),
        );
      } else {
        attrEntitySearchItems = [];
        if (attrEntitySearchStatus) attrEntitySearchStatus.textContent = "";
        if (attrEntitySearchResultsWrap)
          attrEntitySearchResultsWrap.style.display = "none";
      }
    } catch (err) {
      console.error("setSelectedSchemaProp suggestions failed", err);
    }
  }
  function applySchemaSelectionHighlight() {
    try {
      const rows = schemaList.querySelectorAll(".schema-item");
      rows.forEach((r) => {
        const pid = r.getAttribute("data-pid") || "";
        r.classList.toggle(
          "selected",
          !!window.kbSelectedSchemaPropId &&
            pid === window.kbSelectedSchemaPropId,
        );
      });
    } catch {}
  }

  async function addClassSchema(classId, propId) {
    const propertyPath = propertyIdToApiPath(propId);
    if (!propertyPath) throw new Error("属性ID不合法");
    const resp = await fetch("/api/kb/class/schema", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        class_id: classId,
        property_id: propertyPath,
      }),
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    return await resp.json();
  }

  // Events
  btnClsRefresh.addEventListener("click", () =>
    loadClasses((clsSearch.value || "").trim()),
  );
  clsSearch.addEventListener("input", () =>
    loadClasses((clsSearch.value || "").trim()),
  );
  btnSchemaRefresh.addEventListener("click", () => {
    if (window.kbSelectedClassId) loadClassSchema(window.kbSelectedClassId);
  });
  btnAddSchema.addEventListener("click", async () => {
    try {
      const cid = window.kbSelectedClassId;
      const pid = (schemaPropId.value || "").trim();
      if (!cid) {
        alert("请先在上方选择一个分类");
        return;
      }
      if (!pid) {
        alert("请输入属性ID");
        return;
      }

      // Check if a property is selected to add as qualifier
      const selection = window.kbSchemaRemovalSelection;
      if (selection && selection.size === 1) {
        const selectedId = Array.from(selection)[0];
        // Only support adding qualifier to top-level property (no nested qualifiers for now)
        if (!selectedId.includes(":")) {
          const url = new URL(
            "/api/kb/property/qualifier",
            window.location.origin,
          );
          const resp = await fetch(url.toString(), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ parent_id: selectedId, child_id: pid }),
          });
          if (!resp.ok) throw new Error("HTTP " + resp.status);

          schemaPropId.value = "";
          await loadClassSchema(cid);
          updateSchemaRemoveButtonState();
          return;
        }
      }

      await addClassSchema(cid, pid);
      schemaPropId.value = "";
      if (window.kbSchemaRemovalSelection)
        window.kbSchemaRemovalSelection.delete(canonicalizePropertyId(pid));
      await loadClassSchema(cid);
    } catch (e) {
      console.error(e);
      alert("添加失败: " + (e.message || e));
    }
    updateSchemaRemoveButtonState();
  });
  if (btnRemoveSchemaSelected) {
    btnRemoveSchemaSelected.addEventListener("click", async () => {
      try {
        const cid = window.kbSelectedClassId;
        if (!cid) {
          alert("请先在上方选择一个分类");
          return;
        }
        if (!window.kbSchemaRemovalSelection)
          window.kbSchemaRemovalSelection = new Set();
        const selected = Array.from(window.kbSchemaRemovalSelection);
        if (!selected.length) {
          alert("请使用 Ctrl/Shift 标记需要移除的属性");
          return;
        }
        const confirmed =
          selected.length === 1
            ? confirm(`确认移除属性 ${selected[0]} 吗？`)
            : confirm(`确认移除选中的 ${selected.length} 项属性吗？`);
        if (!confirmed) return;
        const results = await Promise.allSettled(
          selected.map(async (pid) => {
            if (pid.includes(":")) {
              // Remove qualifier
              const [parentId, childId] = pid.split(":");
              const url = new URL(
                "/api/kb/property/qualifier",
                window.location.origin,
              );
              url.searchParams.set("parent_id", parentId);
              url.searchParams.set("child_id", childId);
              const resp = await fetch(url.toString(), { method: "DELETE" });
              if (!resp.ok) throw new Error("HTTP " + resp.status);
            } else {
              const path = propertyIdToApiPath(pid);
              if (!path) throw new Error(`属性ID无效: ${pid}`);
              const url = new URL(
                "/api/kb/class/schema",
                window.location.origin,
              );
              url.searchParams.set("class_id", cid);
              url.searchParams.set("property_id", path);
              const resp = await fetch(url.toString(), { method: "DELETE" });
              if (!resp.ok) throw new Error("HTTP " + resp.status);
            }
          }),
        );
        const failures = results.filter((r) => r.status === "rejected");
        if (failures.length) {
          const firstErr = failures[0];
          const reason =
            firstErr && firstErr.status === "rejected" ? firstErr.reason : "";
          console.error("批量移除属性失败", failures);
          alert(
            `部分属性移除失败（${failures.length}/${selected.length}）: ${
              reason?.message || reason || "未知错误"
            }`,
          );
        }
        if (window.kbSchemaRemovalSelection)
          window.kbSchemaRemovalSelection.clear();
        window.kbSchemaRemovalLastIndex = -1;
        await loadClassSchema(cid);
      } catch (err) {
        console.error("批量移除属性失败", err);
        alert("批量移除失败: " + (err.message || err));
      } finally {
        updateSchemaRemoveButtonState();
      }
    });
  }
  updateSchemaRemoveButtonState();
  btnSetClass.addEventListener("click", async () => {
    try {
      const eid = (fId.value || "").trim();
      const cid = window.kbSelectedClassId;
      if (!eid) {
        alert("请先选择左侧一个实体");
        return;
      }
      if (!cid) {
        alert("请在上方选择一个分类");
        return;
      }
      await setEntityClass(eid, cid);
      await loadEntityClass(eid);
    } catch (e) {
      console.error(e);
      alert("设置失败: " + (e.message || e));
    }
  });
  btnClearClass.addEventListener("click", async () => {
    try {
      const eid = (fId.value || "").trim();
      if (!eid) {
        alert("请先选择左侧一个实体");
        return;
      }
      await clearEntityClass(eid, window.kbSelectedClassId || "");
      await loadEntityClass(eid);
    } catch (e) {
      console.error(e);
      alert("清除失败: " + (e.message || e));
    }
  });
  // Removed subclass add/delete handlers

  const clsColorPicker = document.getElementById("clsColorPicker");
  if (clsColorPicker) {
    clsColorPicker.addEventListener("change", async (e) => {
      if (!window.kbSelectedClassId) return;
      const color = e.target.value;
      try {
        const url = appendCurrentDbToUrl(
          new URL("/api/kb/classes/update", window.location.origin),
        );
        const resp = await fetch(url.toString(), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ id: window.kbSelectedClassId, color }),
        });
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        loadClasses((clsSearch?.value || "").trim());
      } catch (e) {
        console.error(e);
        alert("更新颜色失败");
      }
    });
  }

  // 分类图片设置按钮
  const btnClsImage = document.getElementById("btnClsImage");
  if (btnClsImage) {
    // 创建隐藏的文件选择器
    const clsImageInput = document.createElement("input");
    clsImageInput.type = "file";
    clsImageInput.accept = "image/*";
    clsImageInput.style.display = "none";
    document.body.appendChild(clsImageInput);

    btnClsImage.addEventListener("click", () => {
      if (!window.kbSelectedClassId) return;
      clsImageInput.click();
    });

    clsImageInput.addEventListener("change", async () => {
      const file = clsImageInput.files?.[0];
      if (!file || !window.kbSelectedClassId) {
        clsImageInput.value = "";
        return;
      }

      try {
        // 上传文件
        const formData = new FormData();
        formData.append("file", file);
        const uploadUrl = appendCurrentDbToUrl(
          new URL("/api/kb/classes/upload-image", window.location.origin),
        );
        const uploadResp = await fetch(uploadUrl.toString(), {
          method: "POST",
          body: formData,
        });
        if (!uploadResp.ok)
          throw new Error("Upload failed: " + uploadResp.status);
        const { url: imageUrl } = await uploadResp.json();

        // 更新分类图片
        const updateUrl = appendCurrentDbToUrl(
          new URL("/api/kb/classes/update", window.location.origin),
        );
        const updateResp = await fetch(updateUrl.toString(), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            id: window.kbSelectedClassId,
            image: imageUrl,
          }),
        });
        if (!updateResp.ok)
          throw new Error("Update failed: " + updateResp.status);
        loadClasses((clsSearch?.value || "").trim());
      } catch (e) {
        console.error(e);
        alert("上传图片失败");
      } finally {
        clsImageInput.value = "";
      }
    });
  }

  // Initial load of class hierarchy
  loadClasses("");
  try {
    updatePropertyRecommendations();
  } catch {}

  async function refreshClassPanelForCurrentDb() {
    try {
      window.kbSelectedClassId = null;
      window.kbEntityClasses = [];
      window.kbSchemaByClassId = Object.create(null);
      if (window.kbSchemaRemovalSelection)
        window.kbSchemaRemovalSelection.clear();
      window.kbSchemaRemovalLastIndex = -1;
      if (schemaList)
        schemaList.innerHTML = '<div class="muted">加载分类中...</div>';
      if (clsForEntity) clsForEntity.textContent = "未选择实体";
      await loadClasses((clsSearch?.value || "").trim());
      updateSchemaRemoveButtonState();
      try {
        updatePropertyRecommendations({ force: true });
      } catch {}
    } catch (err) {
      console.error("refreshClassPanelForCurrentDb", err);
    }
  }

  if (btnClsAdd) {
    btnClsAdd.addEventListener("click", () => {
      openClassModal();
    });
  }

  if (classModal) {
    classModal.addEventListener("click", (event) => {
      if (event.target === classModal) closeClassModal();
    });
  }

  if (classForm) {
    classForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (window.kbClassCreateSubmitting) return;
      const name = (clsNameInput?.value || "").trim();
      const description = (clsDescInput?.value || "").trim();
      const parentId = (classForm.dataset.parentId || "").trim();
      if (!name) {
        try {
          clsNameInput?.focus();
        } catch {}
        return;
      }
      const submitBtn = classForm.querySelector('button[type="submit"]');
      const prevText = submitBtn ? submitBtn.textContent : "";
      try {
        window.kbClassCreateSubmitting = true;
        if (submitBtn) {
          submitBtn.disabled = true;
          submitBtn.textContent = "保存中...";
        }
        const payload = {
          name,
          description,
          parent_id: parentId || null,
        };
        const created = await createClass(payload);
        closeClassModal();
        const currentSearch = (clsSearch?.value || "").trim();
        const shouldResetSearch =
          currentSearch &&
          !String(name).toLowerCase().includes(currentSearch.toLowerCase());
        if (shouldResetSearch && clsSearch) clsSearch.value = "";
        if (created?.id) {
          mergeCreatedClassIntoLocalList(created, payload);
          window.kbSelectedClassId = created.id;
          expandClassAncestors(created.id);
          try {
            renderClassTree(window.kbClasses || []);
          } catch {}
          revealSelectedClassInTree();
          try {
            await loadClassSchema(created.id);
          } catch {}
        }
        await loadClasses(shouldResetSearch ? "" : currentSearch);
        if (created?.id) {
          window.kbSelectedClassId = created.id;
          expandClassAncestors(created.id);
          try {
            renderClassTree(window.kbClasses || []);
          } catch {}
          revealSelectedClassInTree();
        }
      } catch (err) {
        console.error("create class failed", err);
        alert("新增分类失败: " + ((err && err.message) || err));
      } finally {
        window.kbClassCreateSubmitting = false;
        if (submitBtn) {
          submitBtn.disabled = false;
          submitBtn.textContent = prevText || "保存";
        }
      }
    });
  }

  window.addEventListener("kb:url-param-changed", (event) => {
    const detail = event && event.detail ? event.detail : {};
    if ((detail.key || "") !== "db") return;
    refreshClassPanelForCurrentDb();
  });

  window.addEventListener("popstate", () => {
    refreshClassPanelForCurrentDb();
  });

  // Create class / property actions
  // 搜索已有属性并将结果挂到分类
  // 搜索已有属性并将结果挂到分类
  function renderPropSearchResults(items) {
    if (!propSearchResults || !propSearchResultsWrap) return;
    propSearchResults.innerHTML = "";
    if (!Array.isArray(items) || !items.length) {
      propSearchResultsWrap.style.display = "none";
      return;
    }
    const frag = document.createDocumentFragment();
    items.forEach((it, idx) => {
      const opt = document.createElement("option");
      opt.value = it.id || "";
      const label = it.label || it.id || opt.value;
      const dtypeUi = pickUiDatatype(it);
      const dtype = dtypeUi ? ` [${dtypeUi}]` : "";
      opt.textContent = `${label} (${it.id || ""})${dtype}`;
      if (idx === 0) opt.selected = true;
      frag.appendChild(opt);
    });
    propSearchResults.appendChild(frag);
    propSearchResultsWrap.style.display = "";
    try {
      // Do not auto-fill schemaPropId on render, wait for user selection
      // const firstValue = items[0]?.id || "";
      // if (firstValue) schemaPropId.value = firstValue;
    } catch {}
  }

  async function searchPropertiesByKeyword(keyword) {
    if (!keyword) {
      renderPropSearchResults([]);
      return;
    }
    propCreateMsg.textContent = "搜索中…";
    try {
      const url = new URL("/api/kb/property_search", window.location.origin);
      url.searchParams.set("q", keyword);
      const resp = await fetch(url.toString());
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const data = await resp.json();
      // API returns { items: [], total: N }
      const items = Array.isArray(data?.items) ? data.items.slice() : [];
      if (items.length > 1) {
        // Sort by numeric id if available, fallback to lexical order
        items.sort((a, b) => {
          const leftId = (a?.id || "").toString();
          const rightId = (b?.id || "").toString();
          const leftNum = parseInt(leftId.replace(/\D+/g, ""), 10);
          const rightNum = parseInt(rightId.replace(/\D+/g, ""), 10);
          const leftValid = !Number.isNaN(leftNum);
          const rightValid = !Number.isNaN(rightNum);
          if (leftValid && rightValid && leftNum !== rightNum) {
            return leftNum - rightNum;
          }
          if (leftValid && !rightValid) return -1;
          if (!leftValid && rightValid) return 1;
          return leftId.localeCompare(rightId);
        });
      }
      renderPropSearchResults(items);
      if (!items.length) {
        propCreateMsg.textContent = "未找到匹配属性";
      } else {
        propCreateMsg.textContent = `找到 ${items.length} 条，请选择后点击“添加”`;
      }
    } catch (e) {
      console.error("property search failed", e);
      propCreateMsg.textContent = "搜索失败: " + (e.message || e);
      renderPropSearchResults([]);
    }
  }

  if (propSearchInput) {
    propSearchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        const keyword = (propSearchInput.value || "").trim();
        searchPropertiesByKeyword(keyword);
      }
    });
  }

  if (propSearchResults) {
    propSearchResults.addEventListener("change", () => {
      try {
        const pid = (propSearchResults.value || "").trim();
        if (pid) schemaPropId.value = pid;
      } catch {}
    });
    propSearchResults.addEventListener("dblclick", () => {
      try {
        const pid = (propSearchResults.value || "").trim();
        if (pid) {
          schemaPropId.value = pid;
          closePropPanelModal();
        }
      } catch {}
    });
  }

  btnCreateProperty.addEventListener("click", async () => {
    propCreateMsg.textContent = "";
    let selectedPid = (propSearchResults?.value || "").trim();
    if (!selectedPid) {
      selectedPid = (schemaPropId.value || "").trim();
    }
    if (!selectedPid && propRecommendActiveId) {
      selectedPid = propRecommendActiveId.trim();
    }
    if (!selectedPid) {
      propCreateMsg.textContent = "请先搜索或选择推荐属性";
      return;
    }
    const cid = window.kbSelectedClassId;
    if (!cid) {
      propCreateMsg.textContent = "请先在上方选择一个分类";
      return;
    }
    try {
      propCreateMsg.textContent = "添加中…";
      await addClassSchema(cid, selectedPid);
      try {
        schemaPropId.value = selectedPid;
      } catch {}
      await loadClassSchema(cid);
      try {
        propRecommendActiveId = "";
        if (propRecommendList) {
          propRecommendList
            .querySelectorAll(".prop-rec-item")
            .forEach((el) => el.classList.remove("active"));
        }
      } catch {}
      propCreateMsg.textContent = "已添加到分类";
      setTimeout(() => {
        if (propCreateMsg.textContent === "已添加到分类") {
          propCreateMsg.textContent = "";
        }
      }, 1500);
      try {
        refreshAttrPropDatalist();
      } catch {}
    } catch (e) {
      console.error(e);
      propCreateMsg.textContent = "添加失败: " + (e.message || e);
    }
  });

  // ----------------------
  // Attribute property dropdown (searchable via datalist)
  // ----------------------
  function getCurrentSchemaItems() {
    const cid =
      window.kbSelectedClassId ||
      (Array.isArray(window.kbEntityClasses) &&
        window.kbEntityClasses[0]?.id) ||
      "";
    if (!cid) return [];
    try {
      const m = window.kbSchemaByClassId || {};
      return Array.isArray(m[cid]) ? m[cid] : [];
    } catch {
      return [];
    }
  }
  function refreshAttrPropDatalist() {
    try {
      const list = document.getElementById("attrPropList");
      if (!list) return;
      const items = getCurrentSchemaItems();
      list.innerHTML = "";
      const frag = document.createDocumentFragment();
      items.forEach((it) => {
        const opt = document.createElement("option");
        // Use property id as value, label for readability
        opt.value = it.id || "";
        try {
          opt.label = it.label || it.id || "";
        } catch {}
        frag.appendChild(opt);
      });
      list.appendChild(frag);
    } catch {}
  }

  // Populate the clsNewSelect <select> with types derived from clicked entity
  function refreshClsNewOptions(types) {
    try {
      const sel = document.getElementById("clsNewSelect");
      if (!sel) return;
      const items = Array.isArray(types)
        ? types
        : Array.isArray(window.kbEntityClasses)
          ? window.kbEntityClasses
          : [];
      const placeholder = sel.querySelector('option[value=""]')
        ? sel.querySelector('option[value=""]').textContent
        : "— 从已知类型选择（可选） —";
      sel.innerHTML = "";
      const opt0 = document.createElement("option");
      opt0.value = "";
      opt0.textContent = placeholder;
      sel.appendChild(opt0);
      items.forEach((it) => {
        try {
          const o = document.createElement("option");
          o.value = it.id || it._id || it.id || "";
          o.textContent = it.label || it.id || o.value;
          sel.appendChild(o);
        } catch {}
      });
    } catch (e) {
      console.error("refreshClsNewOptions", e);
    }
  }
  // When a property is chosen from datalist, auto-fill Chinese label if empty
  attrProp.addEventListener("change", () => {
    try {
      const v = (attrProp.value || "").trim();
      if (!v) return;
      const items = getCurrentSchemaItems();
      const found = items.find((it) => it.id === v);
      if (found && (!attrPropLabel.value || !attrPropLabel.value.trim())) {
        attrPropLabel.value = found.label || "";
      }
      Promise.resolve()
        .then(() => loadEntitySuggestionsForProperty(v))
        .catch((err) => console.error("loadEntitySuggestionsForProperty", err));
    } catch {}
  });
  // Also rebuild datalist on focus to keep in sync
  attrProp.addEventListener("focus", () => {
    try {
      refreshAttrPropDatalist();
    } catch {}
  });

  window.openPropPanelModal = openPropPanelModal;
  window.closePropPanelModal = closePropPanelModal;
  window.getActiveClassIdForRecommendations =
    getActiveClassIdForRecommendations;
  window.updatePropertyRecommendations = updatePropertyRecommendations;
  window.loadClasses = loadClasses;
  window.renderClassTree = renderClassTree;
  window.loadEntityClass = loadEntityClass;
  window.setEntityClass = setEntityClass;
  window.clearEntityClass = clearEntityClass;
  window.updateSchemaRemoveButtonState = updateSchemaRemoveButtonState;
  window.loadClassSchema = loadClassSchema;
  window.setSelectedSchemaProp = setSelectedSchemaProp;
  window.applySchemaSelectionHighlight = applySchemaSelectionHighlight;
  window.addClassSchema = addClassSchema;
  window.getCurrentSchemaItems = getCurrentSchemaItems;
  window.refreshAttrPropDatalist = refreshAttrPropDatalist;

  // =====================================================
  // fType 字段辅助（数据列表填充 + 类型同步）
  // =====================================================
  const fTypeInput = byId("fType");
  const fTypeOptions = byId("fTypeOptions");

  function populateFTypeDatalist() {
    if (!fTypeOptions) return;
    fTypeOptions.innerHTML = "";
    const classes = Array.isArray(window.kbClasses) ? window.kbClasses : [];
    classes.forEach((cls) => {
      try {
        const opt = document.createElement("option");
        opt.value = cls.label || cls.name || cls.id;
        opt.dataset.classId = cls.id;
        fTypeOptions.appendChild(opt);
      } catch {}
    });
  }

  function findClassByLabel(label) {
    const q = (label || "").trim().toLowerCase();
    if (!q) return null;
    const classes = Array.isArray(window.kbClasses) ? window.kbClasses : [];
    return (
      classes.find((c) => (c.label || c.name || "").toLowerCase() === q) ||
      classes.find((c) => (c.id || "").toLowerCase() === q) ||
      null
    );
  }

  // =====================================================
  // 属性选择器（内嵌在 attrPanel，支持类型属性 + 全局搜索）
  // =====================================================
  const attrPropPickerList = byId("attrPropPickerList");
  const attrPropPickerStatus = byId("attrPropPickerStatus");
  const attrPropSearchInput = byId("attrPropSearch");
  const btnAttrPropSearchAll = byId("btnAttrPropSearchAll");

  let _propPickerCurrentItems = []; // items shown for the active type
  let _propPickerSearchTimer = null;
  let _propPickerCurrentTypeLabel = "";

  function getCurrentTypeLabelForPropertySearch() {
    const fromInput = (fTypeInput?.value || "").trim();
    if (fromInput) return fromInput;
    const remembered = (_propPickerCurrentTypeLabel || "").trim();
    if (remembered) return remembered;
    const clsId = (window.kbSelectedClassId || "").toString().trim();
    if (!clsId) return "";
    const classes = Array.isArray(window.kbClasses) ? window.kbClasses : [];
    const cls = classes.find((it) => (it?.id || "") === clsId);
    return (cls?.label || cls?.name || "").toString().trim();
  }

  /** Populate the property picker <select> with items */
  function renderAttrPropPickerItems(items, statusText) {
    if (!attrPropPickerList) return;
    attrPropPickerList.innerHTML = "";
    if (attrPropPickerStatus)
      attrPropPickerStatus.textContent = statusText || "";
    if (!items || !items.length) {
      const opt = document.createElement("option");
      opt.disabled = true;
      opt.textContent = statusText
        ? "无匹配属性，请尝试其他关键词"
        : "暂无关联属性，请搜索";
      attrPropPickerList.appendChild(opt);
      return;
    }
    const frag = document.createDocumentFragment();
    items.forEach((it) => {
      const opt = document.createElement("option");
      opt.value = it.id || "";
      const uiType =
        (typeof pickUiDatatype === "function" ? pickUiDatatype(it) : null) ||
        it.datatype ||
        "";
      const nameStr = it.label || it.name || it.id || "";
      opt.textContent = nameStr + (uiType ? `  [${uiType}]` : "");
      opt.dataset.propLabel = nameStr;
      opt.dataset.dtype = it.datatype || "";
      opt.dataset.valuetype = it.valuetype || "";
      frag.appendChild(opt);
    });
    attrPropPickerList.appendChild(frag);
  }

  // Wire the <select> change event once to trigger property selection
  if (attrPropPickerList) {
    attrPropPickerList.addEventListener("change", () => {
      const opt = attrPropPickerList.options[attrPropPickerList.selectedIndex];
      if (!opt || !opt.value || opt.disabled) return;
      const propId = opt.value;
      const label =
        opt.dataset.propLabel ||
        opt.textContent.replace(/\s*\[.*?\]\s*$/, "").trim();
      const dtype = opt.dataset.dtype || "string";
      const valuetype = opt.dataset.valuetype || "";
      // 先更新 dtype UI（用 option 上的精确值），再同步选中状态
      // 这样可避免 setSelectedSchemaProp 内部因找不到 schema item 而回退到 "string"
      try {
        const uiType =
          (typeof window.mapDatatypeToUi === "function"
            ? window.mapDatatypeToUi(dtype, valuetype)
            : null) ||
          (typeof pickUiDatatype === "function"
            ? pickUiDatatype({ datatype: dtype, valuetype })
            : null) ||
          dtype ||
          "string";
        if (typeof window.updateDatatypeUI === "function") {
          window.updateDatatypeUI(uiType, valuetype);
        }
        const attrTypeEl = document.getElementById("attrType");
        if (attrTypeEl) attrTypeEl.value = uiType;
      } catch {}
      // setSelectedSchemaProp 放在后面，且不让其内部再覆盖已设置的 dtype UI
      window.kbSelectedSchemaPropId = propId;
      window.kbSelectedSchemaPropLabel = label;
      try {
        const el = document.getElementById("attrCurrentProp");
        if (el) el.textContent = `当前属性：${label} (${propId})`;
      } catch {}
      applySchemaSelectionHighlight();
      // 仅处理实体类型的搜索建议，不重复调用 updateDatatypeUI
      try {
        const uiType =
          (typeof window.mapDatatypeToUi === "function"
            ? window.mapDatatypeToUi(dtype, valuetype)
            : null) ||
          (typeof pickUiDatatype === "function"
            ? pickUiDatatype({ datatype: dtype, valuetype })
            : null) ||
          dtype ||
          "string";
        if (uiType === "wikibase-entityid") {
          if (typeof loadEntitySuggestionsForProperty === "function") {
            loadEntitySuggestionsForProperty(propId).catch(console.error);
          }
        } else {
          if (typeof attrEntitySearchItems !== "undefined") {
            try {
              attrEntitySearchItems = [];
            } catch {}
          }
          const statusEl = document.getElementById("attrEntitySearchStatus");
          if (statusEl) statusEl.textContent = "";
          const wrapEl = document.getElementById("attrEntitySearchResultsWrap");
          if (wrapEl) wrapEl.style.display = "none";
        }
      } catch {}
    });
  }

  /** Search all properties by keyword and render */
  async function searchAndRenderProps(query, options = {}) {
    try {
      if (attrPropPickerStatus) attrPropPickerStatus.textContent = "搜索中…";
      const url = appendCurrentDbToUrl(
        new URL("/api/kb/property_search", window.location.origin),
      );
      url.searchParams.set("q", query);
      const searchAll = options?.searchAll !== false;
      if (!searchAll) {
        const activeClassId = (window.kbSelectedClassId || "")
          .toString()
          .trim();
        if (activeClassId) url.searchParams.set("class_id", activeClassId);
        const activeType = getCurrentTypeLabelForPropertySearch();
        if (activeType) url.searchParams.set("type_name", activeType);
      }
      url.searchParams.set("limit", "40");
      const data = await apiGet(url.toString());
      const items = Array.isArray(data?.items) ? data.items : [];
      const hint = searchAll
        ? `全局搜索结果 ${items.length} 项`
        : `关联搜索结果 ${items.length} 项`;
      renderAttrPropPickerItems(items, hint);
    } catch (err) {
      console.error("searchAndRenderProps", err);
      if (attrPropPickerStatus) attrPropPickerStatus.textContent = "搜索失败";
    }
  }

  async function loadAttrPropPicker(typeLabelOrId) {
    const label = (typeLabelOrId || "").trim();
    _propPickerCurrentTypeLabel = label;
    if (attrPropSearchInput) attrPropSearchInput.value = "";

    if (!label) {
      // 无类型：清空下拉，提示设置类型
      window.kbSelectedClassId = null;
      _propPickerCurrentTypeLabel = "";
      _propPickerCurrentItems = [];
      renderAttrPropPickerItems([], "");
      if (attrPropPickerStatus)
        attrPropPickerStatus.textContent =
          "设置节点类型后显示推荐属性，或直接搜索";
      return;
    }

    if (attrPropPickerStatus) attrPropPickerStatus.textContent = "加载中…";

    // Ensure classes are loaded
    if (!window.kbClasses || !window.kbClasses.length) {
      try {
        await loadClasses("");
      } catch {}
    }

    const cls = findClassByLabel(label);

    // 优先按节点类型名称查询关联属性推荐（properties.types）
    try {
      const typeUrl = appendCurrentDbToUrl(
        new URL("/api/kb/property_search", window.location.origin),
      );
      typeUrl.searchParams.set("q", "");
      typeUrl.searchParams.set("type_name", label);
      typeUrl.searchParams.set("limit", "40");
      typeUrl.searchParams.set("offset", "0");
      const typeData = await apiGet(typeUrl.toString());
      const typeItems = Array.isArray(typeData?.items) ? typeData.items : [];
      if (typeItems.length) {
        _propPickerCurrentItems = typeItems;
        window.kbSelectedClassId = cls ? cls.id : null;
        renderAttrPropPickerItems(
          typeItems,
          `${label} · ${typeItems.length} 项类型关联属性`,
        );
        return;
      }
    } catch (err) {
      console.error("load type_name property recommendations", err);
    }

    if (!cls) {
      window.kbSelectedClassId = null;
      _propPickerCurrentItems = [];
      renderAttrPropPickerItems([], "");
      if (attrPropPickerStatus)
        attrPropPickerStatus.textContent = `类型"${label}"未注册分类，可搜索属性`;
      return;
    }

    // 同步 class id 供 dtype 查找
    window.kbSelectedClassId = cls.id;

    try {
      const url = appendCurrentDbToUrl(
        new URL("/api/kb/class/schema", window.location.origin),
      );
      url.searchParams.set("class_id", cls.id);
      const data = await apiGet(url.toString());
      const items = Array.isArray(data?.items) ? data.items : [];

      if (!window.kbSchemaByClassId)
        window.kbSchemaByClassId = Object.create(null);
      window.kbSchemaByClassId[cls.id] = items;
      try {
        refreshAttrPropDatalist();
      } catch {}

      _propPickerCurrentItems = items;

      if (!items.length) {
        renderAttrPropPickerItems([], "");
        if (attrPropPickerStatus)
          attrPropPickerStatus.textContent = `${cls.label || cls.name} 暂无关联属性，可搜索添加`;
      } else {
        renderAttrPropPickerItems(
          items,
          `${cls.label || cls.name} · ${items.length} 项推荐属性`,
        );
      }
    } catch (err) {
      console.error("loadAttrPropPicker", err);
      _propPickerCurrentItems = [];
      renderAttrPropPickerItems([], "");
      if (attrPropPickerStatus) attrPropPickerStatus.textContent = "加载失败";
    }
  }

  // Wire fType input change → sync kbSelectedClassId + reload property picker
  if (fTypeInput) {
    fTypeInput.addEventListener("change", () => {
      const val = (fTypeInput.value || "").trim();
      loadAttrPropPicker(val).catch(console.error);
    });
    fTypeInput.addEventListener("input", () => {
      const val = (fTypeInput.value || "").trim();
      if (!val) {
        // 清空类型：清空选择器并提示
        clearTimeout(_propPickerSearchTimer);
        _propPickerCurrentItems = [];
        renderAttrPropPickerItems([], "");
        if (attrPropPickerStatus)
          attrPropPickerStatus.textContent =
            "设置节点类型后显示推荐属性，或直接搜索";
      }
    });
  }

  if (attrPropSearchInput) {
    attrPropSearchInput.addEventListener("input", () => {
      clearTimeout(_propPickerSearchTimer);
      const q = (attrPropSearchInput.value || "").trim();
      if (!q) {
        // 搜索清空 → 恢复类型推荐属性
        if (_propPickerCurrentItems.length) {
          const clsId = window.kbSelectedClassId;
          const cls =
            clsId && Array.isArray(window.kbClasses)
              ? window.kbClasses.find((c) => c.id === clsId)
              : null;
          const prefix = cls ? (cls.label || cls.name) + " · " : "";
          renderAttrPropPickerItems(
            _propPickerCurrentItems,
            `${prefix}${_propPickerCurrentItems.length} 项推荐属性`,
          );
        } else {
          renderAttrPropPickerItems([], "");
          if (attrPropPickerStatus)
            attrPropPickerStatus.textContent =
              "设置节点类型后显示推荐属性，或直接搜索";
        }
        return;
      }
      // 先从当前缓存中过滤
      // 默认行为：输入即全局搜索
      _propPickerSearchTimer = setTimeout(() => {
        searchAndRenderProps(q, { searchAll: true }).catch(console.error);
      }, 300);
    });
    attrPropSearchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        clearTimeout(_propPickerSearchTimer);
        const q = (attrPropSearchInput.value || "").trim();
        if (q)
          searchAndRenderProps(q, { searchAll: true }).catch(console.error);
      }
    });
  }

  if (btnAttrPropSearchAll) {
    btnAttrPropSearchAll.addEventListener("click", () => {
      clearTimeout(_propPickerSearchTimer);
      const q = (attrPropSearchInput?.value || "").trim();
      searchAndRenderProps(q, { searchAll: false }).catch(console.error);
    });
  }

  // Populate fType datalist whenever the class tree is rebuilt
  if (clsTree) {
    const _fTypeObserver = new MutationObserver(() => {
      try {
        populateFTypeDatalist();
      } catch {}
    });
    _fTypeObserver.observe(clsTree, { childList: true });
  }
  document.addEventListener("DOMContentLoaded", () => {
    setTimeout(() => {
      try {
        populateFTypeDatalist();
      } catch {}
    }, 500);
  });

  window.loadAttrPropPicker = loadAttrPropPicker;
  window.loadTypeSchema = loadAttrPropPicker; // backward compat alias

  // =====================================================
  // 标签管理（右侧，绑定 kbSelectedClassId）
  // =====================================================
  const tagInput = byId("tagInput");
  const tagList = byId("tagList");
  const btnTagAdd = byId("btnTagAdd");
  const tagMsg = byId("tagMsg");
  const tagMgrDesc = byId("tagMgrDesc");
  const tagAddBar = byId("tagAddBar");
  const btnTagRefresh = byId("btnTagRefresh");

  let currentTagClassId = null;
  let currentTagClassTags = [];

  function getAllClassTags() {
    const classes = Array.isArray(window.kbClasses) ? window.kbClasses : [];
    const tagMap = new Map();
    classes.forEach((cls) => {
      const tags = Array.isArray(cls?.tags) ? cls.tags : [];
      tags.forEach((rawTag) => {
        const tag = (rawTag ?? "").toString().trim();
        if (!tag) return;
        const key = tag.toLowerCase();
        if (!tagMap.has(key)) tagMap.set(key, tag);
      });
    });
    return Array.from(tagMap.values()).sort((a, b) =>
      a.localeCompare(b, "zh-CN"),
    );
  }

  /** Get current entity tags from left panel fTags input as a Set (lowercase for comparison) */
  function getEntityTagSet() {
    const fTags = document.getElementById("fTags");
    if (!fTags || !fTags.value) return new Set();
    return new Set(
      fTags.value
        .split(/[,，;；、\n]+/)
        .map((t) => t.trim())
        .filter(Boolean)
        .map((t) => t.toLowerCase()),
    );
  }

  /** Toggle a tag in the left panel fTags input */
  function toggleEntityTag(tag) {
    const fTags = document.getElementById("fTags");
    if (!fTags) return;
    const current = fTags.value
      .split(/[,，;；、\n]+/)
      .map((t) => t.trim())
      .filter(Boolean);
    const idx = current.findIndex((t) => t.toLowerCase() === tag.toLowerCase());
    if (idx >= 0) {
      current.splice(idx, 1);
    } else {
      current.push(tag);
    }
    fTags.value = current.join(", ");
    // Trigger input event so entity header syncs
    try {
      fTags.dispatchEvent(new Event("input", { bubbles: true }));
    } catch {}
    renderTagList(currentTagClassTags);
  }

  function renderTagList(tags) {
    if (!tagList) return;
    tagList.innerHTML = "";
    const arr = Array.isArray(tags) ? tags : [];
    if (!arr.length) {
      const hint = document.createElement("span");
      hint.className = "muted";
      hint.style.fontSize = "12px";
      hint.textContent = "暂无标签";
      tagList.appendChild(hint);
      return;
    }
    const entityTags = getEntityTagSet();
    const allowClassTagEdit = Boolean(currentTagClassId);
    const frag = document.createDocumentFragment();
    arr.forEach((tag, idx) => {
      const isApplied = entityTags.has(tag.toLowerCase());
      const chip = document.createElement("span");
      chip.className = "tag" + (isApplied ? " tag-applied" : "");
      chip.style.display = "inline-flex";
      chip.style.alignItems = "center";
      chip.style.gap = "4px";
      chip.style.padding = "3px 8px";
      chip.style.borderRadius = "12px";
      chip.style.fontSize = "12px";
      chip.style.cursor = "pointer";
      chip.style.userSelect = "none";
      chip.style.transition = "all 0.15s ease";
      if (isApplied) {
        chip.style.background = "var(--accent, #4f46e5)";
        chip.style.color = "#fff";
        chip.style.border = "1px solid var(--accent, #4f46e5)";
      } else {
        chip.style.background = "rgba(79,70,229,0.10)";
        chip.style.color = "var(--accent, #4f46e5)";
        chip.style.border = "1px solid rgba(79,70,229,0.25)";
      }
      chip.title = isApplied
        ? `点击移除标签「${tag}」`
        : `点击添加标签「${tag}」`;
      // Click on chip label area → toggle tag on entity
      chip.addEventListener("click", (e) => {
        if (e.target.closest(".tag-delete-btn")) return;
        toggleEntityTag(tag);
      });
      const labelSpan = document.createElement("span");
      labelSpan.textContent = tag;
      chip.appendChild(labelSpan);
      if (isApplied) {
        const check = document.createElement("i");
        check.className = "fa-solid fa-check";
        check.style.fontSize = "10px";
        check.style.opacity = "0.8";
        chip.appendChild(check);
      }
      if (allowClassTagEdit) {
        const del = document.createElement("i");
        del.className = "fa-solid fa-xmark tag-delete-btn";
        del.style.cursor = "pointer";
        del.style.opacity = "0.6";
        del.style.fontSize = "10px";
        del.style.marginLeft = "2px";
        del.title = "从分类中删除此标签";
        del.addEventListener("mouseover", () => {
          del.style.opacity = "1";
        });
        del.addEventListener("mouseout", () => {
          del.style.opacity = "0.6";
        });
        del.addEventListener("click", (e) => {
          e.stopPropagation();
          removeClassTag(idx);
        });
        chip.appendChild(del);
      }
      frag.appendChild(chip);
    });
    tagList.appendChild(frag);
  }

  async function saveClassTags(classId, tags) {
    const url = appendCurrentDbToUrl(
      new URL("/api/kb/classes/update", window.location.origin),
    );
    const resp = await fetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: classId, tags }),
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    // Update local cache
    if (Array.isArray(window.kbClasses)) {
      const found = window.kbClasses.find((c) => c.id === classId);
      if (found) found.tags = tags;
    }
    return await resp.json();
  }

  async function removeClassTag(idx) {
    if (!currentTagClassId) return;
    const tags = currentTagClassTags.slice();
    if (idx < 0 || idx >= tags.length) return;
    tags.splice(idx, 1);
    try {
      await saveClassTags(currentTagClassId, tags);
      currentTagClassTags = tags;
      renderTagList(currentTagClassTags);
      if (tagMsg) {
        tagMsg.textContent = "已删除";
        setTimeout(() => {
          if (tagMsg) tagMsg.textContent = "";
        }, 1500);
      }
    } catch (err) {
      console.error("removeClassTag", err);
      if (tagMsg) tagMsg.textContent = "删除失败";
    }
  }

  async function addClassTagFromInput() {
    if (!currentTagClassId || !tagInput) return;
    const val = (tagInput.value || "").trim();
    if (!val) return;
    const tags = currentTagClassTags.slice();
    if (tags.some((t) => t.toLowerCase() === val.toLowerCase())) {
      if (tagMsg) tagMsg.textContent = "标签已存在";
      return;
    }
    tags.push(val);
    try {
      await saveClassTags(currentTagClassId, tags);
      currentTagClassTags = tags;
      tagInput.value = "";
      renderTagList(currentTagClassTags);
      if (tagMsg) {
        tagMsg.textContent = "已添加";
        setTimeout(() => {
          if (tagMsg) tagMsg.textContent = "";
        }, 1500);
      }
    } catch (err) {
      console.error("addClassTagFromInput", err);
      if (tagMsg) tagMsg.textContent = "添加失败";
    }
  }

  function showTagPanelForClass(cls) {
    if (!cls) {
      currentTagClassId = null;
      currentTagClassTags = getAllClassTags();
      if (tagMgrDesc)
        tagMgrDesc.textContent = `全部标签（${currentTagClassTags.length}）`;
      if (tagAddBar) tagAddBar.style.display = "none";
      renderTagList(currentTagClassTags);
      return;
    }
    currentTagClassId = cls.id;
    currentTagClassTags = Array.isArray(cls.tags) ? cls.tags.slice() : [];
    if (tagMgrDesc)
      tagMgrDesc.textContent = `当前分类：${cls.label || cls.name}（${currentTagClassTags.length}）`;
    if (tagAddBar) tagAddBar.style.display = "flex";
    renderTagList(currentTagClassTags);
    if (tagMsg) tagMsg.textContent = "";
  }

  if (btnTagAdd) {
    btnTagAdd.addEventListener("click", () =>
      addClassTagFromInput().catch(console.error),
    );
  }
  if (tagInput) {
    tagInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        addClassTagFromInput().catch(console.error);
      }
    });
  }
  if (btnTagRefresh) {
    btnTagRefresh.addEventListener("click", () => {
      if (currentTagClassId) {
        const cls = (window.kbClasses || []).find(
          (c) => c.id === currentTagClassId,
        );
        showTagPanelForClass(cls || null);
      } else {
        showTagPanelForClass(null);
      }
    });
  }

  // Hook into class selection to update tag panel
  // Override the renderClassTree click behavior to also update tag panel
  const origRenderClassTree = window.renderClassTree;
  // Instead, we listen on clsTree clicks
  if (clsTree) {
    clsTree.addEventListener(
      "click",
      (e) => {
        // The classification tree click sets kbSelectedClassId; after a short delay read it
        setTimeout(() => {
          try {
            const selectedId = window.kbSelectedClassId;
            if (selectedId) {
              const cls = (window.kbClasses || []).find(
                (c) => c.id === selectedId,
              );
              showTagPanelForClass(cls || null);
            } else {
              showTagPanelForClass(null);
            }
          } catch {}
        }, 50);
      },
      { capture: true },
    );
  }

  // Default to all tags when no class is selected
  setTimeout(() => {
    try {
      if (!window.kbSelectedClassId) showTagPanelForClass(null);
    } catch {}
  }, 200);

  window.showTagPanelForClass = showTagPanelForClass;
  window.refreshClsNewOptions = refreshClsNewOptions;
  window.searchPropertiesByKeyword = searchPropertiesByKeyword;
  window.refreshTagListHighlight = () => renderTagList(currentTagClassTags);
})();
