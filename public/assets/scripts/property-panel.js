(function () {
  const shared = window.kbApp || {};
  const state = shared.state || {};
  const dom = shared.dom || {};
  const byId = dom.byId || ((id) => document.getElementById(id));
  const urlParams = new URLSearchParams(window.location.search);
  let propertyPage = parseInt(urlParams.get("prop_page") || "1", 10);
  let propertyPageSize = parseInt(urlParams.get("prop_limit") || "20", 10);
  let propertyTotal = 0;

  const propertyTable = byId("propertyTable");
  const propPageSizeSelect = byId("propertyPageSize");
  if (typeof state.bindAlias === "function") {
    state.bindAlias("propertySelectedIds", "propertySelectedIds", () => new Set());
  }
  if (propPageSizeSelect) {
    propPageSizeSelect.value = propertyPageSize.toString();
  }

  function updatePropertyPageInfo() {
    const maxPage = Math.max(1, Math.ceil(propertyTotal / propertyPageSize));
    const info = byId("propertyPageInfo");
    if (info) {
      info.textContent = `第 ${propertyPage} / ${maxPage} 页 · 共 ${propertyTotal} 条`;
    }

    const prevButton = byId("btnPropertyPrevPage");
    const nextButton = byId("btnPropertyNextPage");
    if (prevButton) prevButton.disabled = propertyPage <= 1;
    if (nextButton) nextButton.disabled = propertyPage >= maxPage;
  }

  async function loadPropertyList() {
    if (!propertyTable) return;

    const tbody = propertyTable.querySelector("tbody");
    if (!tbody) return;

    tbody.innerHTML = '<tr><td colspan="4" class="muted">加载中…</td></tr>';
    const searchInput = byId("propertyMgmtSearch");
    const q = searchInput ? searchInput.value.trim() : "";

    if (typeof window.updateUrlParam === "function") {
      window.updateUrlParam("prop_limit", propertyPageSize);
    }

    const params = new URLSearchParams({
      limit: propertyPageSize,
      offset: (propertyPage - 1) * propertyPageSize,
    });
    if (q) params.set("q", q);

    try {
      const resp = await fetch("/api/kb/property_search?" + params.toString());
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const data = await resp.json();
      const list = Array.isArray(data.items) ? data.items : [];

      propertyTotal = data.total || list.length;
      if (!list.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="muted">暂无属性</td></tr>';
        updatePropertyPageInfo();
        return;
      }

      tbody.innerHTML = "";
      for (const prop of list) {
        const tr = document.createElement("tr");
        tr.setAttribute("data-id", prop.id || "");

        let displayType = prop.datatype || "string";
        if (prop.data && prop.data.datatype) {
          displayType = prop.data.datatype;
        }

        tr.innerHTML = `<td>${prop.id || ""}</td><td>${
          prop.label || prop.name || ""
        }</td><td>${displayType}</td>
            <td>${prop.valuetype || ""}</td>
            <td>
              <button class="btn sm icon btnPropertyEdit" title="编辑"><i class="fa-solid fa-pen"></i></button>
              <button class="btn sm icon danger btnPropertyDelete" data-id="${prop.id}" title="删除"><i class="fa-solid fa-trash"></i></button>
            </td>`;

        const btnEdit = tr.querySelector(".btnPropertyEdit");
        if (btnEdit) {
          btnEdit.addEventListener("click", (e) => {
            e.stopPropagation();
            openPropertyModal("edit", {
              id: prop.id,
              name: prop.name || prop.label,
              datatype: displayType,
              valuetype: prop.valuetype,
            });
          });
        }

        tbody.appendChild(tr);
      }

      updatePropertyPageInfo();
      updatePropertySelectedStyles();
    } catch (e) {
      tbody.innerHTML = `<tr><td colspan='5' class='muted'>加载失败: ${
        e.message || e
      }</td></tr>`;
      updatePropertyPageInfo();
    }
  }

  function openPropertyModal(mode, data = {}) {
    const modal = byId("propertyModal");
    const title = byId("propertyModalTitle");
    const form = byId("propertyForm");
    if (!modal || !title || !form) return;

    modal.style.display = "flex";
    if (mode === "edit") {
      title.textContent = "编辑属性";
      byId("propId").value = data.id || "";
      byId("propName").value = data.name || "";
      byId("propDatatype").value = data.datatype || "string";
      byId("propValuetype").value = data.valuetype || "";
    } else {
      title.textContent = "新增属性";
      form.reset();
      byId("propId").value = "";
    }
  }

  function closePropertyModal() {
    const modal = byId("propertyModal");
    if (modal) modal.style.display = "none";
  }

  async function deleteProperty(id, skipConfirm = false) {
    if (!id) return;
    if (!skipConfirm && !confirm("确定要删除该属性吗？")) return;

    try {
      const resp = await fetch("/api/kb/property_delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id }),
      });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      if (!skipConfirm) await loadPropertyList();
    } catch (e) {
      alert("删除失败: " + (e.message || e));
    }
  }

  window.propertySelectedIds = new Set();

  function updatePropertySelectedStyles() {
    if (!propertyTable) return;

    const rows = propertyTable.querySelectorAll("tbody tr");
    rows.forEach((tr) => {
      const rowId = tr.getAttribute("data-id") || "";
      tr.classList.toggle("selected", window.propertySelectedIds.has(rowId));
    });

    const deleteSelectedButton = document.getElementById(
      "btnPropertyDeleteSelected"
    );
    if (deleteSelectedButton) {
      deleteSelectedButton.disabled = window.propertySelectedIds.size === 0;
    }
  }

  function togglePropertySelection(id) {
    if (!id) return;

    if (window.propertySelectedIds.has(id)) {
      window.propertySelectedIds.delete(id);
    } else {
      window.propertySelectedIds.add(id);
    }
    updatePropertySelectedStyles();
  }

  function clearPropertySelection() {
    window.propertySelectedIds.clear();
    updatePropertySelectedStyles();
  }

  async function batchDeleteProperties(ids) {
    if (!ids.length) return;
    if (!confirm("确定要批量删除选中的属性吗？")) return;

    for (const id of ids) {
      await deleteProperty(id, true);
    }
    clearPropertySelection();
    await loadPropertyList();
  }

  function bindPropertyEvents() {
    const propertyForm = byId("propertyForm");
    if (propertyForm) {
      propertyForm.addEventListener("submit", async (e) => {
        e.preventDefault();

        const id = byId("propId").value;
        const name = byId("propName").value.trim();
        const datatype = byId("propDatatype").value;
        const valuetype = byId("propValuetype").value.trim();

        if (!name) {
          alert("名称必填");
          return;
        }

        const url = id ? "/api/kb/property_update" : "/api/kb/property_create";
        const body = { name, datatype, valuetype };
        if (id) body.id = id;

        try {
          const resp = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
          });
          if (!resp.ok) throw new Error("HTTP " + resp.status);
          closePropertyModal();
          loadPropertyList();
        } catch (e) {
          alert("保存失败: " + (e.message || e));
        }
      });
    }

    const btnAdd = byId("btnPropertyAdd");
    if (btnAdd) {
      btnAdd.addEventListener("click", () => openPropertyModal("add"));
    }

    if (propertyTable) {
      propertyTable.addEventListener("click", (e) => {
        const btn = e.target.closest(".btnPropertyDelete");
        if (btn) {
          deleteProperty(btn.getAttribute("data-id"));
          return;
        }

        const tr = e.target.closest("tr");
        if (!tr || !tr.hasAttribute("data-id")) return;

        const id = tr.getAttribute("data-id");
        if (e.ctrlKey || e.metaKey) {
          togglePropertySelection(id);
          return;
        }

        if (e.shiftKey) {
          const rows = Array.from(propertyTable.querySelectorAll("tbody tr"));
          const ids = rows.map((row) => row.getAttribute("data-id") || "");
          const anchor = Array.from(window.propertySelectedIds)[0] || ids[0];
          const a = ids.indexOf(anchor);
          const b = ids.indexOf(id);
          if (a !== -1 && b !== -1) {
            const [start, end] = a < b ? [a, b] : [b, a];
            window.propertySelectedIds = new Set(ids.slice(start, end + 1));
            updatePropertySelectedStyles();
          }
          return;
        }

        window.propertySelectedIds = new Set([id]);
        updatePropertySelectedStyles();
      });
    }

    const btnDeleteSelected = byId("btnPropertyDeleteSelected");
    if (btnDeleteSelected) {
      btnDeleteSelected.addEventListener("click", () => {
        batchDeleteProperties(Array.from(window.propertySelectedIds));
      });
    }

    const btnRefresh = byId("btnPropertyRefresh");
    if (btnRefresh) {
      btnRefresh.addEventListener("click", () => loadPropertyList());
    }

    const btnMgmtRefresh = byId("btnPropertyMgmtRefresh");
    if (btnMgmtRefresh) {
      btnMgmtRefresh.addEventListener("click", () => loadPropertyList());
    }

    const propertyMgmtSearch = byId("propertyMgmtSearch");
    if (propertyMgmtSearch) {
      propertyMgmtSearch.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          propertyPage = 1;
          loadPropertyList();
        }
      });
    }

    const btnClearSearch = byId("btnPropertyClearSearch");
    if (btnClearSearch) {
      btnClearSearch.addEventListener("click", () => {
        const propertySearchInput = byId("propertySearch");
        if (propertySearchInput) propertySearchInput.value = "";
        window.propertySearch = "";
        propertyPage = 1;
        loadPropertyList();
      });
    }

    const propertySearchInput = byId("propertySearch");
    if (propertySearchInput) {
      propertySearchInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          window.propertySearch = propertySearchInput.value.trim();
          propertyPage = 1;
          loadPropertyList();
        }
      });
      propertySearchInput.addEventListener("input", (e) => {
        window.propertySearch = e.target.value.trim();
      });
    }

    const btnPrevPage = byId("btnPropertyPrevPage");
    if (btnPrevPage) {
      btnPrevPage.addEventListener("click", () => {
        if (propertyPage > 1) {
          propertyPage--;
          loadPropertyList();
        }
      });
    }

    const btnNextPage = byId("btnPropertyNextPage");
    if (btnNextPage) {
      btnNextPage.addEventListener("click", () => {
        const maxPage = Math.ceil(propertyTotal / propertyPageSize);
        if (propertyPage < maxPage) {
          propertyPage++;
          loadPropertyList();
        }
      });
    }

    const propertyPageSizeInput = byId("propertyPageSize");
    if (propertyPageSizeInput) {
      propertyPageSizeInput.addEventListener("change", (e) => {
        propertyPageSize = parseInt(e.target.value, 10) || 20;
        propertyPage = 1;
        loadPropertyList();
      });
    }

    if (typeof window.fetchKbStats === "function") {
      window.fetchKbStats();
    }
  }

  window.loadPropertyList = loadPropertyList;
  window.openPropertyModal = openPropertyModal;
  window.closePropertyModal = closePropertyModal;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bindPropertyEvents, {
      once: true,
    });
  } else {
    bindPropertyEvents();
  }
})();
