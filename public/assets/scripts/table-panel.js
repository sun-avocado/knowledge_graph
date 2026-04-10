(function () {
  const urlParams = new URLSearchParams(window.location.search);
  let tblPage = parseInt(urlParams.get("page") || "1", 10);
  let tblPageSize = parseInt(urlParams.get("limit") || "20", 10);
  let tblTotalNodes = 0;
  let tblActiveClassId = "";
  let tblActiveClassLabel = "";

  const btnPrevPage = document.getElementById("btnPrevPage");
  const btnNextPage = document.getElementById("btnNextPage");
  const tblPageInfo = document.getElementById("tblPageInfo");
  const tblSortSelect = document.getElementById("tblSort");
  const tblPageSizeSelect = document.getElementById("tblPageSize");
  const tblSearch = document.getElementById("tblSearch");
  const tblCount = document.getElementById("tblCount");

  if (tblPageSizeSelect) {
    tblPageSizeSelect.value = tblPageSize.toString();
  }

  function getUrlParams() {
    const params = new URLSearchParams(window.location.search || "");
    const node = params.get("node") || "";
    const label = params.get("label") || "";
    const view = (params.get("view") || "").toLowerCase();
    const order = params.get("order") || "";
    return { node, label, view, order };
  }

  function normalizeClassIdForQuery(rawId) {
    if (!rawId) return "";
    const id = String(rawId).trim();
    if (!id) return "";
    if (id.startsWith("entity/")) {
      return id.slice("entity/".length) || "";
    }
    return id;
  }

  function updateTblPageInfo() {
    const maxPage = Math.max(1, Math.ceil(tblTotalNodes / tblPageSize));
    if (tblPageInfo) {
      tblPageInfo.textContent = `第 ${tblPage} / ${maxPage} 页 · 共 ${tblTotalNodes} 条`;
    }
    if (btnPrevPage) btnPrevPage.disabled = tblPage <= 1;
    if (btnNextPage) btnNextPage.disabled = tblPage >= maxPage;
  }

  async function loadTablePage(options = {}) {
    const opts = options || {};
    const hasClassId = Object.prototype.hasOwnProperty.call(opts, "classId");
    if (hasClassId) {
      const incomingId = opts.classId || "";
      if (incomingId !== tblActiveClassId && opts.resetPage !== false) {
        tblPage = 1;
      }
      tblActiveClassId = incomingId;
    }
    if (opts.resetPage) {
      tblPage = 1;
    }
    if (Object.prototype.hasOwnProperty.call(opts, "classLabel")) {
      tblActiveClassLabel = opts.classLabel || "";
    } else if (hasClassId && !opts.classId) {
      tblActiveClassLabel = "";
    }

    if (typeof window.setStatus === "function") {
      window.setStatus(true, "加载中…");
    }

    try {
      const offset = (tblPage - 1) * tblPageSize;
      const url = new URL("/api/kb/entity_search", window.location.origin);
      if (typeof window.appendCurrentDbParam === "function") {
        const scopedUrl = window.appendCurrentDbParam(url);
        if (scopedUrl instanceof URL) {
          url.search = scopedUrl.search;
        }
      }
      url.searchParams.set("limit", tblPageSize);
      url.searchParams.set("offset", offset);

      const sortOrder = tblSortSelect ? tblSortSelect.value : "";
      if (typeof window.updateUrlParam === "function") {
        window.updateUrlParam("order", sortOrder === "id" ? "" : sortOrder);
        window.updateUrlParam("page", tblPage);
        window.updateUrlParam("limit", tblPageSize);
      }

      if (sortOrder && sortOrder !== "id") {
        url.searchParams.set("order", sortOrder);
      }

      const keyword = tblSearch ? (tblSearch.value || "").trim() : "";
      if (keyword) url.searchParams.set("q", keyword);
      if (tblActiveClassId) url.searchParams.set("class_id", tblActiveClassId);

      const resp = await fetch(url);
      if (!resp.ok) throw new Error("HTTP " + resp.status);

      const data = await resp.json();
      const nodes = Array.isArray(data.nodes) ? data.nodes : [];
      const tableList = nodes.map((item) => ({
        label_zh: item.label || "",
        id: item.id || "",
      }));

      window.kbTableNodes = tableList;
      tblTotalNodes = data.total || tableList.length;
      updateTblPageInfo();

      if (tblCount) {
        const parts = [`总计 ${tblTotalNodes} 条`];
        if (tblActiveClassLabel) parts.push(`分类 ${tblActiveClassLabel}`);
        else if (tblActiveClassId) parts.push(`分类 ${tblActiveClassId}`);
        tblCount.textContent = parts.join(" · ");
      }

      if (typeof window.renderTableList === "function") {
        window.renderTableList();
      }

      const labelHint = tblActiveClassLabel
        ? ` · 分类 ${tblActiveClassLabel}`
        : tblActiveClassId
        ? ` · 分类 ${tblActiveClassId}`
        : "";
      if (typeof window.setStatus === "function") {
        window.setStatus(false, `已加载 ${tableList.length} 条${labelHint}`);
      }
    } catch (e) {
      if (typeof window.setStatus === "function") {
        window.setStatus(false, "加载失败");
      }
      alert("加载失败: " + (e.message || e));
    }
  }

  async function loadInstancesForClass(classId, options = {}) {
    const normalizedId = normalizeClassIdForQuery(classId);
    const nextOptions = {
      classId: normalizedId,
      resetPage: options.resetPage !== false,
      classLabel: options.classLabel || "",
    };
    await loadTablePage(nextOptions);
  }

  function initTablePanel() {
    const initial = getUrlParams();
    if (tblSortSelect && initial.order) {
      const allowedSortValues = Array.from(tblSortSelect.options || []).map(
        (opt) => opt.value
      );
      if (allowedSortValues.includes(initial.order)) {
        tblSortSelect.value = initial.order;
      }
    }

    if (btnPrevPage) {
      btnPrevPage.addEventListener("click", () => {
        if (tblPage > 1) {
          tblPage--;
          loadTablePage();
        }
      });
    }

    if (btnNextPage) {
      btnNextPage.addEventListener("click", () => {
        const maxPage = Math.ceil(tblTotalNodes / tblPageSize);
        if (tblPage < maxPage) {
          tblPage++;
          loadTablePage();
        }
      });
    }

    if (tblPageSizeSelect) {
      tblPageSizeSelect.addEventListener("change", () => {
        tblPageSize = parseInt(tblPageSizeSelect.value, 10) || 20;
        tblPage = 1;
        loadTablePage();
      });
    }

    if (tblSortSelect) {
      tblSortSelect.addEventListener("change", () => {
        tblPage = 1;
        loadTablePage();
      });
    }

    if (tblSearch) {
      tblSearch.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          tblPage = 1;
          loadTablePage();
        }
      });
    }

    loadTablePage();
  }

  window.normalizeClassIdForQuery = normalizeClassIdForQuery;
  window.loadTablePage = loadTablePage;
  window.loadInstancesForClass = loadInstancesForClass;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initTablePanel, {
      once: true,
    });
  } else {
    initTablePanel();
  }
})();
