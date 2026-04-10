(function () {
  function bindDbSwitchButton() {
    const btnDbSwitch = document.getElementById("btnDbSwitch");
    if (btnDbSwitch) {
      btnDbSwitch.onclick = function () {
        if (typeof window.showDbSelectModal === "function") {
          window.showDbSelectModal(true);
        }
      };
    }
  }

  function updateUrlParam(key, value) {
    if (!window || !window.location || !window.history) {
      return;
    }
    let previousValue = "";
    try {
      previousValue = new URL(window.location.href).searchParams.get(key) || "";
    } catch {}
    const currentUrl = new URL(window.location.href);
    if (value === null || value === undefined || value === "") {
      currentUrl.searchParams.delete(key);
    } else {
      currentUrl.searchParams.set(key, value);
    }
    const nextUrl = currentUrl.pathname + currentUrl.search + currentUrl.hash;
    window.history.replaceState(null, "", nextUrl);
    try {
      window.dispatchEvent(
        new CustomEvent("kb:url-param-changed", {
          detail: { key, value: value || "", previousValue },
        }),
      );
    } catch {}
  }

  function getCurrentDbParam() {
    try {
      const currentUrl = new URL(window.location.href);
      return (currentUrl.searchParams.get("db") || "").trim();
    } catch {
      return "";
    }
  }

  function appendCurrentDbParam(input) {
    try {
      const db = getCurrentDbParam();
      const url =
        input instanceof URL
          ? new URL(input.toString())
          : new URL(String(input), window.location.origin);
      if (db) {
        url.searchParams.set("db", db);
      } else {
        url.searchParams.delete("db");
      }
      return url;
    } catch {
      return input;
    }
  }

  function initDetailPanelHeight() {
    const detail = document.getElementById("detailPanel");
    const header = document.querySelector("header");

    function updateDetailMaxHeight() {
      if (!detail) return;
      const winH = window.innerHeight || document.documentElement.clientHeight;
      const headerH = header ? header.getBoundingClientRect().height : 0;
      const mainPadding = 24;
      const margin = 24;
      const max = Math.max(
        200,
        Math.floor(winH - headerH - mainPadding - margin)
      );
      detail.style.maxHeight = max + "px";
    }

    let resizeTimer = null;
    window.addEventListener("resize", () => {
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(updateDetailMaxHeight, 120);
    });

    document.addEventListener("DOMContentLoaded", updateDetailMaxHeight);
    updateDetailMaxHeight();
  }

  function initKbStats() {
    const formatter = new Intl.NumberFormat("zh-CN");
    const statElements = {
      entity: document.getElementById("kb-stat-entity"),
      link: document.getElementById("kb-stat-link"),
      instance: document.getElementById("kb-stat-instance"),
      property: document.getElementById("kb-stat-property"),
    };

    function formatStatValue(value) {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        return "--";
      }

      const units = [
        { threshold: 1e8, suffix: "亿" },
        { threshold: 1e4, suffix: "万" },
      ];
      for (const { threshold, suffix } of units) {
        if (value >= threshold) {
          const scaled = value / threshold;
          const digits = scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2;
          return Number(scaled.toFixed(digits)).toString() + suffix;
        }
      }
      return formatter.format(value);
    }

    async function fetchKbStats() {
      const entries = Object.entries(statElements);
      if (!entries.length) return;

      for (const [, el] of entries) {
        if (el) el.textContent = "--";
      }

      try {
        const statsUrl = appendCurrentDbParam("/api/kb/stats");
        const resp = await fetch(
          statsUrl instanceof URL ? statsUrl.toString() : statsUrl,
        );
        if (!resp.ok) throw new Error("HTTP " + resp.status);

        const data = await resp.json();
        const counts = (data && data.counts) || data || {};
        for (const [key, el] of entries) {
          if (!el) continue;
          el.textContent = formatStatValue(counts[key]);
        }
      } catch (err) {
        if (window.console && console.warn) {
          console.warn("加载统计失败", err);
        }
      }
    }

    window.fetchKbStats = fetchKbStats;
  }

  function initStatusTextOverflow() {
    try {
      const status = document.getElementById("status");
      const text = status ? status.querySelector("span") : null;
      if (!text) return;

      text.style.whiteSpace = "nowrap";
      text.style.overflow = "hidden";
      text.style.textOverflow = "ellipsis";
    } catch {}
  }

  function initViewportHeightWatcher() {
    function updateMainViewportHeight() {
      try {
        const mainEl = document.querySelector("main");
        if (!mainEl) return;

        const headerEl = document.querySelector("header");
        const statusEl = document.getElementById("kb-statusbar");
        const viewport =
          window.innerHeight || document.documentElement.clientHeight || 0;
        const headerHeight = headerEl ? headerEl.offsetHeight : 0;
        const statusHeight = statusEl ? statusEl.offsetHeight : 0;
        const targetHeight = Math.max(
          0,
          viewport - headerHeight - statusHeight
        );

        mainEl.style.height = targetHeight + "px";
        mainEl.style.maxHeight = targetHeight + "px";

        const styles = window.getComputedStyle(mainEl);
        const padTop = parseFloat(styles.paddingTop) || 0;
        const padBottom = parseFloat(styles.paddingBottom) || 0;
        const innerHeight = Math.max(0, targetHeight - padTop - padBottom);
        const split = mainEl.querySelector(".kb-split");
        if (split) {
          split.style.height = innerHeight + "px";
          split.style.maxHeight = innerHeight + "px";
          split.style.minHeight = innerHeight + "px";
        }
      } catch (err) {
        if (window.console && console.warn) {
          console.warn("updateMainViewportHeight", err);
        }
      }

      try {
        scheduleEntryGridResize();
      } catch {}
    }

    window.updateMainViewportHeight = updateMainViewportHeight;

    const run = () => {
      updateMainViewportHeight();
      if (!window.kbViewportResizeAttached) {
        window.addEventListener("resize", updateMainViewportHeight);
        window.kbViewportResizeAttached = true;
      }

      try {
        const headerEl = document.querySelector("header");
        const statusEl = document.getElementById("kb-statusbar");
        if (window.kbLayoutResizeObserver) {
          window.kbLayoutResizeObserver.disconnect();
        }

        if (typeof ResizeObserver === "function") {
          const ro = new ResizeObserver(() => updateMainViewportHeight());
          if (headerEl) ro.observe(headerEl);
          if (statusEl) ro.observe(statusEl);
          window.kbLayoutResizeObserver = ro;
        }
      } catch (err) {
        if (window.console && console.warn) {
          console.warn("initViewportHeightWatcher", err);
        }
      }
    };

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run, { once: true });
    } else {
      run();
    }
  }

  function initClassModal() {
    document.addEventListener("DOMContentLoaded", () => {
      const btnClsAdd = document.getElementById("btnClsAdd");
      if (btnClsAdd) {
        btnClsAdd.addEventListener("click", () => {
          const form = document.getElementById("classForm");
          if (form) form.reset();

          const modal = document.getElementById("classModal");
          if (!modal) return;

          const title = document.getElementById("classModalTitle");
          if (title) {
            if (window.kbSelectedClassId) {
              let label = window.kbSelectedClassId;
              try {
                const el = document.querySelector(
                  `.cls-item[data-id="${window.kbSelectedClassId}"] span:last-child`
                );
                if (el) label = el.textContent;
              } catch {}
              title.textContent = "New child class (parent: " + label + ")";
            } else {
              title.textContent = "New root class";
            }
          }

          modal.style.display = "flex";
          modal.style.alignItems = "center";
          modal.style.justifyContent = "center";
        });
      }

      const btnClsDelete = document.getElementById("btnClsDelete");
      if (btnClsDelete) {
        btnClsDelete.addEventListener("click", async () => {
          if (!window.kbSelectedClassId) {
            return alert("Please select a class first.");
          }
          if (!confirm("Delete the selected class? This cannot be undone.")) {
            return;
          }
          try {
            const resp = await fetch(
              "/api/kb/classes?id=" +
                encodeURIComponent(window.kbSelectedClassId),
              { method: "DELETE" }
            );
            if (!resp.ok) throw new Error("HTTP " + resp.status);

            window.kbSelectedClassId = null;
            btnClsDelete.style.display = "none";

            if (typeof window.loadClasses === "function") {
              window.loadClasses();
            } else {
              const btnRefresh = document.getElementById("btnClsRefresh");
              if (btnRefresh) btnRefresh.click();
            }
          } catch (e) {
            alert("Delete failed: " + e.message);
          }
        });
      }

      const classForm = document.getElementById("classForm");
      if (classForm) {
        classForm.addEventListener("submit", async (e) => {
          e.preventDefault();
          const nameInput = document.getElementById("clsNameInput");
          const descInput = document.getElementById("clsDescInput");
          const name = nameInput ? nameInput.value.trim() : "";
          const desc = descInput ? descInput.value.trim() : "";

          if (!name) return alert("Name is required.");

          const parent_id = window.kbSelectedClassId || "";

          try {
            const resp = await fetch("/api/kb/classes", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ name, description: desc, parent_id }),
            });
            if (!resp.ok) throw new Error("HTTP " + resp.status);

            document.getElementById("classModal").style.display = "none";
            if (typeof window.loadClasses === "function") {
              window.loadClasses();
            } else {
              const btnRefresh = document.getElementById("btnClsRefresh");
              if (btnRefresh) btnRefresh.click();
            }
          } catch (e) {
            alert("Create failed: " + e.message);
          }
        });
      }
    });
  }

  function updateToc() {
    const view = document.getElementById("wikiView");
    if (!view) return;

    const headers = view.querySelectorAll("h1, h2, h3, h4, h5, h6");
    const btnToc = document.getElementById("btnToc");
    const tocPopup = document.getElementById("tocPopup");

    if (!btnToc || !tocPopup) return;

    if (headers.length === 0) {
      btnToc.style.display = "none";
      tocPopup.innerHTML = "";
      return;
    }

    btnToc.style.display = "flex";
    tocPopup.innerHTML = "";

    headers.forEach((h, index) => {
      if (!h.id) h.id = "wiki-header-" + index;

      const item = document.createElement("div");
      const level = parseInt(h.tagName.substring(1), 10);
      item.className = "toc-item toc-h" + level;
      item.textContent = h.textContent;
      item.onclick = (e) => {
        e.stopPropagation();
        h.scrollIntoView({ behavior: "smooth", block: "start" });
        tocPopup.style.display = "none";
      };
      tocPopup.appendChild(item);
    });
  }

  function initToc() {
    const btnToc = document.getElementById("btnToc");
    const tocPopup = document.getElementById("tocPopup");
    const view = document.getElementById("wikiView");

    if (btnToc && tocPopup) {
      btnToc.addEventListener("click", (e) => {
        e.stopPropagation();
        tocPopup.style.display =
          tocPopup.style.display === "none" ? "block" : "none";
      });

      document.addEventListener("click", (e) => {
        if (
          tocPopup.style.display !== "none" &&
          !tocPopup.contains(e.target) &&
          !btnToc.contains(e.target)
        ) {
          tocPopup.style.display = "none";
        }
      });
    }

    if (view) {
      const observer = new MutationObserver(() => {
        updateToc();
      });
      observer.observe(view, { childList: true, subtree: true });
    }
  }

  window.updateToc = updateToc;
  window.updateUrlParam = updateUrlParam;
  window.getCurrentDbParam = getCurrentDbParam;
  window.appendCurrentDbParam = appendCurrentDbParam;

  bindDbSwitchButton();
  initDetailPanelHeight();
  initKbStats();
  initStatusTextOverflow();
  initViewportHeightWatcher();
  initClassModal();
  initToc();
})();
