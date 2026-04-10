(function () {
  function getUrlParam(name) {
    const url = new URL(window.location.href);
    return url.searchParams.get(name);
  }

  function setUrlParam(name, value) {
    const previousValue = getUrlParam(name) || "";
    const url = new URL(window.location.href);
    url.searchParams.set(name, value);
    window.location.href = url.toString();
    try {
      window.dispatchEvent(
        new CustomEvent("kb:url-param-changed", {
          detail: { key: name, value: value || "", previousValue },
        }),
      );
    } catch {}
  }

  function getProjectDbId(project) {
    return String(
      (project && (project.slug || project.name || project.file)) || ""
    ).replace(/\.sqlite$/, "");
  }

  async function loadProjectListToModal(selectedDb) {
    const wrap = document.getElementById("dbProjectListWrap");
    const err = document.getElementById("dbSelectError");
    if (!wrap || !err) return;

    wrap.innerHTML = "";
    err.style.display = "none";

    let items = [];
    try {
      const resp = await fetch("/api/kb/list_projects");
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const data = await resp.json();
      items = Array.isArray(data.projects) ? data.projects : [];

      const currentDb = getUrlParam("db");
      items = items.filter((it) => getProjectDbId(it) !== currentDb);

      if (!items.length) {
        err.textContent = "No available projects yet.";
        err.style.display = "";
        return;
      }

      let selected = selectedDb || "";
      const searchEl = document.getElementById("dbProjectSearch");
      if (searchEl) {
        searchEl.value = "";
        searchEl.oninput = () => {
          const q = searchEl.value.trim().toLowerCase();
          let visible = 0;
          Array.from(wrap.children).forEach((c) => {
            const title = (c.dataset.title || "").toLowerCase();
            const desc = (c.dataset.desc || "").toLowerCase();
            if (q && !(title.includes(q) || desc.includes(q))) {
              c.style.display = "none";
            } else {
              c.style.display = "";
              visible++;
            }
          });
          if (visible === 0) {
            err.textContent = "No matching project.";
            err.style.display = "";
          } else {
            err.style.display = "none";
          }
        };
      }

      const esc = (s) =>
        String(s === undefined || s === null ? "" : s).replace(
          /[&<>"']/g,
          (c) =>
            ({
              "&": "&amp;",
              "<": "&lt;",
              ">": "&gt;",
              '"': "&quot;",
              "'": "&#39;",
            })[c]
        );

      items.forEach((it) => {
        const dbId = getProjectDbId(it);
        const titleText = it.title ? it.title : dbId;
        const descText = it.desc || it.description || "";
        const isSelected = selected === dbId;
        const card = document.createElement("div");
        card.className = "db-project-card" + (isSelected ? " selected" : "");
        card.tabIndex = 0;
        card.setAttribute("role", "button");
        card.dataset.db = dbId;
        card.dataset.title = titleText;
        card.dataset.desc = descText;

        let avatarHtml = "";
        if (it.image && typeof it.image === "string" && it.image.trim()) {
          avatarHtml = `<div class='db-avatar' style='background:#fff url(${it.image}) center/cover no-repeat;background-size:cover;'></div>`;
        } else {
          const src = (it.title || dbId || "").replace(/\s+/g, "");
          let initials = src ? src[0].toUpperCase() : "?";
          if (src.length > 1) initials += src[1];
          avatarHtml = `<div class='db-avatar' style='background:#e0e7ef;color:#4f46e5;'>${initials}</div>`;
        }

        card.innerHTML = `
          ${avatarHtml}
          <div class='db-project-title' title='${esc(titleText)}'>${esc(titleText)}</div>
          ${descText ? `<div class='db-project-desc' title='${esc(descText)}'>${esc(descText)}</div>` : ""}
          <div class='db-project-id'>${esc(dbId)}</div>
        `;

        card.onclick = () => {
          selected = dbId;
          Array.from(wrap.children).forEach((c) => c.classList.remove("selected"));
          card.classList.add("selected");
          wrap.dataset.selected = dbId;
          card.setAttribute("aria-selected", "true");
        };

        card.ondblclick = (e) => {
          e.preventDefault();
          e.stopPropagation();
          try {
            addModalProjectsToSidebar(items);
          } catch {}
          try {
            (window.setSidebarCollapsed || setSidebarCollapsed)(true, false);
          } catch {}
          try {
            hideDbSelectModal();
          } catch {}
          setUrlParam("db", dbId);
        };

        card.onkeydown = (e) => {
          if (e.key === "Enter" || e.key === " ") {
            card.click();
          }
        };

        if (isSelected) {
          setTimeout(() => {
            card.focus();
          }, 0);
          wrap.dataset.selected = dbId;
        }

        wrap.appendChild(card);
      });
    } catch (e) {
      err.textContent = "Load failed: " + (e.message || e);
      err.style.display = "";
    }

    try {
      addModalProjectsToSidebar(items);
    } catch {}
  }

  function addModalProjectsToSidebar(items) {
    try {
      if (!Array.isArray(items) || !items.length) return;
      const wrap = document.querySelector(".project-list");
      if (!wrap) return;

      try {
        const first = wrap.firstElementChild;
        if (first && first.classList && first.classList.contains("muted")) {
          first.remove();
        }
      } catch {}

      const currentDb = getUrlParam("db") || "";
      for (let i = items.length - 1; i >= 0; i--) {
        const it = items[i];
        const dbId = getProjectDbId(it);
        if (!dbId || dbId === currentDb) continue;
        if (wrap.querySelector(`.project-entry[data-db="${dbId}"]`)) continue;

        const entry = document.createElement("a");
        entry.className = "project-entry";
        entry.href = "#";
        entry.setAttribute("data-db", dbId);
        entry.setAttribute("data-image", it.image || "");
        entry.setAttribute("data-title", it.title || "");
        entry.setAttribute("data-desc", it.desc || it.description || "");
        const title = it.title ? it.title : dbId;
        entry.title = title + " (double click to edit)";
        entry.setAttribute("aria-label", title);
        entry.tabIndex = 0;

        if (it.image && typeof it.image === "string" && it.image.trim()) {
          const img = document.createElement("span");
          img.className = "project-avatar-img";
          img.style.backgroundImage = `url(${it.image})`;
          entry.appendChild(img);
        } else {
          const initials =
            (it.title || dbId || "?").replace(/\s+/g, "").slice(0, 2).toUpperCase() || "?";
          const span = document.createElement("span");
          span.className = "project-initials";
          span.textContent = initials;
          entry.appendChild(span);
        }

        const label = document.createElement("span");
        label.className = "project-entry-label";
        try {
          const base = (title || dbId || "").toString().trim();
          label.dataset.full = base;
          label.dataset.short = Array.from(base).slice(0, 3).join("");
          label.textContent = base;
          label.title = base;
          const sidebarEl = document.getElementById("projectSidebar");
          const isCollapsed = sidebarEl && sidebarEl.classList.contains("collapsed");
          label.setAttribute("aria-hidden", String(!!isCollapsed));
        } catch {
          label.textContent = title;
        }
        entry.appendChild(label);

        entry.addEventListener("click", (e) => {
          e.preventDefault();
          e.stopPropagation();
          if (entry._clickTimer) return;
          entry._clickTimer = setTimeout(() => {
            entry._clickTimer = null;
            try {
              selectProjectWithAnimation(
                dbId,
                entry,
                title,
                entry.getAttribute("data-image") || ""
              );
            } catch {
              try {
                setUrlParam("db", dbId);
              } catch {}
            }
          }, 260);
        });

        entry.addEventListener("dblclick", (e) => {
          e.preventDefault();
          e.stopPropagation();
          if (entry._clickTimer) {
            clearTimeout(entry._clickTimer);
            entry._clickTimer = null;
          }
          try {
            updateHeaderProjectInfo(
              dbId,
              title,
              entry.getAttribute("data-image") || ""
            );
          } catch {}
          openEditProjectModal(
            dbId,
            entry.getAttribute("data-title") || "",
            entry.getAttribute("data-image") || "",
            entry.getAttribute("data-desc") || ""
          );
        });

        entry.addEventListener("keydown", (e) => {
          const key = e.key || e.keyCode;
          if (key === "Enter" || key === 13) {
            e.preventDefault();
            try {
              selectProjectWithAnimation(
                dbId,
                entry,
                title,
                entry.getAttribute("data-image") || ""
              );
            } catch {
              try {
                setUrlParam("db", dbId);
              } catch {}
            }
          }
        });

        try {
          wrap.insertBefore(entry, wrap.firstChild);
        } catch {
          wrap.appendChild(entry);
        }
      }
    } catch (e) {
      console.warn("addModalProjectsToSidebar failed", e);
    }
  }

  function showDbSelectModal() {
    const modal = document.getElementById("dbSelectModal");
    if (!modal) return;
    modal.style.display = "flex";
    modal.style.alignItems = "center";
    modal.style.justifyContent = "center";
    loadProjectListToModal(getUrlParam("db"));
    setTimeout(() => {
      const s = document.getElementById("dbProjectSearch");
      if (s) s.focus();
    }, 80);
    try {
      document.body.style.overflow = "hidden";
    } catch {}
    try {
      if (!modal._backdropHandler) {
        modal._backdropHandler = (e) => {
          if (e.target === modal) hideDbSelectModal();
        };
        modal.addEventListener("click", modal._backdropHandler);
      }
    } catch {}
    try {
      if (!modal._escHandler) {
        modal._escHandler = (ev) => {
          if (ev.key === "Escape") hideDbSelectModal();
        };
        document.addEventListener("keydown", modal._escHandler);
      }
    } catch {}
  }

  function hideDbSelectModal() {
    const modal = document.getElementById("dbSelectModal");
    if (modal) {
      modal.style.display = "none";
      try {
        document.body.style.overflow = "";
      } catch {}
      try {
        if (modal._escHandler) {
          document.removeEventListener("keydown", modal._escHandler);
          modal._escHandler = null;
        }
      } catch {}
      try {
        if (modal._backdropHandler) {
          modal.removeEventListener("click", modal._backdropHandler);
          modal._backdropHandler = null;
        }
      } catch {}
    }
  }

  async function loadDbList() {
    const dbSwitcher = document.getElementById("dbSwitcher");
    if (!dbSwitcher) return;

    try {
      const resp = await fetch("/api/kb/list_projects");
      const data = await resp.json();
      if (Array.isArray(data.projects)) {
        data.projects.forEach((p) => {
          const projectName =
            typeof p === "string"
              ? p.replace(/\.sqlite$/, "")
              : (p.name || p.file || "").toString().replace(/\.sqlite$/, "");
          const title = typeof p === "object" && p.title ? p.title : projectName;
          const opt = document.createElement("option");
          opt.value = projectName;
          opt.textContent =
            title + (projectName !== title ? ` (${projectName})` : "");
          dbSwitcher.appendChild(opt);
        });

        const params = new URLSearchParams(window.location.search);
        const dbName = params.get("db");
        if (dbName) {
          dbSwitcher.value = dbName;
          const found = data.projects.find(
            (p) =>
              (typeof p === "string"
                ? p.replace(/\.sqlite$/, "")
                : (p.name || p.file || "").toString().replace(/\.sqlite$/, "")) ===
              dbName
          );
          if (found) {
            const currentTitle =
              typeof found === "object" && found.title ? found.title : dbName;
            try {
              const img =
                typeof found === "object" && (found.image || found.img)
                  ? found.image || found.img
                  : "";
              updateHeaderProjectInfo(dbName, currentTitle, img);
            } catch {}
          }
        }
      }
    } catch {}

    const mainTitle = document.getElementById("mainTitle");
    if (mainTitle) {
      mainTitle.innerHTML = '<span style="color:#222;">Knowledge Graph</span>';
      try {
        updateSidebarLabelMode();
      } catch {}
    }
  }

  function initDbSwitcher() {
    document.addEventListener("DOMContentLoaded", loadDbList);
    const btnRefreshDbList = document.getElementById("btnRefreshDbList");
    if (btnRefreshDbList) btnRefreshDbList.onclick = loadDbList;
    const dbSwitcher = document.getElementById("dbSwitcher");
    if (dbSwitcher) {
      dbSwitcher.onchange = async function () {
        const name = dbSwitcher.value;
        if (!name) return;
        try {
          const resp = await fetch(
            `/api/kb/create_project?name=${encodeURIComponent(
              name.replace(/\.sqlite$/, "")
            )}`
          );
          const text = await resp.text();
          let data = null;
          try {
            data = text ? JSON.parse(text) : null;
          } catch {
            data = null;
          }
          if (!resp.ok) {
            alert(
              data && data.message ? data.message : text || `Switch failed: HTTP ${resp.status}`
            );
            return;
          }
          if (data && data.success) {
            const nextUrl = new URL(window.location.href);
            nextUrl.searchParams.set("db", name.replace(/\.sqlite$/, ""));
            window.location.href = nextUrl.toString();
          } else {
            alert(data && data.message ? data.message : "Switch failed");
          }
        } catch {
          alert("Switch failed");
        }
      };
    }
  }

  function initFirstNodeSelection() {
    document.addEventListener("DOMContentLoaded", function () {
      if (window.kbCy && typeof window.kbCy.nodes === "function") {
        setTimeout(function () {
          const nodes = window.kbCy.nodes();
          if (nodes && nodes.length > 0) {
            nodes[0].select();
            window.kbCy.center(nodes[0]);
          }
        }, 500);
      }
    });
  }

  function initDbParamBootstrap() {
    document.addEventListener("DOMContentLoaded", async function () {
      const params = new URLSearchParams(window.location.search);
      const dbName = params.get("db");
      if (dbName) {
        try {
          const resp = await fetch(
            `/api/kb/create_project?name=${encodeURIComponent(dbName)}`
          );
          const text = await resp.text();
          try {
            JSON.parse(text || "null");
          } catch {}
        } catch {}
      }
    });
  }

  function initProjectCreationModal() {
    const btnCreateProject = document.getElementById("btnCreateProject");
    const createProjectModal = document.getElementById("createProjectModal");
    const inputProjectName = document.getElementById("inputProjectName");
    const btnCancelCreateProject = document.getElementById(
      "btnCancelCreateProject"
    );
    const createProjectError = document.getElementById("createProjectError");

    function generateProjectDbName() {
      const now = new Date();
      const y = now.getFullYear();
      const m = String(now.getMonth() + 1).padStart(2, "0");
      const d = String(now.getDate()).padStart(2, "0");
      const rand = Math.random().toString(36).slice(2, 8);
      return `project_${y}${m}${d}_${rand}`;
    }

    function openCreateProjectModal() {
      if (!createProjectModal) return;
      createProjectModal.style.display = "flex";
      try {
        if (inputProjectName) inputProjectName.value = generateProjectDbName();
        try {
          if (window.inputProjectCreateImageUrl) {
            window.inputProjectCreateImageUrl.value = "";
          }
        } catch {}
        try {
          if (window.createProjectPreview) {
            window.createProjectPreview.style.display = "none";
            window.createProjectPreviewImg.style.backgroundImage = "";
            window.createProjectPreviewStatus.textContent = "";
          }
        } catch {}
      } catch {}
      try {
        if (createProjectError) createProjectError.style.display = "none";
      } catch {}
    }

    if (btnCreateProject && createProjectModal) {
      btnCreateProject.onclick = openCreateProjectModal;
    }
    if (btnCancelCreateProject && createProjectModal) {
      btnCancelCreateProject.onclick = () => {
        createProjectModal.style.display = "none";
      };
    }

    window.openCreateProjectModal = openCreateProjectModal;
  }

  function initDbModalStartup() {
    document.addEventListener("DOMContentLoaded", () => {
      const oldDbSwitcher = document.getElementById("dbSwitcher");
      if (oldDbSwitcher && oldDbSwitcher.parentElement) {
        oldDbSwitcher.parentElement.remove();
      }
      const oldRefresh = document.getElementById("btnRefreshDbList");
      if (oldRefresh) oldRefresh.remove();

      try {
        if (!getUrlParam("db")) {
          if (!sessionStorage.getItem("kbDbSelectModalShown")) {
            sessionStorage.setItem("kbDbSelectModalShown", "1");
            showDbSelectModal();
          }
        }
      } catch {}

      try {
        const am = document.getElementById("authModal");
        if (am && am.parentElement !== document.body) {
          document.body.appendChild(am);
          try {
            am.style.zIndex = "99999";
          } catch {}
        }
      } catch {}
    });
  }

  window.getUrlParam = getUrlParam;
  window.setUrlParam = setUrlParam;
  window.loadProjectListToModal = loadProjectListToModal;
  window.addModalProjectsToSidebar = addModalProjectsToSidebar;
  window.showDbSelectModal = showDbSelectModal;
  window.hideDbSelectModal = hideDbSelectModal;
  window.loadDbList = loadDbList;

  initDbSwitcher();
  initFirstNodeSelection();
  initDbParamBootstrap();
  initProjectCreationModal();
  initDbModalStartup();
})();
