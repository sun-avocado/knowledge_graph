(function () {
// Inline detail panel helper
// ----------------------
const shared = window.kbApp || {};
const state = shared.state || {};
if (typeof state.bindAlias === "function") {
  state.bindAlias("kbActiveDetailNodeId", "activeDetailNodeId", "");
  state.bindAlias("kbActiveDetailRouteId", "activeDetailRouteId", "");
  state.bindAlias("kbActiveVisNodeId", "activeVisNodeId", "");
}
if (typeof marked !== "undefined" && typeof marked.setOptions === "function") {
  marked.setOptions({
    gfm: true,
    breaks: false,
  });
}

function hideDetailPanel() {
  try {
    const dp = document.getElementById("detailPanel");
    if (dp) dp.style.display = "none";
    // show default table/vis area depending on current mode
    if (window.kbViewMode === "vis") {
      if (cywrap) cywrap.style.display = "";
    }
    if (window.kbViewMode === "table") {
      if (tablePanel) tablePanel.style.display = "";
    }
    // Do not alter btnViewDetail here; setViewMode handles toggle state.
  } catch {}
}

// Helpers (adapted from kb_detail.html)
function setText(el, text) {
  if (el) el.textContent = text || "";
}
function formatPropValue(v) {
  if (v === null || typeof v === "undefined") return "";
  if (
    typeof v === "string" ||
    typeof v === "number" ||
    typeof v === "boolean"
  )
    return String(v);
  if (
    v &&
    typeof v === "object" &&
    (typeof v.date === "string" || typeof v.time === "string")
  ) {
    const t = extractDateString(v);
    if (t) return t;
  }
  if (v && typeof v === "object" && typeof v.amount !== "undefined") {
    const amt = v.amount;
    const unit = v.unit || "";
    return unit ? `${amt} ${unit}` : String(amt);
  }
  if (
    v &&
    typeof v === "object" &&
    typeof v.latitude !== "undefined" &&
    typeof v.longitude !== "undefined"
  ) {
    return `${v.latitude}, ${v.longitude}`;
  }
  try {
    if (
      v &&
      typeof v === "object" &&
      (v["entity-type"] ||
        v["entity_type"] ||
        v.id ||
        v["numeric-id"] ||
        v.numeric_id)
    ) {
      const lbl =
        v["entity_label_zh"] ??
        v.entity_label_zh ??
        v["label_zh"] ??
        v.label_zh ??
        null;
      if (lbl != null && String(lbl).trim() !== "") return String(lbl);
      const n = v["numeric-id"] ?? v.numeric_id ?? null;
      if (n != null) return String(n);
      if (v.id) return String(v.id);
    }
  } catch (e) {}
  if (v && typeof v === "object" && typeof v.url === "string")
    return v.url;
  try {
    return JSON.stringify(v, null, 0);
  } catch {
    return String(v);
  }
}

function isImageUrl(u) {
  try {
    if (!u || typeof u !== "string") return false;
    if (u.startsWith("data:image/")) return true;
    const lower = u.split("?")[0].toLowerCase();
    return /\.(png|jpg|jpeg|gif|webp|bmp|svg)$/.test(lower);
  } catch {
    return false;
  }
}
function extractImageUrls(v, allowLoose = false) {
  const out = [];
  try {
    if (!v) return out;
    if (typeof v === "string") {
      const trimmed = v.trim();
      if (!trimmed) return out;
      if (
        isImageUrl(trimmed) ||
        (allowLoose && /^https?:\/\//i.test(trimmed))
      )
        out.push(trimmed);
    } else if (Array.isArray(v)) {
      for (const it of v) out.push(...extractImageUrls(it, allowLoose));
    } else if (typeof v === "object") {
      const possibleKeys = [
        "url",
        "href",
        "link",
        "value",
        "image",
        "thumbnail",
        "thumb",
        "source",
        "src",
      ];
      for (const key of possibleKeys) {
        const val = v[key];
        if (typeof val === "string") {
          const trimmed = val.trim();
          if (!trimmed) continue;
          if (
            isImageUrl(trimmed) ||
            (allowLoose && /^https?:\/\//i.test(trimmed))
          )
            out.push(trimmed);
        }
      }
      for (const key of Object.keys(v)) {
        const val = v[key];
        if (typeof val === "string") {
          const trimmed = val.trim();
          if (!trimmed) continue;
          if (
            isImageUrl(trimmed) ||
            (allowLoose && /^https?:\/\//i.test(trimmed))
          )
            out.push(trimmed);
        } else if (typeof val === "object")
          out.push(...extractImageUrls(val, allowLoose));
        else if (Array.isArray(val))
          out.push(...extractImageUrls(val, allowLoose));
      }
    }
  } catch {}
  return out;
}
function normalizeImageUrl(url) {
  try {
    const trimmed = (url || "").trim();
    if (!trimmed) return "";
    return trimmed;
  } catch {
    return url;
  }
}
function addImageCandidate(list, seen, url, label, force = false) {
  try {
    if (!url || typeof url !== "string") return;
    const normalized = normalizeImageUrl(url);
    const src = (normalized || url || "").trim();
    if (!src) return;
    if (!force && !isImageUrl(src)) return;
    if (seen.has(src)) return;
    seen.add(src);
    const text = label ? String(label).trim() : "";
    list.push({ url: src, label: text });
  } catch {}
}

function pickLabelValue(value) {
  if (!value) return "";
  if (typeof value === "string" || typeof value === "number")
    return String(value).trim();
  if (Array.isArray(value)) {
    for (const item of value) {
      const txt = pickLabelValue(item);
      if (txt) return txt;
    }
    return "";
  }
  if (typeof value === "object") {
    const candidateKeys = [
      "zh",
      "zh-cn",
      "zh-hans",
      "cn",
      "chs",
      "label",
      "name",
      "title",
      "text",
      "value",
    ];
    for (const key of candidateKeys) {
      const nested = value[key];
      if (typeof nested === "string" || typeof nested === "number") {
        const txt = String(nested).trim();
        if (txt) return txt;
      }
    }
    for (const key of Object.keys(value)) {
      const nested = value[key];
      const txt = pickLabelValue(nested);
      if (txt) return txt;
    }
  }
  return "";
}

function getAttributeLabel(attr) {
  if (!attr || typeof attr !== "object") return "";
  return (
    pickLabelValue(attr.label_zh) ||
    pickLabelValue(attr.label) ||
    pickLabelValue(attr.name_zh) ||
    pickLabelValue(attr.name) ||
    pickLabelValue(attr.key) ||
    pickLabelValue(attr.property_label) ||
    pickLabelValue(attr.property) ||
    pickLabelValue(attr.alias) ||
    pickLabelValue(attr.id) ||
    pickLabelValue(attr.title) ||
    ""
  );
}

const IMAGE_LABEL_KEYWORDS_EN = [
  "image",
  "images",
  "picture",
  "pictures",
  "photo",
  "photos",
  "poster",
  "posters",
  "cover",
  "covers",
  "logo",
  "logos",
  "icon",
  "icons",
  "thumbnail",
  "thumbnails",
  "screenshot",
  "screenshots",
  "banner",
  "banners",
  "avatar",
  "avatars",
];
const IMAGE_LABEL_KEYWORDS_ZH = [
  "图像",
  "图片",
  "照片",
  "封面",
  "海报",
  "横幅",
  "头像",
];
const MAX_INFOBOX_IMAGES = 1;
function isImagePropertyName(name) {
  if (!name) return false;
  const raw = String(name).trim();
  if (!raw) return false;
  const lower = raw.toLowerCase();
  for (const kw of IMAGE_LABEL_KEYWORDS_EN) {
    if (lower.includes(kw)) return true;
  }
  for (const kw of IMAGE_LABEL_KEYWORDS_ZH) {
    if (raw.includes(kw)) return true;
  }
  return false;
}

function clearDetailPanel() {
  try {
    const tb = document.getElementById("detailAttrList");
    if (tb) tb.innerHTML = "";
    const imgBoxReset = document.getElementById("infoboxImgs");
    if (imgBoxReset) {
      imgBoxReset.innerHTML = "";
      imgBoxReset.style.display = "none";
    }
    const wikiView = document.getElementById("wikiView");
    if (wikiView) wikiView.innerHTML = "";
    const tagList = document.getElementById("detail_tagList");
    if (tagList) tagList.innerHTML = "";
    // Do not modify btnViewDetail active state here; view mode
    // (setViewMode) is responsible for updating the toggle button.
    try {
      const be = document.getElementById("btnEditWiki");
      if (be) {
        be.style.display = "none";
        be.onclick = null;
      }
    } catch {}
  } catch {}
}

function normalizeEntityIdForApi(id) {
  const raw = (id ?? "").toString().trim();
  if (!raw) return "";
  if (raw.includes("/")) return raw;
  return "entity/" + raw;
}

async function showNodeDetailInline(nodeId) {
  if (!nodeId) return;
  const routeId = (nodeId ?? "").toString().trim();
  const fullId = normalizeEntityIdForApi(routeId);
  const dp = document.getElementById("detailPanel");
  const inner = document.getElementById("detailInner");
  if (!dp || !inner) return;
  try {
    tablePanel.style.display = "none";
    cywrap.style.display = "none";
  } catch {}
  dp.style.display = "";
  clearDetailPanel();
  // keep the current entity id on the panel for wiki actions
  try {
    dp.dataset.entityId = fullId;
    window.kbActiveDetailNodeId = fullId;
    window.kbActiveDetailRouteId = routeId || fullId;
  } catch {}
  // show edit button immediately (fallback) so user can find it even if wiki content fetch fails
  try {
    const be = document.getElementById("btnEditWiki");
    if (be) {
      be.style.display = "inline-flex";
      // click behavior wired separately to open inline editor
    }
  } catch (e) {}
  inner.querySelector("#wikiView").textContent = "加载详情中…";
  try {
    const url = new URL("/api/kb/node", window.location.origin);
    if (typeof window.appendCurrentDbParam === "function") {
      const scopedUrl = window.appendCurrentDbParam(url);
      if (scopedUrl instanceof URL) {
        url.search = scopedUrl.search;
      }
    }
    url.searchParams.set("id", fullId);
    const resp = await fetch(url.toString());
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const data = await resp.json();
    const doc = data && data.node;
    const neighbors = data && data.neighbors;
    if (!doc) {
      inner.querySelector("#wikiView").innerHTML =
        '<div class="muted">未找到详情</div>';
      return;
    }
    // prefer authoritative id from doc if available
    try {
      const canonicalIdRaw =
        (doc && (doc._id || doc.id || doc._key)) || fullId;
      const canonicalId = (canonicalIdRaw || "").toString().trim() || fullId;
      dp.dataset.entityId = canonicalId;
      window.kbActiveDetailNodeId = canonicalId;
      if (!window.kbActiveDetailRouteId) {
        window.kbActiveDetailRouteId = routeId || canonicalId;
      }
    } catch {}
    // tags
    const tagPanel = document.getElementById("tagPanel");
    if (tagPanel) tagPanel.style.display = "none";
    const tagList = document.getElementById("detail_tagList");
    if (tagList) tagList.innerHTML = "";
    const title =
      (doc && (doc.label_zh || doc.label || doc._key)) || "未知实体";
    setText(document.getElementById("wikiTopTitle"), title);
    
    // Render classes as tags
    const wikiClasses = document.getElementById("wikiClasses");
    if (wikiClasses) {
      wikiClasses.innerHTML = "";
      const classes = Array.isArray(doc.classes) ? doc.classes : [];
      // Fallback to single classLabel if classes array is empty but classLabel exists
      if (classes.length === 0 && doc.classLabel) {
          classes.push({ id: doc.classId, name: doc.classLabel, color: doc.color });
      }
      
      classes.forEach(cls => {
          const tag = document.createElement("span");
          tag.className = "tag";
          tag.style.display = "inline-flex";
          tag.style.alignItems = "center";
          tag.style.gap = "4px";
          tag.style.padding = "2px 6px";
          tag.style.borderRadius = "4px";
          tag.style.fontSize = "12px";
          tag.style.backgroundColor = cls.color ? cls.color + "20" : "#f1f5f9"; // Light background
          tag.style.color = cls.color || "#475569";
          tag.style.border = `1px solid ${cls.color ? cls.color + "40" : "#e2e8f0"}`;
          
          const label = document.createElement("span");
          label.textContent = cls.name || "未命名分类";
          tag.appendChild(label);
          
          const delBtn = document.createElement("i");
          delBtn.className = "fa-solid fa-xmark";
          delBtn.style.cursor = "pointer";
          delBtn.style.opacity = "0.6";
          delBtn.title = "移除分类";
          delBtn.onmouseover = () => delBtn.style.opacity = "1";
          delBtn.onmouseout = () => delBtn.style.opacity = "0.6";
          delBtn.onclick = async (e) => {
              e.stopPropagation();
              if(confirm(`确认移除分类“${cls.name}”吗？`)) {
                  await removeEntityClass(fullId, cls.id);
                  showNodeDetailInline(fullId); // Refresh
              }
          };
          tag.appendChild(delBtn);
          
          wikiClasses.appendChild(tag);
      });
    }

    setText(
      document.getElementById("wikiTopDesc"),
      (doc && (doc.desc_zh || doc.description)) || ""
    );
    document.getElementById("wikiTop").style.display = "";
    // collect images
    const imageEntries = [];
    const seenImageUrls = new Set();
    if (doc && typeof doc.avatar === "string")
      addImageCandidate(
        imageEntries,
        seenImageUrls,
        doc.avatar,
        "头像",
        true
      );
    if (doc && typeof doc.image === "string")
      addImageCandidate(
        imageEntries,
        seenImageUrls,
        doc.image,
        pickLabelValue(doc.image_caption) || "图像",
        true
      );
    if (doc && Array.isArray(doc.images)) {
      for (const img of doc.images) {
        const caption =
          (img && pickLabelValue(img.caption)) ||
          pickLabelValue(doc.image_caption) ||
          pickLabelValue(doc.label_zh) ||
          pickLabelValue(doc.label) ||
          "";
        const urls = extractImageUrls(img, true);
        if (urls && urls.length)
          urls.forEach((u) =>
            addImageCandidate(
              imageEntries,
              seenImageUrls,
              u,
              caption,
              true
            )
          );
        else if (typeof img === "string")
          addImageCandidate(
            imageEntries,
            seenImageUrls,
            img,
            caption,
            true
          );
      }
    }
    // attributes -> props: prefer backend attribute list API used by attribute manager
    let propCount = 0;
    const added = new Set();
    try {
      // Prefer authoritative attribute list from attribute-manager API
      let attrItems = [];
      try {
        const aUrl = new URL("/api/kb/node/attributes", window.location.origin);
        if (typeof window.appendCurrentDbParam === "function") {
          const scopedUrl = window.appendCurrentDbParam(aUrl);
          if (scopedUrl instanceof URL) {
            aUrl.search = scopedUrl.search;
          }
        }
        aUrl.searchParams.set("id", fullId);
        const aResp = await fetch(aUrl.toString());
        if (aResp && aResp.ok) {
          const aData = await aResp.json();
          attrItems = Array.isArray(aData.items) ? aData.items : [];
        } else {
          // API returned non-ok -> leave attrItems empty
          attrItems = [];
        }
      } catch (e) {
        attrItems = [];
      }
      // Use the raw attribute items for rendering so the detail panel
      // matches the editable attribute list exactly. Still scan for
      // images (do not remove image candidates), but do not dedupe or
      // skip props here — let the renderer show the same rows as the
      // attribute manager.
      try {
        // collect images from attrItems (best-effort) but don't alter list
        try {
          for (const it of attrItems) {
            if (!it) continue;
            const name = (
              it.property_label_zh ||
              it.property_label ||
              it.property ||
              getAttributeLabel(it) ||
              pickLabelValue(it.key) ||
              pickLabelValue(it.name) ||
              ""
            ).trim();
            const val =
              typeof it.value !== "undefined"
                ? it.value
                : it.val ||
                  it.text ||
                  it.description ||
                  it.value_raw ||
                  it.data ||
                  "";
            const imageCaption =
              pickLabelValue(it.caption) ||
              pickLabelValue(it.description) ||
              name;
            try {
              const imgs = extractImageUrls(val, true);
              if (imgs && imgs.length)
                imgs.forEach((u) =>
                  addImageCandidate(
                    imageEntries,
                    seenImageUrls,
                    u,
                    imageCaption,
                    true
                  )
                );
            } catch {}
          }
        } catch (e) {}
        const detailAttrListEl =
          document.getElementById("detailAttrList");
        if (detailAttrListEl) {
          // renderAttrList checks container.id === 'detailAttrList' to enable readOnly mode
          renderAttrList(detailAttrListEl, attrItems, fullId);
          if (attrItems && attrItems.length > 0) {
              propCount += attrItems.length;
          }
        }
      } catch (e) {
        console.error("renderAttrList (detail) failed", e);
      }
    } catch (e) {
      console.error("render attributes failed", e);
    }
    // neighbors self-edges and outgoing relations
    if (Array.isArray(neighbors)) {
      const centerId = doc && doc._id;
      for (const it of neighbors) {
        const edge = it && it.edge;
        if (!edge) continue;
        const isSelf =
          centerId &&
          edge.source === centerId &&
          edge.target === centerId;
        const displayName =
          (edge &&
            (edge.property_label_zh ||
              edge.property_label ||
              edge.label_zh ||
              edge.label)) ||
          "属性";
        if (isSelf) {
          let rawVal = edge && (edge.value || edge.val || edge.text);
          if (typeof rawVal === "undefined")
            rawVal = edge && edge.datavalue && edge.datavalue.value;
          if (typeof rawVal === "undefined")
            rawVal =
              edge &&
              edge.mainsnak &&
              edge.mainsnak.datavalue &&
              edge.mainsnak.datavalue.value;
          const imageCaption =
            pickLabelValue(edge && edge.caption) ||
            pickLabelValue(edge && edge.description) ||
            displayName;
          const imageLike = isImagePropertyName
            ? isImagePropertyName(displayName)
            : false;
          if (imageLike) {
            const imgs = extractImageUrls(rawVal, true);
            if (imgs && imgs.length)
              imgs.forEach((u) =>
                addImageCandidate(
                  imageEntries,
                  seenImageUrls,
                  u,
                  imageCaption,
                  true
                )
              );
            continue;
          }
          let skipProp = false;
          try {
            const imgs = extractImageUrls(rawVal);
            if (imgs && imgs.length) {
              imgs.forEach((u) =>
                addImageCandidate(
                  imageEntries,
                  seenImageUrls,
                  u,
                  imageCaption
                )
              );
              skipProp = true;
            }
          } catch {}
          const val = formatPropValue(rawVal);
          const dedupKey = displayName + "::" + String(val);
          if (!skipProp && displayName && !added.has(dedupKey)) {
            // For self-edge attribute-like neighbors: do not append
            // extra rows into the detailAttrList here. We only mark as
            // seen so it doesn't affect counting/visibility elsewhere.
            try {
              added.add(dedupKey);
              propCount++;
            } catch (e) {}
          }
          continue;
        }
        const neighbor = it && it.node;
        const isOutgoing = centerId && edge.source === centerId;
        if (!isOutgoing) continue;
        const relationName = displayName;
        const neighborId =
          neighbor && (neighbor._id || neighbor.id || neighbor._key);
        const neighborLabel =
          (neighbor &&
            (neighbor.label_zh ||
              neighbor.label ||
              neighbor.name ||
              neighbor.title)) ||
          neighborId ||
          (isOutgoing ? edge.target : edge.source) ||
          "";
        const relationVal = {
          __type: "relation",
          direction: isOutgoing ? "outgoing" : "incoming",
          label: neighborLabel,
          id: neighborId,
          href: neighborId
            ? `/kb/detail?id=${encodeURIComponent(neighborId)}`
            : "",
          note: edge && edge.rank ? edge.rank : "",
        };
        const dedupKey =
          relationName +
          "::" +
          (neighborId || neighborLabel || "") +
          "::out";
        if (relationName && !added.has(dedupKey)) {
          // Do not append neighbor/relation rows into the attribute infobox list.
          // Just mark as added so we don't duplicate keys and count it for infobox visibility.
          added.add(dedupKey);
          propCount++;
        }
      }
    }
    const propsPanel = document.getElementById("wikiInfobox");
    if (propsPanel)
      propsPanel.style.display = propCount > 0 ? "" : "none";
    // render images
    try {
      const imgBox = document.getElementById("infoboxImgs");
      if (imgBox) {
        imgBox.innerHTML = "";
        imgBox.style.display = "none";
      }
    } catch {}
    // render wiki content if available and show edit button inside detail panel
    try {
      const view = document.getElementById("wikiView");
      view.innerHTML =
        data.page?.html ||
        (doc && doc.html) ||
        '<div class="muted">暂无百科内容。</div>';
      try {
        const be = document.getElementById("btnEditWiki");
        if (be) {
          be.style.display = "inline-flex";
          // inline edit click is handled by wired listener
        }
      } catch (e) {}
      // Also proactively load the canonical wiki payload into the inline editor
      // This ensures TOC, heading slugs, markdown/state stash, revisions and backlinks
      try {
        const entityForWiki =
          dp && dp.dataset && dp.dataset.entityId
            ? dp.dataset.entityId
            : fullId;
        if (entityForWiki) await loadWikiInline(entityForWiki, "zh");
      } catch (e) {
        // best-effort: do not block detail rendering on wiki fetch failures
        console.error("loadWikiInline failed", e);
      }
    } catch {}
  } catch (e) {
    console.error("load node detail failed", e);
    inner.querySelector("#wikiView").innerHTML =
      '<div class="muted">加载失败</div>';
  }
}
// Inline wiki functions adapted from kb_detail.html
function toggleWikiModeInline(editing) {
  const view = document.getElementById("wikiView");
  const edit = document.getElementById("wikiEditInline");
  const btnEdit = document.getElementById("btnEditWiki");
  const btnCancel = document.getElementById("btnCancelWikiInline");
  const btnSave = document.getElementById("btnSaveWikiInline");
  const infobox = document.getElementById("wikiInfobox");
  if (editing) {
    if (view) view.style.display = "none";
    if (edit) edit.style.display = "block";
    // hide detail attribute list while editing wiki inline
    try {
      const tagPanel = document.getElementById("tagPanel");
      if (tagPanel) tagPanel.style.display = "none";
      const tagList = document.getElementById("detail_tagList");
      if (tagList) tagList.innerHTML = "";
    } catch (e) {}
    try {
      if (infobox) {
        if (typeof infobox.dataset.prevDisplay === "undefined") {
          infobox.dataset.prevDisplay = infobox.style.display || "";
        }
        infobox.style.display = "none";
      }
    } catch (e) {}
    if (btnEdit) btnEdit.style.display = "none";
    if (btnCancel) btnCancel.style.display = "inline-flex";
    if (btnSave) btnSave.style.display = "inline-flex";

    // Initialize EasyMDE if not already initialized
    if (typeof EasyMDE !== 'undefined' && !window.easyMDE) {
      window.easyMDE = new EasyMDE({
        element: document.getElementById("wikiMdInline"),
        spellChecker: false,
        autosave: {
          enabled: false,
        },
        toolbar: [
          "bold", "italic", "heading", "|",
          "quote", "unordered-list", "ordered-list", "|",
          "link", "image", "table", "|",
          "preview", "side-by-side", "fullscreen", "|",
          "guide"
        ],
        status: false,
        minHeight: "400px",
        maxHeight: "900px",
      });
    }
    // Refresh EasyMDE to ensure it renders correctly
    setTimeout(() => {
       if (window.easyMDE) {
           window.easyMDE.codemirror.refresh();
           // Sync value from textarea if needed, though EasyMDE usually does this on init
           // But if we updated textarea while hidden, we might need to push it
           const ta = document.getElementById("wikiMdInline");
           if (ta && ta.value !== window.easyMDE.value()) {
               window.easyMDE.value(ta.value);
           }
       }
    }, 100);

  } else {
    if (view) view.style.display = "";
    if (edit) edit.style.display = "none";
    try {
      if (infobox) {
        const prev = infobox.dataset.prevDisplay;
        infobox.style.display = typeof prev !== "undefined" ? prev : "";
        delete infobox.dataset.prevDisplay;
      }
    } catch (e) {}
    if (btnEdit) btnEdit.style.display = "inline-flex";
    if (btnCancel) btnCancel.style.display = "none";
    if (btnSave) btnSave.style.display = "none";
  }
}

function looksLikeMarkdownTable(md) {
  try {
    const text = String(md || "").replace(/｜/g, "|").replace(/[－—–]/g, "-");
    return /\|.+\|/.test(text) && /\|\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?/.test(text);
  } catch {
    return false;
  }
}

function normalizeMarkdownTables(md) {
  try {
    const source = String(md || "");
    if (!source) return "";
    const lines = source.split(/\r?\n/);
    const out = [];
    const normalizeTableLine = (line) =>
      String(line || "").replace(/｜/g, "|").replace(/[－—–]/g, "-");
    const isTableLine = (line) => normalizeTableLine(line).trim().includes("|");
    const isDelimiterLine = (line) => {
      const normalized = normalizeTableLine(line).trim();
      if (!normalized.includes("|")) return false;
      const cells = normalized
        .split("|")
        .map((part) => part.trim())
        .filter(Boolean);
      if (!cells.length) return false;
      return cells.every((cell) => /^:?-{3,}:?$/.test(cell));
    };

    for (let i = 0; i < lines.length; i += 1) {
      const current = lines[i];
      const next = i + 1 < lines.length ? lines[i + 1] : "";
      const isTableStart = isTableLine(current) && isDelimiterLine(next);
      if (isTableStart) {
        if (out.length && out[out.length - 1].trim() !== "") {
          out.push("");
        }
        out.push(normalizeTableLine(current));
        out.push(normalizeTableLine(next));
        i += 2;
        while (i < lines.length && isTableLine(lines[i])) {
          out.push(normalizeTableLine(lines[i]));
          i += 1;
        }
        if (out.length && out[out.length - 1].trim() !== "") {
          out.push("");
        }
        i -= 1;
        continue;
      }
      out.push(current);
    }

    return out.join("\n");
  } catch {
    return String(md || "");
  }
}

function renderWikiMarkdownToHtml(md) {
  const source = normalizeMarkdownTables(md);
  if (typeof marked === "undefined") {
    return (
      '<pre style="white-space:pre-wrap">' + escapeHtml(source) + "</pre>"
    );
  }
  try {
    return marked.parse(source);
  } catch (e) {
    return (
      '<pre style="white-space:pre-wrap">' + escapeHtml(source) + "</pre>"
    );
  }
}

function setWikiViewHtml(view, html) {
  if (!view) return;
  if (typeof DOMPurify !== "undefined") {
    try {
      view.innerHTML = DOMPurify.sanitize(html);
      return;
    } catch {}
  }
  view.innerHTML = html;
}

async function loadWikiInline(entityId, lang = "zh") {
  try {
    // normalize to entity/<id>
    if (entityId && !entityId.startsWith("entity/")) {
      entityId = entityId.startsWith("/")
        ? entityId.slice(1)
        : "entity/" + entityId;
    }
    const url = new URL("/api/wiki/page", window.location.origin);
    url.searchParams.set("entityId", entityId);
    url.searchParams.set("lang", lang);
    // allow backend to auto-create stub if missing
    url.searchParams.set("create_if_missing", "1");
    const resp = await fetch(url);
    if (!resp.ok) throw new Error("wiki not found");
    const data = await resp.json();
    const page = data.page || {};
    const view = document.getElementById("wikiView");
    try {
      const md = page.md || "";
      const html = typeof page.html === "string" ? page.html : "";
      const shouldPreferClientMarkdown =
        looksLikeMarkdownTable(md) && !/<table[\s>]/i.test(html);
      if (html && !shouldPreferClientMarkdown) {
        setWikiViewHtml(view, html);
      } else if (typeof marked !== "undefined") {
        setWikiViewHtml(view, renderWikiMarkdownToHtml(md));
      } else {
        // fallback: escape and show plain markdown in a <pre>
        setWikiViewHtml(
          view,
          '<pre style="white-space:pre-wrap">' +
            (md ? escapeHtml(md) : "") +
            "</pre>"
        );
      }
    } catch (e) {
      try {
        setWikiViewHtml(
          view,
          '<pre style="white-space:pre-wrap">' +
            (page.md ? escapeHtml(page.md) : "") +
            "</pre>"
        );
      } catch {
        view.textContent = page.md || "";
      }
    }
    
    // Update popup TOC
    if (typeof updateToc === 'function') updateToc();

    // build TOC if wikiToc exists
    try {
      const tocNav = document.getElementById("wikiToc");
      if (tocNav) {
        const toc = Array.isArray(page.toc) ? page.toc : [];
        tocNav.innerHTML = "";
        if (toc.length) {
          tocNav.style.display = "block";
          const title = document.createElement("div");
          title.className = "toc-title";
          title.textContent = "目录";
          tocNav.appendChild(title);
          const counters = [];
          const levels = toc.map((it) => Number(it.level) || 1);
          const baseLevel = levels.length ? Math.min(...levels) : 1;
          for (const item of toc) {
            const level = Math.min(
              6,
              Math.max(1, Number(item.level) || 1)
            );
            const depth = Math.max(0, level - baseLevel);
            if (counters.length <= depth) {
              while (counters.length <= depth) counters.push(0);
            } else {
              counters.length = depth + 1;
            }
            counters[depth] = (counters[depth] || 0) + 1;
            const numberLabel = counters.slice(0, depth + 1).join(".");
            const a = document.createElement("a");
            a.href = "#" + item.slug;
            a.textContent = `${numberLabel} ${item.text}`;
            a.className = "toc-link toc-lv-" + level;
            a.dataset.tocNumber = numberLabel;
            a.addEventListener("click", (e) => {
              e.preventDefault();
              e.stopPropagation();
              const target = document.getElementById(item.slug);
              // prefer scrolling the detail panel container so the whole page doesn't move
              try {
                const container =
                  document.getElementById("detailInner") ||
                  document.getElementById("wikiContent");
                if (container && target) {
                  const containerRect = container.getBoundingClientRect();
                  const targetRect = target.getBoundingClientRect();
                  const offset =
                    targetRect.top -
                    containerRect.top +
                    container.scrollTop;
                  container.scrollTo({ top: offset, behavior: "smooth" });
                } else if (target) {
                  // fallback to document scrolling
                  target.scrollIntoView({
                    behavior: "smooth",
                    block: "start",
                  });
                }
              } catch (err) {
                try {
                  if (target)
                    target.scrollIntoView({
                      behavior: "smooth",
                      block: "start",
                    });
                } catch (e) {}
              }
              try {
                // keep TOC visible after click (some UI flows may hide it)
                tocNav.style.display = "block";
              } catch (err) {}
            });
            tocNav.appendChild(a);
          }
        } else {
          tocNav.style.display = "none";
        }
      }
    } catch (e) {}
    // If headings not id-ed, attempt simple injection for h1..h6 inside view
    try {
      const tmp = view.querySelectorAll("h1,h2,h3,h4,h5,h6");
      tmp.forEach((h) => {
        if (!h.id) {
          const txt = h.textContent || "";
          const slug = txt
            .trim()
            .toLowerCase()
            .replace(/\s+/g, "-")
            .replace(/[^\w\-\u4e00-\u9fff]/g, "");
          if (slug) h.id = slug;
        }
      });
    } catch {}
    // stash for edit
    try {
      const mdContent = page.md || "";
      document.getElementById("wikiMdInline").value = mdContent;
      if (window.easyMDE) {
          window.easyMDE.value(mdContent);
      }
    } catch {}
    try {
      document.getElementById("wikiStateInline").value =
        page.state || "published";
    } catch {}
    // load revisions and backlinks (use normalized id for keys)
    await loadRevisionsInline(entityId, lang);
    await loadBacklinksInline(entityId);
  } catch (e) {
    const view = document.getElementById("wikiView");
    if (view)
      view.innerHTML =
        '<div class="muted">暂无百科内容，点击“编辑百科”创建。</div>';
    const be = document.getElementById("btnEditWiki");
    if (be) be.style.display = "inline-flex";
  }
}

async function saveWikiInline(entityId, lang = "zh") {
  // prefer entityId stored on detailPanel
  try {
    const dp = document.getElementById("detailPanel");
    if (dp && dp.dataset && dp.dataset.entityId) {
      entityId = dp.dataset.entityId || entityId;
    }
  } catch {}
  // normalize to entity/<id> form if needed
  if (entityId && !entityId.startsWith("entity/")) {
    entityId = entityId.startsWith("/")
      ? entityId.slice(1)
      : "entity/" + entityId;
  }
  let md = "";
  if (window.easyMDE) {
      md = window.easyMDE.value();
  } else {
      md = document.getElementById("wikiMdInline").value;
  }
  const summary = document
    .getElementById("wikiSummaryInline")
    .value.trim();
  const state = document.getElementById("wikiStateInline").value;
  const payload = {
    entityId,
    lang,
    md,
    summary,
    state,
    updatedBy: "u:anonymous",
  };
  const resp = await fetch("/api/wiki/page/save", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    alert("保存失败: " + resp.status);
    return;
  }
  // reload and display the saved wiki for the same entity
  await loadWikiInline(entityId, lang);
  toggleWikiModeInline(false);
  await loadRevisionsInline(entityId, lang);
}

async function loadRevisionsInline(entityId, lang = "zh") {
  try {
    // compute key robustly: entity/<id> -> <id>
    const parts = String(entityId || "").split("/");
    const idPart = parts.length > 1 ? parts[1] : parts[0] || "";
    const key = idPart + ":" + lang;
    const url = new URL(
      "/api/wiki/page/revisions",
      window.location.origin
    );
    url.searchParams.set("key", key);
    const resp = await fetch(url);
    if (!resp.ok) return;
    const data = await resp.json();
    const body = document.getElementById("wikiRevBodyInline");
    body.innerHTML = "";
    for (const r of data.items || []) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td style="padding:4px 6px;">${
        r.revNo
      }</td><td style="padding:4px 6px;">${new Date(
        (r.createdAt || 0) * 1000
      ).toLocaleString()}</td><td style="padding:4px 6px;">${
        r.user || ""
      }</td><td style="padding:4px 6px;">${
        r.summary || ""
      }</td><td style="padding:4px 6px;"><button class="btn" data-rev="${
        r.revNo
      }" data-key="${key}" type="button">预览</button></td>`;
      body.appendChild(tr);
    }
    // attach preview handlers
    body.querySelectorAll("button[data-rev]").forEach((btn) =>
      btn.addEventListener("click", (ev) => {
        const rev = btn.getAttribute("data-rev");
        const key = btn.getAttribute("data-key");
        previewRevisionInline(key, rev);
      })
    );
  } catch (e) {
    console.error("loadRevisionsInline error", e);
  }
}

async function previewRevisionInline(pageKey, revNo) {
  try {
    const url = new URL(
      "/api/wiki/page/revision",
      window.location.origin
    );
    url.searchParams.set("key", pageKey);
    url.searchParams.set("revNo", revNo);
    const resp = await fetch(url);
    if (!resp.ok) {
      alert("加载版本失败");
      return;
    }
    const data = await resp.json();
    const rev = data.revision;
    const box = document.getElementById("wikiRevPreviewInline");
    const meta = document.getElementById("wikiRevMetaInline");
    const view = document.getElementById("wikiRevViewInline");
    meta.textContent = `预览版本 rev ${revNo} (${new Date(
      (rev.createdAt || 0) * 1000
    ).toLocaleString()})`;
    view.innerHTML =
      rev.html ||
      '<pre style="white-space:pre-wrap">' +
        (rev.md
          ? rev.md.replace(
              /[&<>]/g,
              (s) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[s])
            )
          : "") +
        "</pre>";
    box.style.display = "block";
    box.dataset.pageKey = pageKey;
    box.dataset.revNo = revNo;
    document.getElementById("btnRestoreRevInline").disabled = false;
  } catch (e) {
    console.error(e);
  }
}

async function restoreRevisionInline() {
  const box = document.getElementById("wikiRevPreviewInline");
  const pageKey = box.dataset.pageKey;
  const revNo = box.dataset.revNo;
  if (!pageKey || !revNo) return;
  if (!confirm("确认将当前页面内容回滚到 rev " + revNo + " 吗?")) return;
  try {
    const payload = {
      key: pageKey,
      revNo: Number(revNo),
      summary: "rollback via UI",
      updatedBy: "u:anonymous",
    };
    const resp = await fetch("/api/wiki/page/revision/restore", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      alert("回滚失败");
      return;
    }
    const data = await resp.json();
    const entityId = "entity/" + pageKey.split(":")[0];
    await loadWikiInline(entityId, pageKey.split(":")[1]);
    await loadRevisionsInline(entityId, pageKey.split(":")[1]);
    closeRevisionPreviewInline();
    alert("已回滚，新版本号: " + data.revNo);
  } catch (e) {
    alert("回滚异常");
  }
}

function closeRevisionPreviewInline() {
  const box = document.getElementById("wikiRevPreviewInline");
  box.style.display = "none";
  box.dataset.pageKey = "";
  box.dataset.revNo = "";
}

async function loadBacklinksInline(entityId) {
  try {
    const url = new URL("/api/wiki/backlinks", window.location.origin);
    url.searchParams.set("target", entityId);
    const resp = await fetch(url);
    if (!resp.ok) return;
    const data = await resp.json();
    const list = document.getElementById("wikiBacklinksListInline");
    const panel = document.getElementById("wikiBacklinksInline");
    list.innerHTML = "";
    for (const it of data.items || []) {
      const li = document.createElement("li");
      const a = document.createElement("a");
      a.href = it.href || "#";
      a.textContent = it.title || it.id || "";
      a.target = "_blank";
      a.rel = "noreferrer";
      li.appendChild(a);
      list.appendChild(li);
    }
    panel.style.display = list.childElementCount ? "block" : "none";
  } catch {}
}

// Wire up inline buttons
(function wireWikiInlineButtons() {
  try {
    const be = document.getElementById("btnEditWiki");
    if (be)
      be.addEventListener("click", () => {
        // show inline editor for current selected id
        const id =
          window.kbSelectedRowId ||
          (document.getElementById("fId") &&
            document.getElementById("fId").value) ||
          "";
        if (!id) return alert("未选择实体");
        loadWikiInline(id, "zh");
        toggleWikiModeInline(true);
      });

    const btnSave = document.getElementById("btnSaveWikiInline");
    if (btnSave)
      btnSave.addEventListener("click", async () => {
        const id =
          window.kbSelectedRowId ||
          (document.getElementById("fId") &&
            document.getElementById("fId").value) ||
          "";
        if (!id) return alert("未选择实体");
        await saveWikiInline(id, "zh");
      });
    const btnCancel = document.getElementById("btnCancelWikiInline");
    if (btnCancel)
      btnCancel.addEventListener("click", () =>
        toggleWikiModeInline(false)
      );
    const btnRestore = document.getElementById("btnRestoreRevInline");
    if (btnRestore)
      btnRestore.addEventListener("click", restoreRevisionInline);
    const btnCloseRev = document.getElementById("btnCloseRevInline");
    if (btnCloseRev)
      btnCloseRev.addEventListener("click", closeRevisionPreviewInline);
  } catch (e) {}
})();

function resolveInitialDetailRoute() {
  try {
    if (typeof window.getRouteStateFromHash === "function") {
      const route = window.getRouteStateFromHash() || {};
      const view = String(route.view || "").trim().toLowerCase();
      const node = String(route.node || "").trim();
      if (view === "detail" && node) {
        return { view, node };
      }
    }
  } catch {}

  try {
    if (typeof window.getUrlParams === "function") {
      const params = window.getUrlParams() || {};
      const view = String(params.view || "").trim().toLowerCase();
      const node = String(params.node || "").trim();
      if (view === "detail" && node) {
        return { view, node };
      }
    }
  } catch {}

  try {
    const hash = String(window.location.hash || "").replace(/^#/, "").trim();
    if (hash) {
      const parsed = new URLSearchParams(hash);
      const explicitView = String(parsed.get("view") || "").trim().toLowerCase();
      const fallbackView = explicitView || hash.split("&")[0].split("=")[0].trim().toLowerCase();
      const node = String(parsed.get("node") || "").trim();
      if (fallbackView === "detail" && node) {
        return { view: "detail", node };
      }
      if (hash.toLowerCase() === "detail") {
        const queryNode = new URLSearchParams(window.location.search || "").get("node") || "";
        if (String(queryNode).trim()) {
          return { view: "detail", node: String(queryNode).trim() };
        }
      }
    }
  } catch {}

  return { view: "", node: "" };
}

function hydrateDetailFromCurrentRoute() {
  try {
    const route = resolveInitialDetailRoute();
    if (route.view !== "detail" || !route.node) return;
    const detailPanel = document.getElementById("detailPanel");
    if (!detailPanel) return;
    const canonicalNodeId =
      typeof normalizeEntityIdForApi === "function"
        ? normalizeEntityIdForApi(route.node)
        : route.node;
    if (!canonicalNodeId) return;
    window.kbSelectedRowId = route.node;
    try {
      window.kbSelectedRowIds = new Set([route.node]);
      window.kbLastAnchorRowId = route.node;
    } catch {}
    window.kbSelectedNodeId = route.node;
    window.kbActiveDetailRouteId = route.node;
    window.kbActiveDetailNodeId = canonicalNodeId;
    detailPanel.style.display = "";
    showNodeDetailInline(route.node);
  } catch (err) {
    if (window.console && console.warn) {
      console.warn("hydrateDetailFromCurrentRoute failed", err);
    }
  }
}

window.hideDetailPanel = hideDetailPanel;
window.setText = setText;
window.formatPropValue = formatPropValue;
window.isImageUrl = isImageUrl;
window.extractImageUrls = extractImageUrls;
window.normalizeImageUrl = normalizeImageUrl;
window.addImageCandidate = addImageCandidate;
window.pickLabelValue = pickLabelValue;
window.getAttributeLabel = getAttributeLabel;
window.isImagePropertyName = isImagePropertyName;
window.clearDetailPanel = clearDetailPanel;
window.normalizeEntityIdForApi = normalizeEntityIdForApi;
window.showNodeDetailInline = showNodeDetailInline;
window.toggleWikiModeInline = toggleWikiModeInline;
window.loadWikiInline = loadWikiInline;
window.saveWikiInline = saveWikiInline;
window.loadRevisionsInline = loadRevisionsInline;
window.previewRevisionInline = previewRevisionInline;
window.restoreRevisionInline = restoreRevisionInline;
window.closeRevisionPreviewInline = closeRevisionPreviewInline;
window.loadBacklinksInline = loadBacklinksInline;
hydrateDetailFromCurrentRoute();
})();
