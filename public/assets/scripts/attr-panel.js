(function () {
  // Attribute Manager JS
  // ----------------------
  const shared = window.kbApp || {};
  const state = shared.state || {};
  const dom = shared.dom || {};
  const byId = dom.byId || ((id) => document.getElementById(id));
  const attrPanel = byId("attrPanel");
  const attrForm = byId("attrForm");
  const attrId = byId("attrId");
  const attrProp = byId("attrProp");
  const attrPropLabel = byId("attrPropLabel");
  const attrType = byId("attrType");
  const attrValue = byId("attrValue");
  // New datatype-specific inputs
  const attrValueUrl = byId("attrValueUrl");
  const attrValueDate = byId("attrValueDate");
  const attrValueAmount = byId("attrValueAmount");
  const attrValueUnit = byId("attrValueUnit");
  const attrValueLat = byId("attrValueLat");
  const attrValueLon = byId("attrValueLon");
  // wikibase-entityid inputs
  const attrEntitySearchInput = byId("attrEntitySearchInput");
  const attrEntitySearchResults = byId("attrEntitySearchResults");
  const attrEntitySearchResultsWrap = byId("attrEntitySearchResultsWrap");
  const attrEntitySearchStatus = byId("attrEntitySearchStatus");
  const attrEntitySelectedPreview = byId("attrEntitySelectedPreview");
  const attrValueEntityType = byId("attrValueEntityType");
  const attrValueEntityId = byId("attrValueEntityId");
  const attrValueEntityNumericId = byId("attrValueEntityNumericId");
  const attrValueQualifier = byId("attrValueQualifier");
  const attrDatatypeHint = byId("attrDatatypeHint");
  const attrDatatypeGroups = byId("attrDatatypeGroups");
  const attrMsg = byId("attrMsg");
  const attrList = byId("attrList");
  const btnAttrSave = byId("btnAttrSave");
  const btnAttrReset = byId("btnAttrReset");
  const btnAttrEditSelected = byId("btnAttrEditSelected");
  const btnAttrDeleteSelected = byId("btnAttrDeleteSelected");
  // commonsMedia (image) inputs
  const attrValueImage = byId("attrValueImage");
  const attrImagePreview = byId("attrImagePreview");
  const attrImagePreviewImg = byId("attrImagePreviewImg");
  const attrImageFileName = byId("attrImageFileName");
  const btnAttrImageClear = byId("btnAttrImageClear");
  const attrValueImageUrl = byId("attrValueImageUrl");
  const btnAttrImageUpload = byId("btnAttrImageUpload");
  const attrImageStateText = byId("attrImageStateText");
  if (typeof state.bindAlias === "function") {
    state.bindAlias("kbSelectedAttrIds", "selectedAttrIds", () => new Set());
    state.bindAlias("kbLastAttrAnchorId", "lastAttrAnchorId", "");
    state.bindAlias("kbAttrCache", "attrCache", () => new Map());
    state.bindAlias(
      "kbPropertySuggestionCache",
      "propertySuggestionCache",
      () => new Map(),
    );
  }
  let attrEntitySearchItems = [];

  function mapDatatypeToUi(datatype, datavalueType) {
    // 数值类型（valuetype / datavalue_type）优先，其次才看 datatype
    const dv = (datavalueType || "").toString().toLowerCase();
    const dt = (datatype || "").toString().toLowerCase();

    // --- 数值类型优先判断 ---
    if (dv === "wikibase-entityid" || dv.startsWith("wikibase-"))
      return "wikibase-entityid";
    if (dv === "globecoordinate" || dv === "globe-coordinate")
      return "globecoordinate";
    if (dv === "commonsmedia") return "commonsMedia";
    if (["string", "url", "time", "quantity", "monolingualtext"].includes(dv))
      return dv;

    // --- 回退到 datatype ---
    if (dt === "wikibase-entityid" || dt.startsWith("wikibase-"))
      return "wikibase-entityid";
    if (dt === "globe-coordinate" || dt === "globecoordinate")
      return "globecoordinate";
    if (dt === "commonsmedia") return "commonsMedia";
    if (["string", "url", "time", "quantity", "monolingualtext"].includes(dt))
      return dt;

    if (dv) return dv;
    if (dt) return dt;
    return "string";
  }

  function pickUiDatatype(item) {
    if (!item || typeof item !== "object") return "";
    if (item.ui_datatype) return item.ui_datatype;
    // 兼容 valuetype（属性表）和 datavalue_type（属性记录）两种字段名
    const valueType = item.datavalue_type || item.valuetype || "";
    return mapDatatypeToUi(item.datatype, valueType);
  }

  if (!window.kbPropertySuggestionCache)
    window.kbPropertySuggestionCache = new Map();

  const PROPERTY_SUGGESTION_TTL = 5 * 60 * 1000;

  function normalizeEntitySearchItem(raw) {
    if (!raw) return null;
    if (typeof raw === "string") {
      const id = raw.trim();
      if (!id) return null;
      const entityType = inferEntityTypeFromId(id);
      const numericMatch = id.match(/(\d+)/);
      const numericId = numericMatch ? numericMatch[1] : "";
      const numericValue = numericId ? Number(numericId) : undefined;
      const valueObj = {
        id,
        "entity-type": entityType,
        entity_type: entityType,
      };
      if (numericValue !== undefined && !Number.isNaN(numericValue)) {
        valueObj["numeric-id"] = numericValue;
        valueObj.numeric_id = numericValue;
      }
      return {
        id,
        label: id,
        entity_type: entityType,
        numeric_id: numericId,
        count: 0,
        value: valueObj,
      };
    }
    if (typeof raw !== "object") return null;
    const source = raw;
    const valueObjRaw =
      source.value && typeof source.value === "object" ? source.value : {};
    const valueObj = { ...valueObjRaw };
    const id =
      source.id ||
      source.entity_id ||
      valueObj.id ||
      (source.value && source.value.id)
        ? String(
            source.id ||
              source.entity_id ||
              valueObj.id ||
              (source.value && source.value.id),
          ).trim()
        : "";
    if (!id) return null;
    if (!valueObj.id) valueObj.id = id;

    let entityType =
      source.entity_type ||
      source["entity-type"] ||
      valueObj["entity-type"] ||
      valueObj.entity_type;
    if (!entityType) entityType = inferEntityTypeFromId(id);
    valueObj["entity-type"] = entityType;
    valueObj.entity_type = entityType;

    let numericId =
      source.numeric_id ??
      source["numeric-id"] ??
      valueObj["numeric-id"] ??
      valueObj.numeric_id;
    if (numericId === undefined || numericId === null || numericId === "") {
      const match = id.match(/(\d+)/);
      numericId = match ? match[1] : "";
    }
    let numericIdStr = "";
    if (numericId !== undefined && numericId !== null) {
      numericIdStr = String(numericId).trim();
      if (numericIdStr) {
        const numericNumber = Number(numericIdStr);
        if (!Number.isNaN(numericNumber)) {
          valueObj["numeric-id"] = numericNumber;
          valueObj.numeric_id = numericNumber;
        } else {
          valueObj["numeric-id"] = numericIdStr;
          valueObj.numeric_id = numericIdStr;
        }
      } else {
        numericIdStr = "";
      }
    }

    const label =
      source.label ||
      source.entity_label ||
      source.entity_label_zh ||
      valueObj.entity_label_zh ||
      valueObj.label ||
      valueObj.label_zh ||
      id;
    if (label && !valueObj.entity_label_zh) valueObj.entity_label_zh = label;

    return {
      id,
      label,
      entity_type: entityType,
      numeric_id: numericIdStr,
      count:
        typeof source.count === "number" && Number.isFinite(source.count)
          ? source.count
          : 0,
      value: valueObj,
    };
  }

  function canonicalizePropertyId(prop) {
    if (prop === null || typeof prop === "undefined") return "";
    let raw = String(prop).trim();
    if (!raw) return "";
    if (raw.includes("/")) {
      raw = raw.split("/").pop() || raw;
    }
    raw = raw.replace(/^property\//i, "");
    if (!raw) return "";
    const upper = raw.toUpperCase();
    const prefixed = upper.match(/^P\s*0*(\d+)$/);
    if (prefixed && prefixed[1]) {
      const num = parseInt(prefixed[1], 10);
      return Number.isFinite(num) ? `${num}` : `${prefixed[1]}`;
    }
    if (/^\d+$/.test(upper)) {
      const num = parseInt(upper, 10);
      return Number.isFinite(num) ? `${num}` : `${upper}`;
    }
    const anyDigits = upper.match(/(\d+)/);
    if (anyDigits && anyDigits[1]) {
      const num = parseInt(anyDigits[1], 10);
      if (Number.isFinite(num)) return `P${num}`;
    }
    return upper;
  }

  function propertyIdToApiPath(prop) {
    // For the new backend, we use the ID directly without 'property/' prefix
    return prop;
  }

  function samePropertyId(a, b) {
    if (!a || !b) return false;
    const left = canonicalizePropertyId(a);
    const right = canonicalizePropertyId(b);
    if (!left || !right) return false;
    return left === right;
  }

  // Render attribute list into a container element using the same DOM/behavior
  function renderAttrList(container, items, nodeId) {
    if (!container) return;
    container.innerHTML = "";
    if (!Array.isArray(items) || !items.length) {
      container.innerHTML = '<div class="muted">暂无属性</div>';
      return;
    }
    const readOnly = container.id === "detailAttrList";

    let displayItems = items;
    // Special handling for images in detail view
    if (readOnly) {
      console.log("Rendering attribute list with image handling", items);
      const imageItems = items.filter((it) => {
        return it.property_label_zh === "图像";
      });

      if (imageItems.length > 0) {
        // Collect all valid image sources
        const imageUrls = [];
        imageItems.forEach((it) => {
          let rawVal = it.value;
          if (typeof rawVal === "string") {
            const trimmed = rawVal.trim();
            if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
              try {
                const parsed = JSON.parse(trimmed);
                if (Array.isArray(parsed)) rawVal = parsed;
              } catch {}
            }
          }
          const values = Array.isArray(rawVal) ? rawVal : [rawVal];
          values.forEach((v) => {
            if (!v) return;
            let src = String(v);
            // If it looks like a filename and not a URL, try Wikimedia Commons
            if (
              !src.startsWith("http") &&
              !src.startsWith("//") &&
              !src.startsWith("data:")
            ) {
              src = `https://commons.wikimedia.org/wiki/Special:FilePath/${encodeURIComponent(src)}`;
            }
            imageUrls.push(src);
          });
        });

        if (imageUrls.length > 0) {
          if (imageUrls.length === 1) {
            // Single image display
            const imgContainer = document.createElement("div");
            imgContainer.style.marginBottom = "12px";
            imgContainer.style.display = "flex";
            imgContainer.style.justifyContent = "center";

            const img = document.createElement("img");
            img.src = imageUrls[0];
            img.style.display = "block";
            img.style.width = "100%";
            img.style.height = "auto";
            img.style.boxSizing = "border-box";
            img.style.maxHeight = "300px";
            img.style.borderRadius = "4px";
            img.style.border = "1px solid var(--border)";
            img.style.objectFit = "contain";
            img.style.backgroundColor = "var(--surface-1)";
            img.style.cursor = "zoom-in";
            img.onclick = () => window.open(imageUrls[0], "_blank");

            imgContainer.appendChild(img);
            container.appendChild(imgContainer);
          } else {
            // Carousel display
            const carousel = document.createElement("div");
            carousel.className = "carousel-container";

            const slides = document.createElement("div");
            slides.className = "carousel-slides";

            imageUrls.forEach((src) => {
              const slide = document.createElement("div");
              slide.className = "carousel-slide";
              const img = document.createElement("img");
              img.src = src;
              // responsive: fill slide width and preserve aspect ratio
              img.style.display = "block";
              img.style.width = "100%";
              img.style.height = "auto";
              img.style.boxSizing = "border-box";
              img.style.maxHeight = "100%";
              img.style.borderRadius = "4px";
              img.style.objectFit = "contain";
              img.style.backgroundColor = "var(--surface-1)";
              img.style.cursor = "zoom-in";
              img.onclick = () => window.open(src, "_blank");
              slide.appendChild(img);
              slides.appendChild(slide);
            });

            carousel.appendChild(slides);

            // Controls
            const prevBtn = document.createElement("button");
            prevBtn.className = "carousel-prev";
            prevBtn.innerHTML = "&#10094;";

            const nextBtn = document.createElement("button");
            nextBtn.className = "carousel-next";
            nextBtn.innerHTML = "&#10095;";

            const dots = document.createElement("div");
            dots.className = "carousel-dots";

            imageUrls.forEach((_, i) => {
              const dot = document.createElement("span");
              dot.className = `carousel-dot ${i === 0 ? "active" : ""}`;
              dot.onclick = (e) => {
                e.stopPropagation();
                goToSlide(i);
              };
              dots.appendChild(dot);
            });

            carousel.appendChild(prevBtn);
            carousel.appendChild(nextBtn);
            carousel.appendChild(dots);

            container.appendChild(carousel);

            // Carousel Logic
            let currentIndex = 0;
            const slideCount = imageUrls.length;
            // Hint to browser for smoother transforms
            slides.style.willChange = "transform";

            // Update transform using pixels (more reliable than % when layout changes)
            function updateTransform() {
              const offset = currentIndex * carousel.clientWidth;
              slides.style.transform = `translate3d(-${offset}px,0,0)`;
            }

            function updateDots() {
              const allDots = dots.querySelectorAll(".carousel-dot");
              allDots.forEach((d, i) =>
                d.classList.toggle("active", i === currentIndex),
              );
            }

            function goToSlide(index) {
              if (index < 0) index = slideCount - 1;
              if (index >= slideCount) index = 0;
              currentIndex = index;
              updateTransform();
              updateDots();
            }

            prevBtn.onclick = (e) => {
              e.stopPropagation();
              goToSlide(currentIndex - 1);
            };

            nextBtn.onclick = (e) => {
              e.stopPropagation();
              goToSlide(currentIndex + 1);
            };

            // Make sure transform is correct after images load or on resize
            const imgs = slides.querySelectorAll("img");
            let imgsLoaded = 0;
            if (imgs.length) {
              imgs.forEach((img) => {
                if (img.complete) {
                  imgsLoaded++;
                } else {
                  img.addEventListener("load", () => {
                    imgsLoaded++;
                    if (imgsLoaded === imgs.length) updateTransform();
                  });
                }
              });
            }

            // Ensure correct position on first render and on window resize
            requestAnimationFrame(() => {
              updateTransform();
              updateDots();
            });
            window.addEventListener("resize", () => updateTransform());

            // Basic pointer swipe support for touch/drag
            (function enableSwipe() {
              let isPointerDown = false;
              let startX = 0;
              let lastClientX = 0;

              carousel.addEventListener("pointerdown", (ev) => {
                // don't start drag when pressing on controls
                if (
                  ev.target.closest(
                    ".carousel-prev, .carousel-next, .carousel-dot",
                  )
                )
                  return;
                isPointerDown = true;
                startX = ev.clientX;
                lastClientX = ev.clientX;
                carousel.setPointerCapture &&
                  carousel.setPointerCapture(ev.pointerId);
                // disable transition while dragging
                slides.style.transition = "none";
              });

              carousel.addEventListener("pointermove", (ev) => {
                if (!isPointerDown) return;
                const dx = ev.clientX - startX;
                const offset = -currentIndex * carousel.clientWidth + dx;
                slides.style.transform = `translate3d(${offset}px,0,0)`;
                lastClientX = ev.clientX;
              });

              function endPointer(ev) {
                if (!isPointerDown) return;
                isPointerDown = false;
                try {
                  carousel.releasePointerCapture &&
                    carousel.releasePointerCapture(ev.pointerId);
                } catch (e) {}
                // restore transition
                slides.style.transition = "";
                const dx = lastClientX - startX;
                if (Math.abs(dx) > carousel.clientWidth * 0.15) {
                  if (dx > 0) goToSlide(currentIndex - 1);
                  else goToSlide(currentIndex + 1);
                } else {
                  updateTransform(); // snap back
                }
              }

              ["pointerup", "pointercancel", "pointerleave"].forEach((evt) =>
                carousel.addEventListener(evt, endPointer),
              );
            })();
          }
        }

        // Filter out images from the list
        displayItems = items.filter((it) => {
          return it.property_label_zh !== "图像";
        });
      }
    }

    const frag = document.createDocumentFragment();
    let lastReadOnlyProp = null;
    let lastEditableProp = null;
    for (const it of displayItems) {
      // If rendering detail (read-only) and value is an array (or a
      // JSON-encoded array string), render each element as its own row
      // so multi-valued properties appear one-per-line. For editable
      // lists (left panel) keep original single-row behavior.
      let rawVal = it.value;
      // try to parse JSON-encoded array values
      if (typeof rawVal === "string") {
        const trimmed = rawVal.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
          try {
            const parsed = JSON.parse(trimmed);
            if (Array.isArray(parsed)) rawVal = parsed;
          } catch {
            // ignore parse errors and treat as string
          }
        }
      }
      const values = Array.isArray(rawVal) ? rawVal : [rawVal];
      for (let vi = 0; vi < values.length; vi++) {
        const valItem = values[vi];
        const row = document.createElement("div");
        row.style.display = "flex";
        row.style.alignItems = "center";
        row.style.gap = "6px";
        row.style.padding = "4px 0";
        // Use the attribute id for editable rows; for detail rows append index
        // to ensure each rendered row has a unique data-id when in detail mode.
        const dataId = `${it.id}::${vi}`;
        row.setAttribute("data-id", dataId);
        // detail panel should be display-only (no selection/edit)
        row.style.cursor = readOnly ? "default" : "pointer";
        const label = document.createElement("div");
        label.style.flex = "0 0 140px";
        label.style.color = "var(--muted)";
        label.style.fontSize = "12px";
        let showLabel = "";
        try {
          if (readOnly) {
            const preferLabel =
              typeof it?.property_label_zh === "string"
                ? it.property_label_zh.trim()
                : "";
            const helperLabel =
              typeof getAttributeLabel === "function"
                ? getAttributeLabel(it) || ""
                : "";
            const curLabel = preferLabel || helperLabel;
            const canonicalProp = canonicalizePropertyId(it?.property);
            const rawProp = typeof it?.property === "string" ? it.property : "";
            if (
              !canonicalProp ||
              !lastReadOnlyProp ||
              canonicalProp !== lastReadOnlyProp
            ) {
              const base = curLabel || "";
              showLabel = base || rawProp;
              lastReadOnlyProp = canonicalProp || null;
            } else {
              showLabel = "";
            }
          } else {
            const canonicalProp = canonicalizePropertyId(it?.property);
            const preferLabel =
              typeof it?.property_label_zh === "string"
                ? it.property_label_zh.trim()
                : "";
            const labelToUse = preferLabel || canonicalProp || "";
            if (
              !canonicalProp ||
              !lastEditableProp ||
              canonicalProp !== lastEditableProp
            ) {
              showLabel = labelToUse;
              lastEditableProp = canonicalProp || null;
            } else {
              showLabel = "";
            }
          }
        } catch (e) {
          showLabel = readOnly
            ? `${it.property || ""}`
            : it?.property_label_zh || "";
        }
        label.textContent = showLabel;
        const val = document.createElement("div");
        val.style.flex = "1";
        val.style.fontSize = "12px";
        val.style.padding = "2px 6px";
        val.style.borderRadius = "4px";
        // For detail panel we want multi-line visible values; allow wrapping there.
        if (readOnly) {
          val.style.whiteSpace = "normal";
        } else {
          val.style.whiteSpace = "nowrap";
          val.style.overflow = "hidden";
          val.style.textOverflow = "ellipsis";
        }
        const valDtype = pickUiDatatype(it) || it.datatype;
        try {
          val.innerHTML = renderAttrValue(valDtype, valItem);
        } catch (e) {
          val.textContent = formatAttrValue(valDtype, valItem);
        }
        row.appendChild(label);
        row.appendChild(val);
        // Selection/edit behavior only for editable lists (not detail view)
        if (!readOnly) {
          row.addEventListener("click", (e) => {
            const id = dataId;
            const idsInOrder = Array.from(
              container.querySelectorAll("div[data-id]"),
            ).map((el) => el.getAttribute("data-id"));
            if (
              e.shiftKey &&
              window.kbLastAttrAnchorId &&
              idsInOrder.includes(window.kbLastAttrAnchorId)
            ) {
              const start = idsInOrder.indexOf(window.kbLastAttrAnchorId);
              const end = idsInOrder.indexOf(id);
              const [a, b] = start < end ? [start, end] : [end, start];
              window.kbSelectedAttrIds.clear();
              idsInOrder
                .slice(a, b + 1)
                .forEach((x) => window.kbSelectedAttrIds.add(x));
            } else if (e.ctrlKey || e.metaKey) {
              if (window.kbSelectedAttrIds.has(id))
                window.kbSelectedAttrIds.delete(id);
              else window.kbSelectedAttrIds.add(id);
              window.kbLastAttrAnchorId = id;
            } else {
              window.kbSelectedAttrIds.clear();
              window.kbSelectedAttrIds.add(id);
              window.kbLastAttrAnchorId = id;
              // auto-edit single selection
              try {
                fillAttrForm(nodeId, it, vi);
              } catch {}
            }
            updateAttrSelectionStyles();
            ensureAttrButtonsState();
          });
        }
        frag.appendChild(row);
      }
    }
    container.appendChild(frag);
    // only update selection/styles for interactive attrList (left panel)
    if (!readOnly) {
      updateAttrSelectionStyles();
      ensureAttrButtonsState();
    }
  }

  function normalizeEntityIdForCompare(id) {
    try {
      if (typeof normalizeEntityIdForApi === "function") {
        const normalized = normalizeEntityIdForApi(id || "");
        return (normalized || "").trim();
      }
    } catch {}
    return (id || "").toString().trim();
  }

  function isDetailPanelShowingEntity(nodeId) {
    try {
      if (!nodeId) return false;
      const detailPanelEl = document.getElementById("detailPanel");
      if (!detailPanelEl || detailPanelEl.style.display === "none")
        return false;
      const panelId =
        (detailPanelEl.dataset && detailPanelEl.dataset.entityId) ||
        window.kbActiveDetailNodeId ||
        window.kbActiveDetailRouteId ||
        "";
      if (!panelId) return false;
      return (
        normalizeEntityIdForCompare(panelId) ===
        normalizeEntityIdForCompare(nodeId)
      );
    } catch (err) {
      console.error("isDetailPanelShowingEntity failed", err);
      return false;
    }
  }

  function syncDetailAttrList(items, nodeId) {
    try {
      if (!isDetailPanelShowingEntity(nodeId)) return;
      const detailListEl = document.getElementById("detailAttrList");
      if (!detailListEl) return;
      renderAttrList(
        detailListEl,
        Array.isArray(items) ? items : [],
        normalizeEntityIdForCompare(nodeId),
      );
    } catch (err) {
      console.error("syncDetailAttrList failed", err);
    }
  }

  function resetAttrForm() {
    window.kbEditingValueIndex = -1;
    attrId.value = "";
    attrProp.value = "";
    attrPropLabel.value = "";
    attrType.value = "string";
    try {
      attrValue.value = "";
    } catch {}
    try {
      attrValueUrl.value = "";
      attrValueDate.value = "";
      attrValueAmount.value = "";
      attrValueUnit.value = "";
      attrValueLat.value = "";
      attrValueLon.value = "";
      attrValueMonoText.value = "";
      attrValueMonoLang.value = "";
    } catch {}
    // Clear image upload
    clearImageUpload();
    clearEntitySearchState();
    attrMsg.textContent = "";
    updateDatatypeUI("string");
  }

  function updateDatatypeUI(dtype, valueType) {
    const normalized = mapDatatypeToUi(dtype, valueType) || "string";
    // 动态获取 attrDatatypeGroups，确保能正确找到元素
    const groupsContainer = document.getElementById("attrDatatypeGroups");
    const groups = groupsContainer
      ? groupsContainer.querySelectorAll(".dtype-group")
      : [];
    groups.forEach((g) => {
      const gDtype = g.getAttribute("data-dtype");
      const shouldShow = gDtype === normalized;
      if (shouldShow) {
        g.style.display = "flex";
      } else {
        g.style.display = "none";
      }
    });
    const hintEl = document.getElementById("attrDatatypeHint");
    if (hintEl) {
      const parts = [`数据类型：${normalized || "string"}`];
      if (valueType) parts.push(`数值类型：${valueType}`);
      hintEl.textContent = parts.join("　");
    }
  }

  function inferEntityTypeFromId(entityId) {
    if (!entityId) return "item";
    const c = entityId.charAt(0).toUpperCase();
    if (c === "Q") return "item";
    if (c === "P") return "property";
    if (c === "L") return "lexeme";
    if (c === "E") return "entity";
    return "item";
  }

  function updateEntitySelectionPreview(label, id) {
    if (!attrEntitySelectedPreview) return;
    if (!id) {
      attrEntitySelectedPreview.textContent = "当前未选择实体";
      return;
    }
    const text = label ? `${label} (${id})` : id;
    attrEntitySelectedPreview.textContent = `已选择：${text}`;
  }

  function renderEntitySearchResults(items) {
    if (!attrEntitySearchResults || !attrEntitySearchResultsWrap) return;
    attrEntitySearchResults.innerHTML = "";
    const normalized = [];
    (Array.isArray(items) ? items : []).forEach((raw) => {
      const norm = normalizeEntitySearchItem(raw);
      if (norm && norm.id) normalized.push(norm);
    });
    attrEntitySearchItems = normalized;
    if (!attrEntitySearchItems.length) {
      attrEntitySearchResultsWrap.style.display = "none";
      return;
    }
    const frag = document.createDocumentFragment();
    attrEntitySearchItems.forEach((it, idx) => {
      const id = (it?.id || "").toString();
      const opt = document.createElement("option");
      opt.value = id;
      const label = it?.label || id;
      const countInfo =
        typeof it?.count === "number" && it.count > 0 ? ` · ${it.count}次` : "";
      opt.textContent = `${label} (${id})${countInfo}`;
      if (idx === 0) opt.selected = true;
      frag.appendChild(opt);
    });
    attrEntitySearchResults.appendChild(frag);
    attrEntitySearchResultsWrap.style.display = "";
  }

  function applyEntitySelectionById(entityId) {
    if (
      !attrValueEntityType ||
      !attrValueEntityId ||
      !attrValueEntityNumericId
    ) {
      return;
    }
    if (!entityId) {
      attrValueEntityType.value = "";
      attrValueEntityId.value = "";
      attrValueEntityNumericId.value = "";
      updateEntitySelectionPreview("", "");
      return;
    }
    const found = attrEntitySearchItems.find(
      (it) => (it?.id || "") === entityId,
    );
    const id = entityId;
    const label = found?.label || "";
    let numericValue = found?.numeric_id || "";
    if (!numericValue) {
      const numericMatch = id.match(/(\d+)/);
      numericValue = numericMatch ? numericMatch[1] : "";
    }
    attrValueEntityId.value = id;
    attrValueEntityNumericId.value = numericValue ? String(numericValue) : "";
    attrValueEntityType.value = found?.entity_type || inferEntityTypeFromId(id);
    updateEntitySelectionPreview(label, id);
    if (attrEntitySearchStatus) {
      attrEntitySearchStatus.textContent = label
        ? `已选择：${label} (${id})`
        : `已选择：${id}`;
    }
  }

  async function searchEntitiesByKeyword(keyword) {
    if (!attrEntitySearchStatus) return;
    const term = (keyword || "").trim();
    if (!term) {
      attrEntitySearchStatus.textContent = "请输入检索关键词";
      renderEntitySearchResults([]);
      return;
    }
    attrEntitySearchStatus.textContent = "检索中…";
    try {
      const url = new URL("/api/kb/entity_search", window.location.origin);
      if (typeof window.appendCurrentDbParam === "function") {
        const scopedUrl = window.appendCurrentDbParam(url);
        if (scopedUrl instanceof URL) {
          url.search = scopedUrl.search;
        }
      }
      url.searchParams.set("limit", 20);
      url.searchParams.set("offset", 0);
      url.searchParams.set("q", term);
      const resp = await fetch(url.toString());
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const data = await resp.json();
      const items = Array.isArray(data?.nodes) ? data.nodes : [];
      renderEntitySearchResults(items);
      if (!items.length) {
        attrEntitySearchStatus.textContent = "未找到匹配实体";
      } else {
        attrEntitySearchStatus.textContent = `找到 ${items.length} 条结果`;
        if (attrEntitySearchResults) attrEntitySearchResults.focus();
      }
    } catch (err) {
      console.error("entity search failed", err);
      attrEntitySearchStatus.textContent = "检索失败";
      renderEntitySearchResults([]);
    }
  }

  async function loadEntitySuggestionsForProperty(propertyId, options = {}) {
    const prop = (propertyId || "").trim();
    if (!prop) return;
    const limit = Number(options.limit || 100);
    const cacheKey = `${prop}::${limit}`;
    const now = Date.now();
    try {
      const cached = window.kbPropertySuggestionCache.get(cacheKey);
      if (cached && now - cached.ts < PROPERTY_SUGGESTION_TTL) {
        renderEntitySearchResults(cached.items);
        if (attrEntitySearchStatus) {
          attrEntitySearchStatus.textContent = cached.items.length
            ? `推荐属性值（${cached.items.length} 条）`
            : "暂无推荐属性值";
        }
        return;
      }
    } catch {}
    if (attrEntitySearchStatus)
      attrEntitySearchStatus.textContent = "加载推荐属性值…";
    try {
      const url = new URL(
        "/api/kb/property/value_suggestions",
        window.location.origin,
      );
      if (typeof window.appendCurrentDbParam === "function") {
        const scopedUrl = window.appendCurrentDbParam(url);
        if (scopedUrl instanceof URL) {
          url.search = scopedUrl.search;
        }
      }
      url.searchParams.set("property", prop);
      const currentEntityId = window.kbSelectedRowId || "";
      if (currentEntityId) {
        url.searchParams.set("entity_id", currentEntityId);
      }
      if (limit && Number.isFinite(limit))
        url.searchParams.set("limit", String(limit));
      const resp = await fetch(url.toString());
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const data = await resp.json();
      const items = Array.isArray(data?.items) ? data.items : [];
      try {
        window.kbPropertySuggestionCache.set(cacheKey, {
          ts: now,
          items,
        });
      } catch {}
      const currentProp = (window.kbSelectedSchemaPropId || "").trim();
      const currentInput = (attrProp?.value || "").trim();
      if (
        currentProp &&
        !samePropertyId(currentProp, prop) &&
        (!currentInput || !samePropertyId(currentInput, prop))
      ) {
        return;
      }
      renderEntitySearchResults(items);
      if (attrEntitySearchStatus) {
        attrEntitySearchStatus.textContent = items.length
          ? `推荐属性值（${items.length} 条）`
          : "暂无推荐属性值";
      }
    } catch (err) {
      console.error("loadEntitySuggestionsForProperty failed", err);
      if (attrEntitySearchStatus)
        attrEntitySearchStatus.textContent = "推荐加载失败";
    }
  }

  function clearEntitySearchState() {
    attrEntitySearchItems = [];
    if (attrEntitySearchResults) attrEntitySearchResults.innerHTML = "";
    if (attrEntitySearchResultsWrap)
      attrEntitySearchResultsWrap.style.display = "none";
    if (attrEntitySearchStatus) attrEntitySearchStatus.textContent = "";
    if (attrEntitySearchInput) attrEntitySearchInput.value = "";
    if (attrValueEntityType) attrValueEntityType.value = "";
    if (attrValueEntityId) attrValueEntityId.value = "";
    if (attrValueEntityNumericId) attrValueEntityNumericId.value = "";
    updateEntitySelectionPreview("", "");
  }

  function clearImageUpload() {
    if (attrValueImage) attrValueImage.value = "";
    if (attrValueImageUrl) attrValueImageUrl.value = "";
    if (attrImagePreview) attrImagePreview.style.display = "none";
    if (attrImagePreviewImg) attrImagePreviewImg.src = "";
    if (attrImageFileName) attrImageFileName.textContent = "";
    if (attrImageStateText) attrImageStateText.textContent = "";
  }

  // Handle upload button click - trigger file picker
  if (btnAttrImageUpload) {
    btnAttrImageUpload.addEventListener("click", (e) => {
      e.preventDefault();
      if (attrValueImage) attrValueImage.click();
    });
  }

  // Handle image file selection
  if (attrValueImage) {
    attrValueImage.addEventListener("change", async (e) => {
      const file = e.target.files && e.target.files[0];
      if (!file) {
        clearImageUpload();
        return;
      }
      // Show preview
      const reader = new FileReader();
      reader.onload = (ev) => {
        if (attrImagePreviewImg) attrImagePreviewImg.src = ev.target.result;
        if (attrImagePreview) attrImagePreview.style.display = "block";
        if (attrImageFileName) attrImageFileName.textContent = file.name;
        // Fill URL input with data URL
        if (attrValueImageUrl) attrValueImageUrl.value = ev.target.result;
        if (attrImageStateText)
          attrImageStateText.textContent = `已加载: ${file.name}`;
      };
      reader.readAsDataURL(file);
    });
  }

  // Handle image clear button
  if (btnAttrImageClear) {
    btnAttrImageClear.addEventListener("click", (e) => {
      e.preventDefault();
      clearImageUpload();
    });
  }

  // When user manually types/pastes a URL, update preview
  if (attrValueImageUrl) {
    attrValueImageUrl.addEventListener("input", () => {
      const url = attrValueImageUrl.value.trim();
      if (url && !url.startsWith("data:")) {
        if (attrImagePreviewImg) attrImagePreviewImg.src = url;
        if (attrImagePreview) attrImagePreview.style.display = "block";
        if (attrImageStateText) attrImageStateText.textContent = "";
      } else if (!url) {
        if (attrImagePreview) attrImagePreview.style.display = "none";
        if (attrImagePreviewImg) attrImagePreviewImg.src = "";
      }
    });
  }

  if (attrEntitySearchInput) {
    attrEntitySearchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        searchEntitiesByKeyword(attrEntitySearchInput.value || "");
      }
    });
  }

  if (attrEntitySearchResults) {
    attrEntitySearchResults.addEventListener("change", () => {
      const selectedId = (attrEntitySearchResults.value || "").trim();
      if (selectedId) applyEntitySelectionById(selectedId);
    });
    attrEntitySearchResults.addEventListener("dblclick", () => {
      const selectedId = (attrEntitySearchResults.value || "").trim();
      if (selectedId) applyEntitySelectionById(selectedId);
    });
    attrEntitySearchResults.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        const selectedId = (attrEntitySearchResults.value || "").trim();
        if (selectedId) applyEntitySelectionById(selectedId);
      }
    });
  }

  async function loadAttributes(nodeId) {
    if (!nodeId) {
      attrPanel.style.display = "none";
      return;
    }
    attrPanel.style.display = "";
    attrList.innerHTML = '<div class="muted">加载属性中…</div>';

    const fullId = nodeId.startsWith("entity/") ? nodeId : "entity/" + nodeId;

    // Always fetch from server to ensure fresh data
    const attrUrl = new URL("/api/kb/node/attributes", window.location.origin);
    if (typeof window.appendCurrentDbParam === "function") {
      const scopedUrl = window.appendCurrentDbParam(attrUrl);
      if (scopedUrl instanceof URL) {
        attrUrl.search = scopedUrl.search;
      }
    }
    attrUrl.searchParams.set("id", fullId);
    const resp = await fetch(attrUrl.toString());
    if (!resp.ok) {
      attrList.innerHTML = '<div class="muted">加载失败</div>';
      return;
    }
    const data = await resp.json();
    let items = Array.isArray(data.items) ? data.items : [];
    // store in cache
    try {
      window.kbAttrCache.set(fullId, { ts: Date.now(), items: items });
    } catch {}
    window.kbAttrItems = items;
    try {
      renderAttrList(attrList, items, fullId);
    } catch (e) {
      console.error("renderAttrList failed", e);
    }
    // Update count badge
    try {
      const badge = document.getElementById("attrCountBadge");
      if (badge) badge.textContent = items.length;
    } catch {}
    syncDetailAttrList(items, fullId);
  }

  // 从 kbAttrItems 同步当前节点图像到 cy 图
  function syncCyNodeImage(nodeId) {
    if (!window.kbCy) return;
    try {
      const rawId = (nodeId || "").replace(/^entity\//, "");
      const cyNode = window.kbCy.getElementById(rawId);
      if (!cyNode || !cyNode.length) return;
      const items = window.kbAttrItems || [];
      const imageAttr = items.find(
        (it) =>
          it.datatype === "commonsMedia" ||
          (it.datavalue && it.datavalue.type === "commonsMedia"),
      );
      if (imageAttr) {
        let url = "";
        const v = imageAttr.value;
        if (typeof v === "string") url = v;
        else if (Array.isArray(v) && typeof v[0] === "string") url = v[0];
        cyNode.data("image", url || "");
      } else {
        cyNode.data("image", "");
      }
    } catch {}
  }

  function updateAttrSelectionStyles() {
    try {
      // update left attrList
      try {
        attrList.querySelectorAll("div[data-id]").forEach((el) => {
          const id = el.getAttribute("data-id");
          const valEl = el.children[1];
          if (window.kbSelectedAttrIds.has(id)) {
            el.style.background = "";
            if (valEl) {
              valEl.style.background = "rgba(79,70,229,0.10)";
            }
          } else {
            el.style.background = "";
            if (valEl) {
              valEl.style.background = "";
            }
          }
        });
      } catch (e) {}
      // detail infobox is display-only; do not apply selection highlight there
    } catch {}
  }

  function ensureAttrButtonsState() {
    const count = window.kbSelectedAttrIds.size || 0;
    if (btnAttrEditSelected) btnAttrEditSelected.disabled = count !== 1;
    if (btnAttrDeleteSelected) btnAttrDeleteSelected.disabled = count === 0;
  }

  function extractDateString(value) {
    try {
      if (value == null) return "";
      let raw = "";
      if (typeof value === "string") {
        raw = value;
      } else if (typeof value === "object") {
        if (typeof value.date === "string" && value.date.trim())
          raw = value.date;
        else if (typeof value.time === "string" && value.time.trim())
          raw = value.time;
        else if (typeof value.value === "string" && value.value.trim())
          raw = value.value;
      }
      raw = (raw || "").trim();
      if (!raw) return "";
      if (raw.startsWith("+")) raw = raw.slice(1);
      if (raw.includes("T")) raw = raw.split("T")[0];
      return raw;
    } catch {
      return "";
    }
  }

  function formatAttrValue(dtype, v) {
    try {
      if (dtype === "wikibase-entityid") {
        try {
          if (!v) return "";
          // Prefer a human-readable Chinese label when available
          const lbl =
            v["entity_label_zh"] ??
            v.entity_label_zh ??
            v["label_zh"] ??
            v.label_zh ??
            null;
          if (lbl != null && String(lbl).trim() !== "") return String(lbl);
          // Prefer numeric-id, fallback to id
          const n = v["numeric-id"] ?? v.numeric_id ?? null;
          if (n != null && n !== "") return String(n);
          if (v.id) return String(v.id);
          return JSON.stringify(v);
        } catch {
          return JSON.stringify(v);
        }
      }
      if (dtype === "string" || dtype === "url") return String(v || "");
      if (dtype === "quantity")
        return `${v?.amount ?? ""}${v?.unit ? " " + v.unit : ""}`;
      if (dtype === "time") {
        const txt = extractDateString(v);
        return txt || JSON.stringify(v);
      }
      if (dtype === "globecoordinate")
        return `${v?.latitude ?? ""}, ${v?.longitude ?? ""}`;
      return JSON.stringify(v);
    } catch {
      return String(v || "");
    }
  }

  // Escape HTML in text nodes
  function escapeHtml(s) {
    if (s == null) return "";
    return String(s).replace(/[&<>"']/g, function (c) {
      return {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      }[c];
    });
  }

  // Render attribute value as an HTML string with a span and type-specific class
  function renderAttrValue(dtype, v) {
    let qualifierHtml = "";
    if (v && typeof v === "object" && v.qualifier) {
      qualifierHtml = ` <span class="muted" style="color: var(--muted);">(${escapeHtml(
        v.qualifier,
      )})</span>`;
    }

    const getInner = () => {
      try {
        const dt = (dtype || "").toLowerCase();
        let text = "";
        if (dt === "wikibase-entityid") {
          try {
            if (!v) return '<span class="prop-val null">(空)</span>';
            const lbl =
              v["entity_label_zh"] ??
              v.entity_label_zh ??
              v["label_zh"] ??
              v.label_zh ??
              null;
            if (lbl != null && String(lbl).trim() !== "") text = String(lbl);
            else if (v["numeric-id"] ?? v.numeric_id)
              text = String(v["numeric-id"] ?? v.numeric_id);
            else if (v.id) text = String(v.id);
            else text = JSON.stringify(v);
            return `<span class="prop-val entityid">${escapeHtml(text)}</span>`;
          } catch {
            return `<span class="prop-val object">${escapeHtml(
              JSON.stringify(v),
            )}</span>`;
          }
        }
        if (dt === "string")
          return `<span class="prop-val string">${escapeHtml(
            String(v || ""),
          )}</span>`;
        if (dt === "monolingualtext") {
          try {
            // expect { language: 'en', text: '...' }
            const lang = v?.language || v?.lang || v?.languageCode || "";
            const text = v?.text ?? v?.value ?? "";
            const out = (lang ? `(${lang}) ` : "") + String(text || "");
            return `<span class="prop-val monolingualtext">${escapeHtml(
              out,
            )}</span>`;
          } catch {
            return `<span class="prop-val monolingualtext">${escapeHtml(
              JSON.stringify(v),
            )}</span>`;
          }
        }
        if (dt === "url") {
          const href = escapeHtml(String(v || ""));
          return `<a class="prop-val url" href="${href}" target="_blank" rel="noopener noreferrer">${href}</a>`;
        }
        if (dt === "quantity") {
          const txt = `${v?.amount ?? ""}${v?.unit ? " " + v.unit : ""}`;
          return `<span class="prop-val quantity">${escapeHtml(txt)}</span>`;
        }
        if (dt === "time") {
          const txt = extractDateString(v) || JSON.stringify(v);
          return `<span class="prop-val time">${escapeHtml(txt)}</span>`;
        }
        if (dt === "globecoordinate") {
          const txt = `${v?.latitude ?? ""}, ${v?.longitude ?? ""}`;
          return `<span class="prop-val globecoordinate">${escapeHtml(
            txt,
          )}</span>`;
        }
        if (v === null || v === undefined)
          return `<span class="prop-val null">(空)</span>`;
        if (typeof v === "number")
          return `<span class="prop-val number">${escapeHtml(
            String(v),
          )}</span>`;
        if (typeof v === "boolean")
          return `<span class="prop-val boolean">${escapeHtml(
            String(v),
          )}</span>`;
        if (Array.isArray(v))
          return `<span class="prop-val array">${escapeHtml(
            JSON.stringify(v),
          )}</span>`;
        if (typeof v === "object")
          return `<span class="prop-val object">${escapeHtml(
            JSON.stringify(v),
          )}</span>`;
        return `<span class="prop-val string">${escapeHtml(String(v))}</span>`;
      } catch (e) {
        return `<span class="prop-val string">${escapeHtml(
          String(v || ""),
        )}</span>`;
      }
    };

    return getInner() + qualifierHtml;
  }

  function fillAttrForm(nodeId, it, valueIndex = -1) {
    window.kbEditingValueIndex = valueIndex;
    attrId.value = it?.id || "";
    attrProp.value = it?.property || "";
    attrPropLabel.value = it?.property_label_zh || "";
    const dtype = pickUiDatatype(it) || it?.datatype || "string";
    attrType.value = dtype;
    updateDatatypeUI(dtype, it?.datavalue_type || it?.valuetype || "");

    if (attrValueQualifier && attrValueQualifier.parentElement) {
      const supportsQualifier = [
        "wikibase-entityid",
        "time",
        "quantity",
        "globecoordinate",
        "monolingualtext",
      ].includes(dtype);
      attrValueQualifier.parentElement.style.display = supportsQualifier
        ? "block"
        : "none";
    }

    let val = it?.value;
    if (valueIndex >= 0 && Array.isArray(val)) {
      val = val[valueIndex];
    }

    // Extract qualifier
    let qualifier = "";
    if (val && typeof val === "object" && "qualifier" in val) {
      qualifier = val.qualifier || "";
    }
    attrValueQualifier.value = qualifier;

    // map value back to simple inputs
    try {
      if (dtype === "time") {
        const d = extractDateString(val);
        attrValueDate.value = d;
      } else if (dtype === "wikibase-entityid") {
        // value expected as { "entity-type": "item", "id": "Q...", "numeric-id": 123 }
        try {
          const v = val || {};
          const entityId = v.id || "";
          const numericRaw = v?.["numeric-id"] ?? v?.numeric_id ?? "";
          attrValueEntityType.value =
            v?.["entity-type"] ||
            v?.entity_type ||
            inferEntityTypeFromId(entityId);
          attrValueEntityId.value = entityId;
          attrValueEntityNumericId.value =
            numericRaw !== null && numericRaw !== undefined
              ? String(numericRaw)
              : "";
          const entityLabel =
            v?.entity_label_zh || v?.entity_label || v?.label || "";
          updateEntitySelectionPreview(entityLabel, entityId);
          if (attrEntitySearchStatus) attrEntitySearchStatus.textContent = "";
          renderEntitySearchResults([]);
        } catch {
          attrValueEntityType.value = "";
          attrValueEntityId.value = "";
          attrValueEntityNumericId.value = "";
          updateEntitySelectionPreview("", "");
        }
      } else if (dtype === "quantity") {
        attrValueAmount.value = val?.amount ?? "";
        attrValueUnit.value = val?.unit ?? "";
      } else if (dtype === "globecoordinate") {
        attrValueLat.value = val?.latitude ?? "";
        attrValueLon.value = val?.longitude ?? "";
      } else if (dtype === "url") {
        attrValueUrl.value = typeof val === "string" ? val : "";
      } else if (dtype === "monolingualtext") {
        attrValueMonoText.value = val?.text ?? val?.value ?? "";
        attrValueMonoLang.value = val?.language ?? val?.lang ?? "";
      } else if (dtype === "commonsMedia") {
        // Handle image value - could be URL or data URL
        const imageUrl = typeof val === "string" ? val : "";
        if (imageUrl && attrValueImageUrl) {
          attrValueImageUrl.value = imageUrl;
          if (attrImagePreviewImg) attrImagePreviewImg.src = imageUrl;
          if (attrImagePreview) attrImagePreview.style.display = "block";
          if (attrImageFileName) {
            // Extract filename from URL if possible
            try {
              const urlObj = new URL(imageUrl);
              const filename = urlObj.pathname.split("/").pop() || "已上传图片";
              attrImageFileName.textContent = filename;
              if (attrImageStateText)
                attrImageStateText.textContent = `已选择: ${filename}`;
            } catch {
              const displayName = imageUrl.startsWith("data:")
                ? "已上传图片"
                : imageUrl;
              attrImageFileName.textContent = displayName;
              if (attrImageStateText)
                attrImageStateText.textContent = `已选择: ${displayName}`;
            }
          }
        } else {
          clearImageUpload();
        }
      } else {
        // string or fallback
        attrValue.value =
          typeof val === "string"
            ? val
            : val != null
              ? JSON.stringify(val)
              : "";
      }
    } catch {
      try {
        attrValue.value = "";
        attrValueUrl.value = "";
        attrValueDate.value = "";
        attrValueAmount.value = "";
        attrValueUnit.value = "";
        attrValueLat.value = "";
        attrValueLon.value = "";
        attrValueMonoText.value = "";
        attrValueMonoLang.value = "";
        clearImageUpload();
      } catch {}
    }
    // Reflect selection banner and set global selected prop for editing value only
    try {
      setSelectedSchemaProp(
        it?.property || "",
        it?.property_label_zh || it?.property || "",
      );
      // Also set the hidden input values so form submission works
      if (attrProp) attrProp.value = it?.property || "";
      if (attrPropLabel)
        attrPropLabel.value = it?.property_label_zh || it?.property || "";
    } catch {}
  }

  async function deleteAttr(edgeId) {
    // Backend expects path parameter /api/kb/attributes/<edge_id>
    try {
      const resp = await fetch(
        `/api/kb/attributes/${encodeURIComponent(edgeId)}`,
        { method: "DELETE" },
      );
      if (!resp.ok) throw new Error("HTTP " + resp.status);
    } catch (e) {
      console.error(e);
      alert("删除失败: " + (e.message || e));
    }
  }

  attrForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    attrMsg.textContent = "";
    const nodeId = (fId.value || "").trim();
    if (!nodeId) {
      attrMsg.textContent = "请先选择左侧节点";
      return;
    }
    // Allow editing existing attribute without re-selecting from right panel
    let prop = (window.kbSelectedSchemaPropId || "").trim();
    let plabel = (window.kbSelectedSchemaPropLabel || "").trim();

    // If editing existing attribute (attrId has value), use hidden fields if global selection is empty
    const currentAttrId = (attrId.value || "").trim();
    if (currentAttrId && !prop) {
      prop = (attrProp.value || "").trim();
      plabel = (attrPropLabel.value || "").trim();
    }

    if (!prop) {
      attrMsg.textContent = "请在上方属性选择器中点击选择一个属性";
      return;
    }
    const canonicalProp = canonicalizePropertyId(prop);
    if (!canonicalProp) {
      attrMsg.textContent = "属性ID无效";
      return;
    }
    const raw = (attrValue.value || "").trim();
    // Determine datatype from selected schema property cache (if available)
    let dtype = (attrType.value || "").trim() || "string";
    try {
      const clsId = window.kbSelectedClassId;
      const items = clsId ? window.kbSchemaByClassId?.[clsId] || [] : [];
      const found = items.find((it) => it.id === prop);
      if (found) {
        const mapped =
          pickUiDatatype(found) || found.datatype || found.datavalue_type;
        if (mapped) dtype = mapped;
      }
    } catch {}
    attrType.value = dtype;
    let value = null;
    if (dtype === "url") {
      value = (attrValueUrl.value || "").trim();
      if (!value) {
        attrMsg.textContent = "请输入URL";
        return;
      }
    } else if (dtype === "time") {
      const d = (attrValueDate.value || "").trim();
      if (!d) {
        attrMsg.textContent = "请选择日期";
        return;
      }
      value = { date: d };
    } else if (dtype === "quantity") {
      const amtStr = (attrValueAmount.value || "").trim();
      if (!amtStr) {
        attrMsg.textContent = "请输入数值";
        return;
      }
      const amt = parseFloat(amtStr);
      if (Number.isNaN(amt)) {
        attrMsg.textContent = "数值格式错误";
        return;
      }
      value = { amount: amt, unit: (attrValueUnit.value || "").trim() };
    } else if (dtype === "globecoordinate") {
      const latStr = (attrValueLat.value || "").trim();
      const lonStr = (attrValueLon.value || "").trim();
      if (!latStr || !lonStr) {
        attrMsg.textContent = "请输入经纬度";
        return;
      }
      const lat = parseFloat(latStr);
      const lon = parseFloat(lonStr);
      if (Number.isNaN(lat) || Number.isNaN(lon)) {
        attrMsg.textContent = "经纬度格式错误";
        return;
      }
      value = { latitude: lat, longitude: lon };
    } else if (dtype === "wikibase-entityid") {
      const entityType = (attrValueEntityType.value || "").trim();
      const entityId = (attrValueEntityId.value || "").trim();
      const numericIdStr = (attrValueEntityNumericId.value || "").trim();
      if (!entityType || !entityId || !numericIdStr) {
        attrMsg.textContent = "请填写全部wikibase-entityid字段";
        return;
      }
      const numericId = parseInt(numericIdStr, 10);
      if (Number.isNaN(numericId)) {
        attrMsg.textContent = "numeric-id格式错误";
        return;
      }
      value = {
        "entity-type": entityType,
        id: entityId,
        "numeric-id": numericId,
      };
    } else if (dtype === "monolingualtext") {
      const text = (attrValueMonoText.value || "").trim();
      const lang = (attrValueMonoLang.value || "").trim();
      if (!text || !lang) {
        attrMsg.textContent = "请输入文本和语言代码";
        return;
      }
      value = { text, language: lang };
    } else if (dtype === "commonsMedia") {
      // Handle image upload
      const imageUrl = (attrValueImageUrl.value || "").trim();
      if (!imageUrl) {
        attrMsg.textContent = "请选择图片文件";
        return;
      }
      value = imageUrl;
    } else {
      // fallback treat as string
      if (!raw) {
        attrMsg.textContent = "请输入属性值";
        return;
      }
      value = raw;
    }

    // Attach qualifier if present and value is an object
    const qualifier = (attrValueQualifier.value || "").trim();
    if (qualifier) {
      if (value && typeof value === "object") {
        value.qualifier = qualifier;
      }
    }

    const body = {
      id: (attrId.value || "").trim() || undefined,
      node_id: nodeId,
      property: canonicalProp,
      property_label_zh: plabel || undefined,
      datatype: dtype,
      value,
    };

    // Handle array update if editing a specific index
    if (
      body.id &&
      typeof window.kbEditingValueIndex === "number" &&
      window.kbEditingValueIndex >= 0
    ) {
      const originalItem = Array.isArray(window.kbAttrItems)
        ? window.kbAttrItems.find((it) => it.id === body.id)
        : null;
      if (originalItem && Array.isArray(originalItem.value)) {
        const newArray = [...originalItem.value];
        if (window.kbEditingValueIndex < newArray.length) {
          newArray[window.kbEditingValueIndex] = value;
          body.value = newArray;
        }
      }
    }

    attrMsg.textContent = "保存中…";
    try {
      const saveUrl = new URL(
        "/api/kb/attributes/save",
        window.location.origin,
      );
      if (typeof window.appendCurrentDbParam === "function") {
        const scopedUrl = window.appendCurrentDbParam(saveUrl);
        if (scopedUrl instanceof URL) {
          saveUrl.search = scopedUrl.search;
        }
      }
      const resp = await fetch(saveUrl.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        let detail = "";
        try {
          const j = await resp.json();
          detail = j?.detail || j?.error || "";
        } catch {}
        throw new Error("HTTP " + resp.status + (detail ? ": " + detail : ""));
      }
      await resp.json();
      attrMsg.textContent = "已保存";
      await loadAttributes(nodeId);
      // 保存图像属性后同步更新关系图节点
      syncCyNodeImage(nodeId);

      // 自动将属性绑定到当前节点的类型（如果类型已设置且属性不在模型中）
      try {
        const clsId = window.kbSelectedClassId;
        if (clsId && canonicalProp) {
          const existing = Array.isArray(window.kbSchemaByClassId?.[clsId])
            ? window.kbSchemaByClassId[clsId]
            : [];
          const alreadyBound = existing.some((it) => it.id === canonicalProp);
          if (!alreadyBound && typeof window.addClassSchema === "function") {
            await window.addClassSchema(clsId, canonicalProp);
            // 更新本地缓存并刷新属性选择器
            if (!window.kbSchemaByClassId)
              window.kbSchemaByClassId = Object.create(null);
            if (!Array.isArray(window.kbSchemaByClassId[clsId])) {
              window.kbSchemaByClassId[clsId] = [];
            }
            window.kbSchemaByClassId[clsId].push({
              id: canonicalProp,
              name: plabel || canonicalProp,
              label: plabel || canonicalProp,
              datatype: dtype,
            });
            if (typeof window.loadAttrPropPicker === "function") {
              const fTypeEl = document.getElementById("fType");
              const typeVal = (fTypeEl?.value || "").trim();
              if (typeVal) await window.loadAttrPropPicker(typeVal);
            }
            attrMsg.textContent = "已保存并绑定到类型";
          }
        }
      } catch (bindErr) {
        console.warn("auto-bind property to class failed", bindErr);
      }

      // 保存后清除id和表单内容，便于连续录入
      attrId.value = "";
      attrProp.value = "";
      attrPropLabel.value = "";
      attrType.value = "string";
      attrValue.value = "";
      try {
        attrValueUrl.value = "";
        attrValueDate.value = "";
        attrValueAmount.value = "";
        attrValueUnit.value = "";
        attrValueLat.value = "";
        attrValueLon.value = "";
        attrValueMonoText.value = "";
        attrValueMonoLang.value = "";
      } catch {}
      clearEntitySearchState();
      updateDatatypeUI("string");
    } catch (e) {
      console.error(e);
      attrMsg.textContent = "保存失败";
    }
  });

  btnAttrReset.addEventListener("click", resetAttrForm);
  if (btnAttrEditSelected)
    btnAttrEditSelected.addEventListener("click", async () => {
      if (window.kbSelectedAttrIds.size !== 1) return;
      const id = Array.from(window.kbSelectedAttrIds)[0];
      const nodeIdRaw = (fId.value || "").trim();
      if (!nodeIdRaw) return;
      // 自动加 entity/ 前缀（如果没有）
      const nodeId = nodeIdRaw.startsWith("entity/")
        ? nodeIdRaw
        : "entity/" + nodeIdRaw;
      try {
        const url = new URL("/api/kb/node/attributes", window.location.origin);
        if (typeof window.appendCurrentDbParam === "function") {
          const scopedUrl = window.appendCurrentDbParam(url);
          if (scopedUrl instanceof URL) {
            url.search = scopedUrl.search;
          }
        }
        url.searchParams.set("id", nodeId);
        const resp = await fetch(url.toString());
        const data = await resp.json();
        const arr = Array.isArray(data.items) ? data.items : [];
        const found = arr.find((x) => x.id === id);
        if (found) fillAttrForm(nodeId, found);
      } catch {}
    });
  if (btnAttrDeleteSelected)
    btnAttrDeleteSelected.addEventListener("click", async () => {
      if (!window.kbSelectedAttrIds.size) return;
      if (
        !confirm(`确定删除选中的 ${window.kbSelectedAttrIds.size} 个属性值？`)
      )
        return;
      const nodeId = (fId.value || "").trim();

      const deletions = new Map();
      for (const uniqueId of Array.from(window.kbSelectedAttrIds)) {
        const parts = uniqueId.split("::");
        const id = parts[0];
        const index = parts.length > 1 ? parseInt(parts[1], 10) : -1;
        if (!deletions.has(id)) deletions.set(id, new Set());
        if (index >= 0) deletions.get(id).add(index);
      }

      for (const [id, indices] of deletions.entries()) {
        try {
          const originalItem = Array.isArray(window.kbAttrItems)
            ? window.kbAttrItems.find((it) => it.id === id)
            : null;
          if (originalItem && Array.isArray(originalItem.value)) {
            const newArray = originalItem.value.filter(
              (_, idx) => !indices.has(idx),
            );
            if (newArray.length === 0) {
              await deleteAttr(id);
            } else {
              const body = {
                id: id,
                node_id: nodeId,
                property: originalItem.property,
                datatype: originalItem.datatype,
                value: newArray,
              };
              const saveUrl = new URL(
                "/api/kb/attributes/save",
                window.location.origin,
              );
              if (typeof window.appendCurrentDbParam === "function") {
                const scopedUrl = window.appendCurrentDbParam(saveUrl);
                if (scopedUrl instanceof URL) {
                  saveUrl.search = scopedUrl.search;
                }
              }
              await fetch(saveUrl.toString(), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
              });
            }
          } else {
            await deleteAttr(id);
          }
        } catch (e) {
          console.error(e);
        }
      }
      window.kbSelectedAttrIds.clear();
      await loadAttributes(nodeId);
      syncCyNodeImage(nodeId);
    });

  window.mapDatatypeToUi = mapDatatypeToUi;
  window.pickUiDatatype = pickUiDatatype;
  window.canonicalizePropertyId = canonicalizePropertyId;
  window.propertyIdToApiPath = propertyIdToApiPath;
  window.samePropertyId = samePropertyId;
  window.renderAttrList = renderAttrList;
  window.syncDetailAttrList = syncDetailAttrList;
  window.resetAttrForm = resetAttrForm;
  window.updateDatatypeUI = updateDatatypeUI;
  window.clearEntitySearchState = clearEntitySearchState;
  window.loadAttributes = loadAttributes;
  window.updateAttrSelectionStyles = updateAttrSelectionStyles;
  window.ensureAttrButtonsState = ensureAttrButtonsState;
  window.fillAttrForm = fillAttrForm;
  window.deleteAttr = deleteAttr;
  window.syncCyNodeImage = syncCyNodeImage;
})();
