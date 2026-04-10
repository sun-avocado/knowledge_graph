(function () {
  const root = window;
  const app = (root.kbApp = root.kbApp || {});
  const store = (app.store = app.store || Object.create(null));

  function cloneDefault(value) {
    if (typeof value === "function") return value();
    if (Array.isArray(value)) return value.slice();
    if (value && typeof value === "object") {
      if (value instanceof Set) return new Set(value);
      if (value instanceof Map) return new Map(value);
      return { ...value };
    }
    return value;
  }

  function ensure(key, defaultValue) {
    if (!(key in store)) {
      store[key] = cloneDefault(defaultValue);
    }
    return store[key];
  }

  function get(key, fallback) {
    return key in store ? store[key] : fallback;
  }

  function set(key, value) {
    store[key] = value;
    return value;
  }

  function patch(values) {
    if (!values || typeof values !== "object") return store;
    Object.keys(values).forEach((key) => {
      store[key] = values[key];
    });
    return store;
  }

  function bindAlias(alias, key, defaultValue) {
    ensure(key, defaultValue);
    const desc = Object.getOwnPropertyDescriptor(root, alias);
    if (desc && !desc.configurable) return;

    Object.defineProperty(root, alias, {
      configurable: true,
      enumerable: true,
      get() {
        return store[key];
      },
      set(value) {
        store[key] = value;
      },
    });
  }

  function byId(id) {
    return document.getElementById(id);
  }

  function qs(selector, scope) {
    return (scope || document).querySelector(selector);
  }

  function qsa(selector, scope) {
    return Array.from((scope || document).querySelectorAll(selector));
  }

  app.state = {
    ensure,
    get,
    set,
    patch,
    bindAlias,
  };

  app.dom = {
    byId,
    qs,
    qsa,
  };

  bindAlias("kbSelectedRowId", "selectedRowId", "");
  bindAlias("kbSelectedRowIds", "selectedRowIds", () => new Set());
  bindAlias("kbLastAnchorRowId", "lastAnchorRowId", "");
  bindAlias("kbSelectionHydrated", "selectionHydrated", false);
  bindAlias("kbSelectedAttrIds", "selectedAttrIds", () => new Set());
  bindAlias("kbLastAttrAnchorId", "lastAttrAnchorId", "");
  bindAlias("kbAttrCache", "attrCache", () => new Map());
  bindAlias("kbPropertySuggestionCache", "propertySuggestionCache", () => new Map());
  bindAlias("kbSelectedClassId", "selectedClassId", null);
  bindAlias("kbClasses", "classes", () => []);
  bindAlias("kbEntityClasses", "entityClasses", () => []);
  bindAlias("kbSchemaByClassId", "schemaByClassId", () => Object.create(null));
  bindAlias("kbSelectedSchemaPropId", "selectedSchemaPropId", "");
  bindAlias("kbSelectedSchemaPropLabel", "selectedSchemaPropLabel", "");
  bindAlias("kbSchemaRemovalSelection", "schemaRemovalSelection", () => new Set());
  bindAlias("kbSchemaRemovalLastIndex", "schemaRemovalLastIndex", -1);
  bindAlias("kbCollapsedClassIds", "collapsedClassIds", () => new Set());
  bindAlias("kbClassTreeInitiallyCollapsed", "classTreeInitiallyCollapsed", true);
  bindAlias("kbClassMeta", "classMeta", null);
  bindAlias("kbClassDragSourceId", "classDragSourceId", null);
  bindAlias("kbPropertyRecommendationsCache", "propertyRecommendationsCache", () => new Map());
  bindAlias("kbPickMode", "pickMode", null);
  bindAlias("kbActiveDetailNodeId", "activeDetailNodeId", "");
  bindAlias("kbActiveDetailRouteId", "activeDetailRouteId", "");
  bindAlias("kbActiveVisNodeId", "activeVisNodeId", "");
  bindAlias("kbTableNodes", "tableNodes", () => []);
  bindAlias("propertySelectedIds", "propertySelectedIds", () => new Set());
})();
