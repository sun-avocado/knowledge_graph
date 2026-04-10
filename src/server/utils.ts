import { db } from "./db.ts";

// ── Formatters ───────────────────────────────────────────────────────────────

export function formatNode(row: any) {
  let aliases = [] as any[];
  let tags = [] as any[];
  let extraData = {} as Record<string, any>;
  try {
    aliases = JSON.parse(row.aliases || "[]");
  } catch {}
  try {
    tags = JSON.parse(row.tags || "[]");
  } catch {}
  try {
    extraData = JSON.parse(row.data || "{}");
  } catch {}

  const normalizeList = (val: any): any[] => {
    if (Array.isArray(val))
      return val.filter((v) => v !== null && v !== undefined);
    if (!val && val !== "") return [];
    if (typeof val === "string") {
      const trimmed = val.trim();
      if (!trimmed) return [];
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) return parsed;
      } catch {}
      return trimmed
        .split(/[\n,，;、]+/)
        .map((item) => item.trim())
        .filter(Boolean);
    }
    return [];
  };

  const descZhFromExtra =
    typeof extraData.desc_zh === "string" && extraData.desc_zh.trim()
      ? extraData.desc_zh.trim()
      : typeof extraData.description === "string" &&
          extraData.description.trim()
        ? extraData.description.trim()
        : typeof extraData.desc === "string" && extraData.desc.trim()
          ? extraData.desc.trim()
          : "";
  const aliasesFromExtra = normalizeList(
    extraData.aliases_zh ?? extraData.aliases ?? extraData.alias,
  );
  const tagsFromExtra = normalizeList(extraData.tags ?? extraData.tag_list);

  let classes = [];
  try {
    classes = db
      .query(
        `SELECT c.id, c.name, c.color FROM classes c
         JOIN entity_classes ec ON c.id = ec.class_id
         WHERE ec.entity_id = ?`,
      )
      .all(row.id) as any[];
  } catch {}

  let color = null;
  let classId = null;
  let classLabel = null;

  if (classes.length > 0) {
    const cls = classes[0];
    if (cls.color) color = cls.color;
    classId = cls.id;
    classLabel = cls.name;
  }

  let image = "";
  try {
    let imageProp = db
      .query("SELECT id FROM properties WHERE name = ? OR name = ? LIMIT 1")
      .get("图像", "image") as any;
    let imagePropId = imageProp && imageProp.id ? imageProp.id : null;
    if (imagePropId) {
      const imageAttr = db
        .query(
          "SELECT value FROM attributes WHERE node_id = ? AND key = ? LIMIT 1",
        )
        .get(row.id, imagePropId) as any;
      if (imageAttr && imageAttr.value) {
        try {
          const parsed = JSON.parse(imageAttr.value);
          if (typeof parsed === "string") {
            image = parsed;
          } else if (
            Array.isArray(parsed) &&
            parsed.length > 0 &&
            typeof parsed[0] === "string"
          ) {
            image = parsed[0];
          }
        } catch {
          image = imageAttr.value;
        }
      }
    }
  } catch {}

  if (!image) {
    if (typeof extraData.image === "string" && extraData.image.trim()) {
      image = extraData.image.trim();
    } else if (typeof extraData.icon === "string" && extraData.icon.trim()) {
      image = extraData.icon.trim();
    } else if (
      typeof extraData.avatar === "string" &&
      extraData.avatar.trim()
    ) {
      image = extraData.avatar.trim();
    } else if (typeof extraData.img === "string" && extraData.img.trim()) {
      image = extraData.img.trim();
    } else if (typeof extraData.logo === "string" && extraData.logo.trim()) {
      image = extraData.logo.trim();
    }
  }

  return {
    ...extraData,
    id: row.id,
    _id: row.id,
    name: row.name,
    label: row.name,
    label_zh: extraData.label_zh ?? row.name,
    type: row.type,
    description: row.description ?? descZhFromExtra ?? "",
    desc_zh: descZhFromExtra || row.description || "",
    aliases: aliases.length ? aliases : aliasesFromExtra,
    aliases_zh: aliasesFromExtra.length ? aliasesFromExtra : aliases,
    tags: tags.length ? tags : tagsFromExtra,
    color: color,
    classId: classId,
    classLabel: classLabel,
    classes: classes,
    image: image,
  };
}

export function formatEdge(row: any) {
  let extraData = {};
  try {
    extraData = JSON.parse(row.data || "{}");
  } catch {}

  let label = row.type;
  try {
    let prop = db
      .query("SELECT name FROM properties WHERE id = ?")
      .get(row.type) as any;

    if (
      (!prop || !prop.name) &&
      typeof row.type === "string" &&
      row.type.startsWith("P")
    ) {
      const stripped = row.type.substring(1);
      const propStripped = db
        .query("SELECT name FROM properties WHERE id = ?")
        .get(stripped) as any;
      if (propStripped && propStripped.name) {
        prop = propStripped;
      }
    }

    if (prop && prop.name) {
      label = prop.name;
    }
  } catch {}

  return {
    id: row.id,
    source: row.source,
    target: row.target,
    label: label,
    ...extraData,
  };
}

export function formatAttribute(row: any) {
  let parsedValue: any = row.value;
  try {
    parsedValue = JSON.parse(row.value);
  } catch {}

  let propName = row.key;
  try {
    let prop = db
      .query("SELECT name FROM properties WHERE id = ?")
      .get(row.key) as any;
    if (
      (!prop || !prop.name) &&
      typeof row.key === "string" &&
      row.key.startsWith("P")
    ) {
      const stripped = row.key.substring(1);
      const propStripped = db
        .query("SELECT name FROM properties WHERE id = ?")
        .get(stripped) as any;
      if (propStripped && propStripped.name) {
        prop = propStripped;
      }
    }
    if (prop && prop.name) {
      propName = prop.name;
    }
  } catch {}

  if (
    row.datatype === "wikibase-entityid" &&
    parsedValue &&
    typeof parsedValue === "object"
  ) {
    try {
      let entityId =
        parsedValue.id ??
        parsedValue.value?.id ??
        parsedValue["entity-id"] ??
        parsedValue["entityId"] ??
        parsedValue["entity_id"] ??
        null;

      if (!entityId && (parsedValue["numeric-id"] ?? parsedValue.numeric_id)) {
        entityId = parsedValue["numeric-id"] ?? parsedValue.numeric_id;
      }

      if (entityId != null) {
        let targetId = String(entityId);
        if (targetId.startsWith("entity/")) {
          targetId = targetId.substring("entity/".length);
        }

        const candidateIds = new Set<string>([targetId]);
        if (/^[Qq]\d+$/.test(targetId)) {
          candidateIds.add(targetId.substring(1));
        }

        let resolvedNode: any = null;
        for (const candidate of candidateIds) {
          resolvedNode = db
            .query("SELECT name FROM nodes WHERE id = ? LIMIT 1")
            .get(candidate) as any;
          if (resolvedNode && resolvedNode.name) break;
        }

        if (resolvedNode && resolvedNode.name) {
          parsedValue.entity_label_zh = resolvedNode.name;
          parsedValue.label_zh = resolvedNode.name;
        }
      }
    } catch {}
  }

  return {
    id: row.id,
    node_id: row.node_id,
    property: row.key,
    property_label_zh: propName,
    datatype: row.datatype,
    value: parsedValue,
    datavalue: {
      value: parsedValue,
      type: row.datatype,
    },
  };
}

// ── ID Generators ─────────────────────────────────────────────────────────────

export function getNextNumericNodeId(): string {
  const maxIdResult = db
    .query(
      "SELECT MAX(CAST(id AS INTEGER)) as maxId FROM nodes WHERE id GLOB '[0-9]*'",
    )
    .get() as any;
  const nextId = (maxIdResult?.maxId || 0) + 1;
  return nextId.toString();
}

export function getNextNumericPropertyId(): string {
  const maxIdResult = db
    .query(
      "SELECT MAX(CAST(id AS INTEGER)) as maxId FROM properties WHERE id GLOB '[0-9]*'",
    )
    .get() as any;
  const nextId = (maxIdResult?.maxId || 0) + 1;
  return nextId.toString();
}

// ── Normalizers ───────────────────────────────────────────────────────────────

export const ENTRY_VALUE_BACKEND_SPLIT = /[\s,，;；、\n\u3000]+/g;

export function normalizeEntryValueList(values: any, fallback: any): string[] {
  let source: string[] = [];
  if (Array.isArray(values) && values.length) {
    source = values.map((item: any) => (item ?? "").toString());
  } else if (fallback !== undefined && fallback !== null) {
    const raw = fallback.toString();
    source = raw
      .split(ENTRY_VALUE_BACKEND_SPLIT)
      .map((item: string) => item.trim())
      .filter(Boolean);
  }
  return Array.from(
    new Set(source.map((item: string) => item.trim()).filter(Boolean)),
  );
}

export function canonicalizePropertyKey(prop: string): string {
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
    return Number.isFinite(num) ? `P${num}` : `P${prefixed[1]}`;
  }
  if (/^\d+$/.test(upper)) {
    const num = parseInt(upper, 10);
    return Number.isFinite(num) ? `P${num}` : `P${upper}`;
  }
  const anyDigits = upper.match(/(\d+)/);
  if (anyDigits && anyDigits[1]) {
    const num = parseInt(anyDigits[1], 10);
    if (Number.isFinite(num)) return `P${num}`;
  }
  return upper;
}

export function extractNumericPropertyId(value: string): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const prefixed = trimmed.match(/^P\s*0*(\d+)$/i);
  if (prefixed && prefixed[1]) {
    const num = parseInt(prefixed[1], 10);
    if (Number.isFinite(num)) return num.toString();
  }
  if (/^\d+$/.test(trimmed)) {
    const num = parseInt(trimmed, 10);
    if (Number.isFinite(num)) return num.toString();
  }
  return null;
}

// ── DB-Dependent Helpers ──────────────────────────────────────────────────────

export type EnsurePropertyRecordResult = {
  id: string | null;
  created: boolean;
};

export function ensurePropertyRecord(
  propertyId: string,
  label: string,
  valuetype?: string,
): EnsurePropertyRecordResult {
  const rawId = (propertyId || "").trim();
  const rawLabel = (label || "").trim() || rawId;

  if (!rawId && !rawLabel) {
    return { id: null, created: false };
  }

  const candidateSet = new Set<string>();

  const pushCandidates = (value: string) => {
    if (!value) return;
    const trimmed = value.trim();
    if (!trimmed) return;
    candidateSet.add(trimmed);
    const canonical = canonicalizePropertyKey(trimmed);
    if (canonical) {
      candidateSet.add(canonical);
      const numericFromCanonical = extractNumericPropertyId(canonical);
      if (numericFromCanonical) candidateSet.add(numericFromCanonical);
    }
    const numericDirect = extractNumericPropertyId(trimmed);
    if (numericDirect) candidateSet.add(numericDirect);
  };

  pushCandidates(rawId);
  pushCandidates(rawLabel);

  const candidates = Array.from(candidateSet)
    .map((value) => value.trim())
    .filter(Boolean);

  for (const candidate of candidates) {
    try {
      const existing = db
        .query("SELECT id FROM properties WHERE id = ? LIMIT 1")
        .get(candidate) as any;
      if (existing?.id) {
        return { id: existing.id, created: false };
      }
    } catch {}
  }

  if (rawLabel) {
    try {
      const existingByName = db
        .query("SELECT id FROM properties WHERE lower(name) = lower(?) LIMIT 1")
        .get(rawLabel) as any;
      if (existingByName?.id) {
        return { id: existingByName.id, created: false };
      }
    } catch {}
  }

  const newId = getNextNumericPropertyId();
  const propertyName = rawLabel || rawId || newId;

  try {
    db.run(
      "INSERT INTO properties (id, name, datatype, valuetype, types, description) VALUES (?, ?, ?, ?, ?, ?)",
      [newId, propertyName, "string", valuetype || null, "[]", ""],
    );
    return { id: newId, created: true };
  } catch (err) {
    console.warn("ensurePropertyRecord failed", err);
    try {
      const fallback = db
        .query("SELECT id FROM properties WHERE id = ? LIMIT 1")
        .get(newId) as any;
      if (fallback?.id) {
        return { id: fallback.id, created: false };
      }
    } catch {}
    return { id: newId, created: false };
  }
}

export type AttributeRecordOptions = {
  datatype?: string;
};

export function parseStoredAttributeValues(
  raw: any,
  datatype = "string",
): any[] {
  if (raw === undefined || raw === null) return [];
  const text = raw.toString();
  if (!text) return [];
  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (parsed && typeof parsed === "object") {
      return [parsed];
    }
    if (
      typeof parsed === "string" ||
      typeof parsed === "number" ||
      typeof parsed === "boolean"
    ) {
      const val = parsed.toString();
      return val ? [val] : [];
    }
  } catch {}
  if (datatype === "wikibase-entityid") {
    return [];
  }
  const trimmed = text.trim();
  return trimmed ? [trimmed] : [];
}

export function normalizeStringAttributeValues(values: any[]): string[] {
  return Array.from(
    new Set(
      (values || []).map((v) => (v ?? "").toString().trim()).filter(Boolean),
    ),
  );
}

export type EntityAttributeValue = {
  "entity-type"?: string;
  id: string;
  "numeric-id"?: number;
  label_zh?: string;
  label?: string;
  name?: string;
  qualifier?: string;
};

export function extractEntityId(value: any): string {
  if (!value) return "";
  const candidates = [
    value.id,
    value["entity-id"],
    value.entity_id,
    value.entityId,
    value.value?.id,
    value.value?.entity_id,
    value.value?.entityId,
    value.nodeId,
    value.node_id,
    value.target,
  ];
  for (const candidate of candidates) {
    if (!candidate && candidate !== 0) continue;
    const text = candidate.toString().trim();
    if (!text) continue;
    return text.startsWith("entity/") ? text.substring(7) : text;
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (text) return text.startsWith("entity/") ? text.substring(7) : text;
  }
  return "";
}

export function normalizeEntityAttributeValues(
  values: any[],
): EntityAttributeValue[] {
  const map = new Map<string, EntityAttributeValue>();
  for (const raw of values || []) {
    const id = extractEntityId(raw);
    if (!id) continue;
    const entityType =
      raw?.["entity-type"] ||
      raw?.entity_type ||
      raw?.entityType ||
      raw?.value?.["entity-type"] ||
      raw?.value?.entity_type ||
      raw?.value?.entityType ||
      "item";
    const numericIdRaw =
      raw?.["numeric-id"] ??
      raw?.numeric_id ??
      raw?.numericId ??
      raw?.value?.["numeric-id"] ??
      raw?.value?.numeric_id ??
      raw?.value?.numericId;
    const numericId = Number(numericIdRaw);
    const label =
      raw?.label_zh ??
      raw?.label ??
      raw?.name ??
      raw?.value?.label_zh ??
      raw?.value?.label ??
      raw?.value?.name ??
      undefined;
    const qualifier = raw?.qualifier ?? raw?.value?.qualifier ?? undefined;
    map.set(id, {
      "entity-type": entityType,
      id,
      ...(Number.isFinite(numericId)
        ? { "numeric-id": Number(numericId) }
        : {}),
      ...(label ? { label_zh: label, label } : {}),
      ...(qualifier ? { qualifier } : {}),
    });
  }
  return Array.from(map.values());
}

export function serializeAttributeValues(
  values: any[],
  datatype: string,
): string {
  if (datatype === "string") {
    if (values.length <= 1) {
      return values.length ? values[0]!.toString() : "";
    }
    return JSON.stringify(values);
  }
  if (values.length === 1) {
    return JSON.stringify(values[0]);
  }
  return JSON.stringify(values);
}

export function ensureAttributeRecord(
  nodeId: string,
  propertyId: string,
  values: any[],
  options: AttributeRecordOptions = {},
): { created: boolean; updated: boolean } {
  if (!nodeId || !propertyId) {
    return { created: false, updated: false };
  }
  const datatype = (options.datatype || "string").trim() || "string";
  const normalizedValues =
    datatype === "wikibase-entityid"
      ? normalizeEntityAttributeValues(values)
      : normalizeStringAttributeValues(values);
  if (!normalizedValues.length) {
    return { created: false, updated: false };
  }
  const existing = db
    .query(
      "SELECT id, value, datatype FROM attributes WHERE node_id = ? AND key = ? LIMIT 1",
    )
    .get(nodeId, propertyId) as any;
  if (!existing) {
    const id = `attr/${crypto.randomUUID()}`;
    const serialized = serializeAttributeValues(normalizedValues, datatype);
    db.run(
      "INSERT INTO attributes (id, node_id, key, value, datatype) VALUES (?, ?, ?, ?, ?)",
      [id, nodeId, propertyId, serialized, datatype],
    );
    return { created: true, updated: false };
  }
  const existingDatatype = (existing.datatype || "string").trim() || "string";
  let existingValues = parseStoredAttributeValues(
    existing.value,
    existingDatatype,
  );
  if (
    existingDatatype === "wikibase-entityid" &&
    datatype === "wikibase-entityid"
  ) {
    existingValues = normalizeEntityAttributeValues(existingValues);
  } else if (existingDatatype === "string" && datatype === "string") {
    existingValues = normalizeStringAttributeValues(existingValues);
  } else {
    existingValues = [];
  }

  let mergedValues: any[] = [];
  if (datatype === "wikibase-entityid") {
    const map = new Map<string, EntityAttributeValue>();
    for (const val of existingValues as EntityAttributeValue[]) {
      map.set(val.id, val);
    }
    for (const val of normalizedValues as EntityAttributeValue[]) {
      map.set(val.id, val);
    }
    mergedValues = Array.from(map.values());
  } else {
    mergedValues = Array.from(
      new Set([
        ...(existingValues as string[]),
        ...(normalizedValues as string[]),
      ]),
    );
  }

  const previousSerialized = serializeAttributeValues(existingValues, datatype);
  const serialized = serializeAttributeValues(mergedValues, datatype);
  if (serialized === previousSerialized) {
    return { created: false, updated: false };
  }
  db.run("UPDATE attributes SET value = ?, datatype = ? WHERE id = ?", [
    serialized,
    datatype,
    existing.id,
  ]);
  return { created: false, updated: true };
}

export function ensureNodeByName(
  name: string,
  options: { description?: string; projectId?: number | null } = {},
): { node: any; created: boolean } {
  const normalized = (name ?? "").toString().trim();
  if (!normalized) {
    throw new Error("Node name is required");
  }
  const projectId =
    typeof options.projectId === "number" && Number.isFinite(options.projectId)
      ? options.projectId
      : null;
  const existing =
    projectId !== null
      ? (db
          .query(
            "SELECT * FROM nodes WHERE lower(name) = lower(?) AND project_id = ? LIMIT 1",
          )
          .get(normalized, projectId) as any)
      : (db
          .query("SELECT * FROM nodes WHERE lower(name) = lower(?) LIMIT 1")
          .get(normalized) as any);
  const nextDescription = (options.description ?? "").toString().trim();
  if (existing) {
    const currentDescription = (existing.description ?? "").toString().trim();
    if (nextDescription && nextDescription !== currentDescription) {
      db.run(
        "UPDATE nodes SET description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [nextDescription, existing.id],
      );
      const refreshed = db
        .query("SELECT * FROM nodes WHERE id = ?")
        .get(existing.id) as any;
      return { node: refreshed, created: false };
    }
    return { node: existing, created: false };
  }
  const id = getNextNumericNodeId();
  db.run(
    "INSERT INTO nodes (id, name, type, description, aliases, tags, project_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
    [id, normalized, "entity", nextDescription, "[]", "[]", projectId],
  );
  const created = db.query("SELECT * FROM nodes WHERE id = ?").get(id) as any;
  return { node: created, created: true };
}
