import { db, getProjectByIdentifier } from "../db.ts";
import {
  formatNode,
  formatAttribute,
  formatEdge,
  normalizeEntryValueList,
  ensureNodeByName,
  canonicalizePropertyKey,
  ensurePropertyRecord,
  ensureAttributeRecord,
  getNextNumericNodeId,
} from "../utils.ts";

type EntityAttributeValue = {
  [key: string]: any;
};

export async function handleCoreKbRoutes(
  req: Request,
  url: URL,
  method: string,
) {
  const dbParam = (url.searchParams.get("db") || "").trim();
  const scopedProject =
    dbParam && dbParam !== "app" ? getProjectByIdentifier(dbParam) : null;
  const scopedProjectId = Number(scopedProject?.id || 0) || null;
  const hasProjectScope = scopedProjectId !== null;
  const scopedClause = (alias = "") => {
    const prefix = alias ? `${alias}.` : "";
    return `${prefix}project_id = ?`;
  };
  const applyScope = <T extends any[]>(params: T) =>
    hasProjectScope ? [...params, scopedProjectId] : params;

  const parsePropertyTypes = (raw: any): string[] => {
    if (!raw && raw !== "") return [];
    if (Array.isArray(raw)) {
      return raw.map((item) => (item ?? "").toString().trim()).filter(Boolean);
    }
    const text = raw.toString().trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        return parsed
          .map((item) => (item ?? "").toString().trim())
          .filter(Boolean);
      }
    } catch {}
    return text
      .split(/[\n,，;；、|]+/g)
      .map((item) => item.trim())
      .filter(Boolean);
  };

  const syncPropertyTypeForNode = (propertyId: string, nodeId: string) => {
    const pid = (propertyId || "").toString().trim();
    const nid = (nodeId || "")
      .toString()
      .trim()
      .replace(/^entity\//, "");
    if (!pid || !nid) return;
    try {
      const nodeRow = hasProjectScope
        ? (db
            .query(`SELECT type FROM nodes WHERE id = ? AND ${scopedClause()}`)
            .get(nid, scopedProjectId) as any)
        : (db.query("SELECT type FROM nodes WHERE id = ?").get(nid) as any);
      const typeName = (nodeRow?.type || "").toString().trim();
      if (!typeName) return;

      const propRow = db
        .query("SELECT types FROM properties WHERE id = ? LIMIT 1")
        .get(pid) as any;
      if (!propRow) return;

      const merged = Array.from(
        new Set([...parsePropertyTypes(propRow.types), typeName]),
      );
      db.run(
        "UPDATE properties SET types = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [JSON.stringify(merged), pid],
      );
    } catch (err) {
      console.warn("syncPropertyTypeForNode failed", err);
    }
  };

  const syncAllPropertyTypesForNode = (nodeId: string) => {
    const nid = (nodeId || "")
      .toString()
      .trim()
      .replace(/^entity\//, "");
    if (!nid) return;
    try {
      const rows = db
        .query("SELECT DISTINCT key FROM attributes WHERE node_id = ?")
        .all(nid) as any[];
      for (const row of rows) {
        const key = (row?.key || "").toString().trim();
        if (!key) continue;
        syncPropertyTypeForNode(key, nid);
      }
    } catch (err) {
      console.warn("syncAllPropertyTypesForNode failed", err);
    }
  };

  if (url.pathname === "/api/kb/graph" && method === "GET") {
    // 加载当前项目的所有节点
    const nodesQuery = hasProjectScope
      ? `SELECT * FROM nodes WHERE ${scopedClause()}`
      : `SELECT * FROM nodes`;
    const nodesParams = hasProjectScope ? [scopedProjectId] : [];
    const nodes = db
      .query(nodesQuery)
      .all(...nodesParams)
      .map(formatNode);

    // 构建当前项目的节点ID集合
    const nodeIdSet = new Set(nodes.map((n: any) => String(n.id)));

    const edges = [];
    // 加载当前项目节点的关系属性
    const attrsQuery = hasProjectScope
      ? `SELECT a.* FROM attributes a 
         INNER JOIN nodes n ON a.node_id = n.id 
         WHERE a.datatype = 'wikibase-entityid' AND n.project_id = ?`
      : `SELECT * FROM attributes WHERE datatype = 'wikibase-entityid'`;
    const attrs = (
      hasProjectScope
        ? db.query(attrsQuery).all(scopedProjectId)
        : db.query(attrsQuery).all()
    ) as any[];
    for (const attr of attrs) {
      try {
        const vals = JSON.parse(attr.value);
        const list = Array.isArray(vals) ? vals : [vals];
        for (const val of list) {
          if (val && val.id) {
            let targetId = val.id;
            if (targetId.startsWith("entity/")) {
              targetId = targetId.replace("entity/", "");
            }
            // 只添加目标节点也在当前项目中的边
            if (!hasProjectScope || nodeIdSet.has(targetId)) {
              edges.push(
                formatEdge({
                  id: attr.id + ":" + targetId,
                  source: attr.node_id,
                  target: targetId,
                  type: attr.key,
                  data: JSON.stringify({
                    isAttribute: true,
                    qualifier: val.qualifier,
                  }),
                }),
              );
            }
          }
        }
      } catch {}
    }
    return Response.json({
      nodes,
      edges,
      counts: { nodes: nodes.length, edges: edges.length },
    });
  }

  if (url.pathname === "/api/kb/node/graph" && method === "GET") {
    let id = url.searchParams.get("id");
    if (!id) return new Response("Missing id", { status: 400 });

    if (id.startsWith("entity/")) {
      const stripped = id.replace("entity/", "");
      const nodeExists = hasProjectScope
        ? db
            .query(`SELECT 1 FROM nodes WHERE id = ? AND ${scopedClause()}`)
            .get(stripped, scopedProjectId)
        : db.query("SELECT 1 FROM nodes WHERE id = ?").get(stripped);
      if (nodeExists) id = stripped;
    }

    const centerNode = hasProjectScope
      ? db
          .query(`SELECT * FROM nodes WHERE id = ? AND ${scopedClause()}`)
          .get(id, scopedProjectId)
      : db.query("SELECT * FROM nodes WHERE id = ?").get(id);
    if (!centerNode) return Response.json({ nodes: [], edges: [] });

    const edges: any[] = [];
    const outgoingAttrs = db
      .query(
        "SELECT * FROM attributes WHERE node_id = ? AND datatype = 'wikibase-entityid'",
      )
      .all(id) as any[];

    const processAttr = (attr: any) => {
      try {
        const vals = JSON.parse(attr.value);
        const list = Array.isArray(vals) ? vals : [vals];
        for (const val of list) {
          if (val && val.id) {
            let targetId = val.id;
            if (targetId.startsWith("entity/")) {
              targetId = targetId.replace("entity/", "");
            }

            edges.push(
              formatEdge({
                id: attr.id + ":" + targetId,
                source: attr.node_id,
                target: targetId,
                type: attr.key,
                data: JSON.stringify({
                  isAttribute: true,
                  qualifier: val.qualifier,
                }),
              }),
            );
          }
        }
      } catch {}
    };

    outgoingAttrs.forEach(processAttr);

    const incomingAttrs = db
      .query(
        "SELECT * FROM attributes WHERE datatype = 'wikibase-entityid' AND value LIKE ?",
      )
      .all(`%${id}%`) as any[];
    incomingAttrs.forEach((attr) => {
      try {
        const vals = JSON.parse(attr.value);
        const list = Array.isArray(vals) ? vals : [vals];
        const pointsToId = list.some((v: any) => {
          let tid = v?.id;
          if (tid && tid.startsWith("entity/"))
            tid = tid.replace("entity/", "");
          return tid === id;
        });
        if (pointsToId) processAttr(attr);
      } catch {}
    });

    const neighborIds = new Set<string>();
    neighborIds.add(id);
    edges.forEach((e: any) => {
      neighborIds.add(e.source);
      neighborIds.add(e.target);
    });

    const nodes = db
      .query(
        `SELECT * FROM nodes WHERE id IN (${Array.from(neighborIds)
          .map(() => "?")
          .join(",")})${hasProjectScope ? ` AND ${scopedClause()}` : ""}`,
      )
      .all(
        ...Array.from(neighborIds),
        ...(hasProjectScope ? [scopedProjectId] : []),
      )
      .map(formatNode);

    return Response.json({ nodes, edges });
  }

  if (url.pathname === "/api/kb/entity_search" && method === "GET") {
    const q = url.searchParams.get("q") || "";
    const limit = parseInt(url.searchParams.get("limit") || "20");
    const offset = parseInt(url.searchParams.get("offset") || "0");
    const classId = (url.searchParams.get("class_id") || "").trim();

    const likeParam = `%${q}%`;
    const params: any[] = [likeParam];
    const countParams: any[] = [likeParam];

    let joinClause = "";
    let whereClause = "WHERE n.name LIKE ?";

    if (classId) {
      joinClause += " INNER JOIN entity_classes ec ON ec.entity_id = n.id";
      whereClause += " AND ec.class_id = ?";
      params.push(classId);
      countParams.push(classId);
    }

    if (hasProjectScope) {
      whereClause += ` AND ${scopedClause("n")}`;
      params.push(scopedProjectId);
      countParams.push(scopedProjectId);
    }

    const nodes = db
      .query(
        `SELECT DISTINCT n.* FROM nodes n${joinClause} ${whereClause} LIMIT ? OFFSET ?`,
      )
      .all(...params, limit, offset)
      .map(formatNode);

    const total = db
      .query(
        `SELECT COUNT(DISTINCT n.id) as count FROM nodes n${joinClause} ${whereClause}`,
      )
      .get(...countParams) as any;

    return Response.json({ nodes, total: total?.count || 0 });
  }

  if (url.pathname === "/api/kb/entry/single" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const targetName = (body?.targetName ?? "").toString().trim();
      if (!targetName) {
        return Response.json({ error: "缺少目标名称" }, { status: 400 });
      }

      const targetDescription = (body?.targetDescription ?? "")
        .toString()
        .trim();
      const attributesRaw = Array.isArray(body?.attributes)
        ? body.attributes
        : [];
      const attributes = attributesRaw
        .map((attr: any) => {
          const property = (attr?.property ?? "").toString().trim();
          const values = normalizeEntryValueList(attr?.values, attr?.rawValue);
          return { property, values };
        })
        .filter((attr: { property: string; values: string[] }) => {
          return Boolean(attr.property) && attr.values.length > 0;
        });

      const summary = {
        targetId: "",
        createdNodes: 0,
        reusedNodes: 0,
        createdEdges: 0,
        reusedEdges: 0,
        createdAttributes: 0,
        updatedAttributes: 0,
        valueNodes: [] as Array<{ id: string; name: string; property: string }>,
      };

      const targetResult = ensureNodeByName(targetName, {
        description: targetDescription,
        ...(hasProjectScope ? { projectId: scopedProjectId } : {}),
      });
      summary.targetId = targetResult.node.id;
      if (targetResult.created) summary.createdNodes += 1;
      else summary.reusedNodes += 1;

      const targetNameLower = targetName.toLowerCase();
      for (const attr of attributes) {
        const propertyLabel = attr.property;
        const propertyKey =
          canonicalizePropertyKey(propertyLabel) || propertyLabel;
        const propertyRecord = ensurePropertyRecord(propertyKey, propertyLabel);
        const propertyId = (propertyRecord.id || propertyKey || "")
          .toString()
          .trim();
        if (!propertyId) continue;

        const entityAttributeValues: EntityAttributeValue[] = [];
        for (const rawValue of attr.values) {
          let normalizedValue = rawValue.toString().trim();
          let qualifier = "";
          const match = normalizedValue.match(/^(.*?)[\(（](.*?)[\)）]$/);
          if (match) {
            normalizedValue = match[1].trim();
            qualifier = match[2].trim();
          }

          if (!normalizedValue) continue;
          if (normalizedValue.toLowerCase() === targetNameLower) continue;
          const valueResult = ensureNodeByName(
            normalizedValue,
            hasProjectScope ? { projectId: scopedProjectId } : {},
          );
          if (valueResult.created) summary.createdNodes += 1;
          else summary.reusedNodes += 1;
          summary.valueNodes.push({
            id: valueResult.node.id,
            name: valueResult.node.name,
            property: propertyLabel,
          });
          const numericId = Number(valueResult.node.id);
          entityAttributeValues.push({
            "entity-type": "item",
            id: valueResult.node.id.toString(),
            ...(Number.isFinite(numericId) ? { "numeric-id": numericId } : {}),
            label_zh: valueResult.node.name,
            label: valueResult.node.name,
            name: valueResult.node.name,
            qualifier: qualifier || undefined,
          });
        }

        let attributeChanges = { created: false, updated: false };
        if (entityAttributeValues.length) {
          attributeChanges = ensureAttributeRecord(
            targetResult.node.id,
            propertyId,
            entityAttributeValues,
            { datatype: "wikibase-entityid" },
          );
        } else if (attr.values.length) {
          attributeChanges = ensureAttributeRecord(
            targetResult.node.id,
            propertyId,
            attr.values,
            {
              datatype: "string",
            },
          );
        }
        syncPropertyTypeForNode(propertyId, targetResult.node.id);
        if (attributeChanges.created) summary.createdAttributes += 1;
        if (attributeChanges.updated) summary.updatedAttributes += 1;
      }

      const formattedTarget = formatNode(
        db.query("SELECT * FROM nodes WHERE id = ?").get(summary.targetId),
      );
      return Response.json({ ok: true, target: formattedTarget, summary });
    } catch (err) {
      console.error(err);
      return new Response("Single entry import failed", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/nodes" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      let id = body.id;
      if (!id) id = getNextNumericNodeId();
      id = id.toString();
      const name = body.name || "New Node";
      const type = body.type || "entity";
      const desc = body.description || "";
      const aliases = JSON.stringify(body.aliases || []);
      const tags = JSON.stringify(body.tags || []);

      db.run(
        "INSERT INTO nodes (id, name, type, description, aliases, tags, project_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [id, name, type, desc, aliases, tags, scopedProjectId],
      );

      const newNode = hasProjectScope
        ? db
            .query(`SELECT * FROM nodes WHERE id = ? AND ${scopedClause()}`)
            .get(id, scopedProjectId)
        : db.query("SELECT * FROM nodes WHERE id = ?").get(id);
      return Response.json({ ok: true, node: formatNode(newNode) });
    } catch (e) {
      console.error(e);
      return new Response("Error creating node", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/nodes/update" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const id = body.id;
      if (!id) return new Response("Missing id", { status: 400 });

      const updates = [];
      const params = [];

      if (body.name !== undefined) {
        updates.push("name = ?");
        params.push(body.name);
      }
      if (body.description !== undefined) {
        updates.push("description = ?");
        params.push(body.description);
      }
      if (body.aliases !== undefined) {
        updates.push("aliases = ?");
        params.push(JSON.stringify(body.aliases));
      }
      if (body.tags !== undefined) {
        updates.push("tags = ?");
        params.push(JSON.stringify(body.tags));
      }
      if (body.type !== undefined) {
        updates.push("type = ?");
        params.push(body.type !== null ? String(body.type) : null);
      }

      if (updates.length > 0) {
        params.push(id);
        const scopeSql = hasProjectScope ? ` AND ${scopedClause()}` : "";
        db.run(
          `UPDATE nodes SET ${updates.join(", ")} WHERE id = ?${scopeSql}`,
          hasProjectScope ? [...params, scopedProjectId] : params,
        );
      }

      const updatedNode = hasProjectScope
        ? db
            .query(`SELECT * FROM nodes WHERE id = ? AND ${scopedClause()}`)
            .get(id, scopedProjectId)
        : db.query("SELECT * FROM nodes WHERE id = ?").get(id);
      if (body.type !== undefined) {
        syncAllPropertyTypesForNode(id);
      }
      return Response.json({ ok: true, node: formatNode(updatedNode) });
    } catch (e) {
      console.error(e);
      return new Response("Error updating node", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/nodes" && method === "DELETE") {
    let idParam = url.searchParams.get("id");
    if (!idParam) return new Response("Missing id", { status: 400 });
    idParam = idParam.trim();
    if (!idParam) return new Response("Missing id", { status: 400 });

    const normalizedId = idParam.startsWith("entity/")
      ? idParam.replace("entity/", "")
      : idParam;
    const prefixedId = normalizedId.startsWith("entity/")
      ? normalizedId
      : `entity/${normalizedId}`;
    const candidateIds = Array.from(
      new Set(
        [normalizedId, idParam, prefixedId].filter(
          (value) => typeof value === "string" && value.trim(),
        ),
      ),
    );

    if (!candidateIds.length) {
      return new Response("Missing id", { status: 400 });
    }

    const placeholders = candidateIds.map(() => "?").join(",");
    const scopedIds = hasProjectScope
      ? (
          db
            .query(
              `SELECT id FROM nodes WHERE id IN (${placeholders}) AND ${scopedClause()}`,
            )
            .all(...candidateIds, scopedProjectId) as any[]
        ).map((row) => row.id)
      : candidateIds;
    if (!scopedIds.length) return Response.json({ success: true });
    const scopedPlaceholders = scopedIds.map(() => "?").join(",");
    db.run(
      `DELETE FROM attributes WHERE node_id IN (${scopedPlaceholders})`,
      scopedIds,
    );
    db.run(
      `DELETE FROM entity_classes WHERE entity_id IN (${scopedPlaceholders})`,
      scopedIds,
    );
    db.run(`DELETE FROM nodes WHERE id IN (${scopedPlaceholders})`, scopedIds);

    return Response.json({ success: true });
  }

  if (url.pathname === "/api/kb/node" && method === "GET") {
    let id = url.searchParams.get("id");
    if (!id) return new Response("Missing id", { status: 400 });

    let node = hasProjectScope
      ? db
          .query(`SELECT * FROM nodes WHERE id = ? AND ${scopedClause()}`)
          .get(id, scopedProjectId)
      : db.query("SELECT * FROM nodes WHERE id = ?").get(id);
    if (!node && id.startsWith("entity/")) {
      const strippedId = id.replace("entity/", "");
      node = hasProjectScope
        ? db
            .query(`SELECT * FROM nodes WHERE id = ? AND ${scopedClause()}`)
            .get(strippedId, scopedProjectId)
        : db.query("SELECT * FROM nodes WHERE id = ?").get(strippedId);
      if (node) id = strippedId;
    }

    if (!node) return new Response("Not found", { status: 404 });

    const neighborIds = new Set<string>();
    const outgoingAttrs = db
      .query(
        "SELECT value FROM attributes WHERE node_id = ? AND datatype = 'wikibase-entityid'",
      )
      .all(id) as any[];
    outgoingAttrs.forEach((attr) => {
      try {
        const vals = JSON.parse(attr.value);
        const list = Array.isArray(vals) ? vals : [vals];
        list.forEach((v: any) => {
          if (v?.id) {
            let tid = v.id;
            if (tid.startsWith("entity/")) tid = tid.replace("entity/", "");
            neighborIds.add(tid);
          }
        });
      } catch {}
    });

    const incomingAttrs = db
      .query(
        "SELECT node_id, value FROM attributes WHERE datatype = 'wikibase-entityid' AND value LIKE ?",
      )
      .all(`%${id}%`) as any[];
    incomingAttrs.forEach((attr) => {
      try {
        const vals = JSON.parse(attr.value);
        const list = Array.isArray(vals) ? vals : [vals];
        const pointsToId = list.some((v: any) => {
          let tid = v?.id;
          if (tid && tid.startsWith("entity/"))
            tid = tid.replace("entity/", "");
          return tid === id;
        });
        if (pointsToId) neighborIds.add(attr.node_id);
      } catch {}
    });

    const neighbors = [];
    if (neighborIds.size > 0) {
      const neighborNodes = db
        .query(
          `SELECT * FROM nodes WHERE id IN (${Array.from(neighborIds)
            .map(() => "?")
            .join(",")})${hasProjectScope ? ` AND ${scopedClause()}` : ""}`,
        )
        .all(
          ...Array.from(neighborIds),
          ...(hasProjectScope ? [scopedProjectId] : []),
        )
        .map(formatNode);
      neighbors.push(...neighborNodes);
    }

    return Response.json({ node: formatNode(node), neighbors });
  }

  if (url.pathname === "/api/kb/node/attributes" && method === "GET") {
    let id = url.searchParams.get("id");
    if (!id) return new Response("Missing id", { status: 400 });

    if (id.startsWith("entity/")) {
      const stripped = id.replace("entity/", "");
      const nodeExists = hasProjectScope
        ? db
            .query(`SELECT 1 FROM nodes WHERE id = ? AND ${scopedClause()}`)
            .get(stripped, scopedProjectId)
        : db.query("SELECT 1 FROM nodes WHERE id = ?").get(stripped);
      if (nodeExists) id = stripped;
    }

    const attrs = db
      .query("SELECT * FROM attributes WHERE node_id = ?")
      .all(id)
      .map(formatAttribute);
    return Response.json({ items: attrs });
  }

  if (url.pathname === "/api/kb/attributes/save" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const nodeId = body.node_id || body.entity_id;
      const prop = body.property || body.prop;
      const value = body.value;
      const datatype = body.datatype || "string";
      const id = body.id || `attr/${crypto.randomUUID()}`;

      const existing = db
        .query("SELECT * FROM attributes WHERE id = ?")
        .get(id);
      const valueStr =
        typeof value === "object" ? JSON.stringify(value) : String(value);

      if (existing) {
        db.run(
          "UPDATE attributes SET key = ?, value = ?, datatype = ? WHERE id = ?",
          [prop, valueStr, datatype, id],
        );
      } else {
        db.run(
          "INSERT INTO attributes (id, node_id, key, value, datatype) VALUES (?, ?, ?, ?, ?)",
          [id, nodeId, prop, valueStr, datatype],
        );
      }
      syncPropertyTypeForNode(prop, nodeId);

      const saved = db.query("SELECT * FROM attributes WHERE id = ?").get(id);
      return Response.json(formatAttribute(saved));
    } catch (e) {
      console.error(e);
      return new Response("Error saving attribute", { status: 500 });
    }
  }

  if (url.pathname.startsWith("/api/kb/attributes/") && method === "DELETE") {
    const prefix = "/api/kb/attributes/";
    let id = url.pathname.substring(prefix.length);
    if (id && id !== "blacklist") {
      id = decodeURIComponent(id);
      db.run("DELETE FROM attributes WHERE id = ?", [id]);
      return Response.json({ success: true });
    }
  }

  if (url.pathname === "/api/kb/node/relations" && method === "GET") {
    let id = url.searchParams.get("id");
    if (!id) return new Response("Missing id", { status: 400 });

    if (id.startsWith("entity/")) {
      const stripped = id.replace("entity/", "");
      const nodeExists = hasProjectScope
        ? db
            .query(`SELECT 1 FROM nodes WHERE id = ? AND ${scopedClause()}`)
            .get(stripped, scopedProjectId)
        : db.query("SELECT 1 FROM nodes WHERE id = ?").get(stripped);
      if (nodeExists) id = stripped;
    }

    const edges: any[] = [];
    const outgoingAttrs = db
      .query(
        "SELECT * FROM attributes WHERE node_id = ? AND datatype = 'wikibase-entityid'",
      )
      .all(id) as any[];
    const processAttr = (attr: any) => {
      try {
        const vals = JSON.parse(attr.value);
        const list = Array.isArray(vals) ? vals : [vals];
        for (const val of list) {
          if (val && val.id) {
            let targetId = val.id;
            if (targetId.startsWith("entity/"))
              targetId = targetId.replace("entity/", "");
            edges.push(
              formatEdge({
                id: attr.id + ":" + targetId,
                source: attr.node_id,
                target: targetId,
                type: attr.key,
                data: JSON.stringify({ isAttribute: true }),
              }),
            );
          }
        }
      } catch {}
    };
    outgoingAttrs.forEach(processAttr);

    const incomingAttrs = db
      .query(
        "SELECT * FROM attributes WHERE datatype = 'wikibase-entityid' AND value LIKE ?",
      )
      .all(`%${id}%`) as any[];
    incomingAttrs.forEach((attr) => {
      try {
        const vals = JSON.parse(attr.value);
        const list = Array.isArray(vals) ? vals : [vals];
        const pointsToId = list.some((v: any) => {
          let tid = v?.id;
          if (tid && tid.startsWith("entity/"))
            tid = tid.replace("entity/", "");
          return tid === id;
        });
        if (pointsToId) processAttr(attr);
      } catch {}
    });

    return Response.json({ items: edges });
  }

  if (url.pathname === "/api/kb/edge" && method === "GET") {
    const id = url.searchParams.get("id");
    if (!id) return new Response("Missing id", { status: 400 });

    const parts = id.split(":");
    if (parts.length < 2)
      return new Response("Invalid edge id", { status: 400 });

    const attrId = parts[0] as string;
    const targetId = parts.slice(1).join(":");
    const attr = db
      .query("SELECT * FROM attributes WHERE id = ?")
      .get(attrId) as any;
    if (!attr) return new Response("Not found", { status: 404 });

    try {
      const vals = JSON.parse(attr.value);
      const list = Array.isArray(vals) ? vals : [vals];
      const found = list.find((v: any) => {
        let tid = v?.id;
        if (tid && tid.startsWith("entity/")) tid = tid.replace("entity/", "");
        return tid === targetId;
      });

      if (found) {
        return Response.json(
          formatEdge({
            id,
            source: attr.node_id,
            target: targetId,
            type: attr.key,
            data: JSON.stringify({ isAttribute: true }),
          }),
        );
      }
    } catch {}

    return new Response("Not found", { status: 404 });
  }

  if (url.pathname === "/api/kb/relations/create" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const source = body.source;
      const target = body.target;
      const type = body.type || "related";

      let targetId = target;
      if (targetId.startsWith("entity/"))
        targetId = targetId.replace("entity/", "");
      const targetNode = db
        .query("SELECT name FROM nodes WHERE id = ?")
        .get(targetId) as any;
      const targetName = targetNode?.name || targetId;

      const entityValue: EntityAttributeValue = {
        "entity-type": "item",
        id: targetId,
        label: targetName,
        label_zh: targetName,
      };

      const propRec = ensurePropertyRecord(type, type);
      const propId = propRec.id || type;

      ensureAttributeRecord(source, propId, [entityValue], {
        datatype: "wikibase-entityid",
      });
      syncPropertyTypeForNode(propId, source);

      const attr = db
        .query("SELECT * FROM attributes WHERE node_id = ? AND key = ?")
        .get(source, propId) as any;

      return Response.json(
        formatEdge({
          id: attr.id + ":" + targetId,
          source,
          target: targetId,
          type: propId,
          data: JSON.stringify({ isAttribute: true }),
        }),
      );
    } catch (e) {
      console.error(e);
      return new Response("Error creating edge", { status: 500 });
    }
  }

  if (url.pathname.startsWith("/api/kb/relations/") && method === "DELETE") {
    const prefix = "/api/kb/relations/";
    let id = url.pathname.substring(prefix.length);

    if (id) {
      id = decodeURIComponent(id);
      const parts = id.split(":");
      if (parts.length >= 2) {
        const attrId = parts[0] as string;
        const targetId = parts.slice(1).join(":");

        const attr = db
          .query("SELECT * FROM attributes WHERE id = ?")
          .get(attrId) as any;
        if (attr) {
          try {
            const vals = JSON.parse(attr.value);
            let list = Array.isArray(vals) ? vals : [vals];
            const initialLen = list.length;
            list = list.filter((v: any) => {
              let tid = v?.id;
              if (tid && tid.startsWith("entity/"))
                tid = tid.replace("entity/", "");
              return tid !== targetId;
            });

            if (list.length !== initialLen) {
              if (list.length === 0) {
                db.run("DELETE FROM attributes WHERE id = ?", [attrId]);
              } else {
                db.run("UPDATE attributes SET value = ? WHERE id = ?", [
                  JSON.stringify(list),
                  attrId,
                ]);
              }
            }
          } catch {}
        }
      }
      return Response.json({ success: true });
    }
  }

  if (url.pathname === "/api/kb/stats" && method === "GET") {
    const nodeCount = hasProjectScope
      ? (db
          .query(`SELECT COUNT(*) as count FROM nodes WHERE ${scopedClause()}`)
          .get(scopedProjectId) as any)
      : (db.query("SELECT COUNT(*) as count FROM nodes").get() as any);
    const classCount = db
      .query("SELECT COUNT(*) as count FROM classes")
      .get() as any;
    const propertyCount = db
      .query("SELECT COUNT(*) as count FROM properties")
      .get() as any;
    const attributeCount = hasProjectScope
      ? (db
          .query(
            `SELECT COUNT(*) as count
             FROM attributes
             WHERE node_id IN (SELECT id FROM nodes WHERE ${scopedClause()})`,
          )
          .get(scopedProjectId) as any)
      : (db.query("SELECT COUNT(*) as count FROM attributes").get() as any);

    return Response.json({
      counts: {
        entity: nodeCount.count,
        link: attributeCount.count,
        instance: classCount.count,
        property: propertyCount.count,
      },
    });
  }

  return null;
}
