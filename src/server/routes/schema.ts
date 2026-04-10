import { db, getProjectByIdentifier } from "../db.ts";
import { resolve } from "path";
import { mkdirSync, writeFileSync } from "fs";

const UPLOADS_DIR = resolve(import.meta.dir, "..", "..", "..", "..", "uploads");
const CLASS_IMAGES_DIR = resolve(UPLOADS_DIR, "class-images");
mkdirSync(CLASS_IMAGES_DIR, { recursive: true });

function resolveScopedProject(req: Request, url: URL) {
  const directDb = (url.searchParams.get("db") || "").trim();
  if (directDb && directDb !== "app") {
    return getProjectByIdentifier(directDb);
  }

  try {
    const referer = (req.headers.get("referer") || "").trim();
    if (referer) {
      const refererUrl = new URL(referer);
      const refererDb = (refererUrl.searchParams.get("db") || "").trim();
      if (refererDb && refererDb !== "app") {
        return getProjectByIdentifier(refererDb);
      }
    }
  } catch {}

  return null;
}

export async function handleSchemaRoutes(
  req: Request,
  url: URL,
  method: string,
) {
  const parseTypes = (raw: any): string[] => {
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

  const scopedProject = resolveScopedProject(req, url);
  const scopedProjectId = Number(scopedProject?.id || 0) || null;
  const hasProjectScope = scopedProjectId !== null;

  if (url.pathname === "/api/kb/classes" && method === "GET") {
    const q = url.searchParams.get("q") || "";
    const querySql = hasProjectScope
      ? `WITH RECURSIVE scoped_classes(id) AS (
           SELECT c.id
           FROM classes c
           WHERE c.project_id = ?
              OR (
                c.project_id IS NULL
                AND EXISTS (
                  SELECT 1
                  FROM entity_classes ec
                  INNER JOIN nodes n ON n.id = ec.entity_id
                  WHERE ec.class_id = c.id AND n.project_id = ?
                )
                AND NOT EXISTS (
                  SELECT 1
                  FROM entity_classes ec
                  INNER JOIN nodes n ON n.id = ec.entity_id
                  WHERE ec.class_id = c.id AND n.project_id <> ?
                )
              )
           UNION
           SELECT parent.id
           FROM classes parent
           INNER JOIN classes child ON child.parent_id = parent.id
           INNER JOIN scoped_classes scoped ON scoped.id = child.id
         )
         SELECT c.*, (
           SELECT COUNT(*)
           FROM entity_classes ec
           INNER JOIN nodes n ON n.id = ec.entity_id
           WHERE ec.class_id = c.id AND n.project_id = ?
         ) AS instance_count
         FROM classes c
         WHERE c.name LIKE ?
           AND c.id IN (SELECT id FROM scoped_classes)
         ORDER BY COALESCE(c.sort_order, c.rowid), c.name`
      : `SELECT c.*, (
           SELECT COUNT(*) FROM entity_classes ec WHERE ec.class_id = c.id
         ) AS instance_count
         FROM classes c
         WHERE c.name LIKE ?
         ORDER BY COALESCE(c.sort_order, c.rowid), c.name`;
    const classes = db
      .query(querySql)
      .all(
        ...(hasProjectScope
          ? [
              scopedProjectId,
              scopedProjectId,
              scopedProjectId,
              scopedProjectId,
              `%${q}%`,
            ]
          : [`%${q}%`]),
      )
      .map((row: any) => {
        let rowTags: string[] = [];
        try {
          rowTags = JSON.parse(row.tags || "[]");
        } catch {}
        return {
          id: row.id,
          name: row.name,
          label: row.name,
          description: row.description,
          parent: row.parent_id,
          project_id: row.project_id ?? null,
          color: row.color,
          image: row.image,
          sort_order: row.sort_order,
          instance_count: row.instance_count ?? 0,
          tags: rowTags,
        };
      });
    return Response.json(classes);
  }

  if (url.pathname === "/api/kb/classes" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const id = body.id || `class/${crypto.randomUUID()}`;
      const name = String(body.name || "New Class").trim();
      const desc = body.description || "";
      const parentId = body.parent_id || null;
      const projectId = hasProjectScope
        ? scopedProjectId
        : body.project_id || null;
      const color = body.color || null;
      const image = body.image || null;
      if (!name) return new Response("Missing name", { status: 400 });

      const existingClass = parentId
        ? (db
            .query(
              `SELECT *
               FROM classes
               WHERE name = ?
                 AND parent_id = ?
                 AND ((project_id IS NULL AND ? IS NULL) OR project_id = ?)
               LIMIT 1`,
            )
            .get(name, parentId, projectId, projectId) as any)
        : (db
            .query(
              `SELECT *
               FROM classes
               WHERE name = ?
                 AND parent_id IS NULL
                 AND ((project_id IS NULL AND ? IS NULL) OR project_id = ?)
               LIMIT 1`,
            )
            .get(name, projectId, projectId) as any);
      if (existingClass) {
        return Response.json({
          id: existingClass.id,
          name: existingClass.name,
          label: existingClass.name,
          description: existingClass.description,
          parent: existingClass.parent_id,
          project_id: existingClass.project_id ?? null,
          color: existingClass.color,
          image: existingClass.image,
          sort_order: existingClass.sort_order,
          deduped: true,
        });
      }

      const sortOrderRaw = body.sort_order;
      let sortOrder =
        typeof sortOrderRaw === "number" ? sortOrderRaw : Number(sortOrderRaw);

      if (!Number.isFinite(sortOrder)) {
        let querySql = "";
        let row: any = null;
        if (parentId) {
          querySql =
            "SELECT COALESCE(MAX(sort_order), 0) as max_order FROM classes WHERE parent_id = ? AND ((project_id IS NULL AND ? IS NULL) OR project_id = ?)";
          row = db.query(querySql).get(parentId, projectId, projectId) as any;
        } else {
          querySql =
            "SELECT COALESCE(MAX(sort_order), 0) as max_order FROM classes WHERE parent_id IS NULL AND ((project_id IS NULL AND ? IS NULL) OR project_id = ?)";
          row = db.query(querySql).get(projectId, projectId) as any;
        }
        sortOrder = (row?.max_order || 0) + 1;
      }

      db.run(
        "INSERT INTO classes (id, name, description, parent_id, project_id, color, image, sort_order) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [id, name, desc, parentId, projectId, color, image, sortOrder],
      );

      const newClass = db
        .query("SELECT * FROM classes WHERE id = ?")
        .get(id) as any;
      return Response.json({
        id: newClass.id,
        name: newClass.name,
        label: newClass.name,
        description: newClass.description,
        parent: newClass.parent_id,
        project_id: newClass.project_id ?? null,
        color: newClass.color,
        image: newClass.image,
        sort_order: newClass.sort_order,
      });
    } catch (e) {
      console.error(e);
      return new Response("Error creating class", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/classes/update" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const id = body.id;
      if (!id) return new Response("Missing id", { status: 400 });
      if (hasProjectScope) {
        const existing = db
          .query(
            "SELECT id, project_id FROM classes WHERE id = ? AND (project_id = ? OR project_id IS NULL)",
          )
          .get(id, scopedProjectId) as any;
        if (!existing) return new Response("Class not found", { status: 404 });
      }

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
      if (body.parent_id !== undefined) {
        updates.push("parent_id = ?");
        params.push(body.parent_id);
      }
      if (body.color !== undefined) {
        updates.push("color = ?");
        params.push(body.color);
      }
      if (body.image !== undefined) {
        updates.push("image = ?");
        params.push(body.image);
      }
      if (body.sort_order !== undefined) {
        const orderVal =
          typeof body.sort_order === "number"
            ? body.sort_order
            : Number(body.sort_order);
        updates.push("sort_order = ?");
        params.push(Number.isFinite(orderVal) ? orderVal : null);
      }
      if (body.tags !== undefined) {
        updates.push("tags = ?");
        params.push(
          body.tags !== null
            ? JSON.stringify(Array.isArray(body.tags) ? body.tags : [])
            : null,
        );
      }
      if (hasProjectScope) {
        updates.push("project_id = COALESCE(project_id, ?)");
        params.push(scopedProjectId);
      }

      if (updates.length > 0) {
        if (hasProjectScope) {
          params.push(id, scopedProjectId);
          db.run(
            `UPDATE classes SET ${updates.join(", ")} WHERE id = ? AND (project_id = ? OR project_id IS NULL)`,
            params,
          );
        } else {
          params.push(id);
          db.run(
            `UPDATE classes SET ${updates.join(", ")} WHERE id = ?`,
            params,
          );
        }
      }

      const updatedClass = hasProjectScope
        ? (db
            .query("SELECT * FROM classes WHERE id = ? AND project_id = ?")
            .get(id, scopedProjectId) as any)
        : (db.query("SELECT * FROM classes WHERE id = ?").get(id) as any);
      if (!updatedClass)
        return new Response("Class not found", { status: 404 });
      let updatedClassTags = [];
      try {
        updatedClassTags = JSON.parse(updatedClass.tags || "[]");
      } catch {}
      return Response.json({
        id: updatedClass.id,
        name: updatedClass.name,
        label: updatedClass.name,
        description: updatedClass.description,
        parent: updatedClass.parent_id,
        project_id: updatedClass.project_id ?? null,
        color: updatedClass.color,
        image: updatedClass.image,
        sort_order: updatedClass.sort_order,
        tags: updatedClassTags,
      });
    } catch (e) {
      console.error(e);
      return new Response("Error updating class", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/classes/reorder" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const updates = Array.isArray(body?.updates) ? body.updates : [];
      if (!updates.length) {
        return new Response("Missing updates", { status: 400 });
      }

      const stmt = hasProjectScope
        ? db.prepare(
            "UPDATE classes SET parent_id = ?, sort_order = ?, project_id = COALESCE(project_id, ?) WHERE id = ? AND (project_id = ? OR project_id IS NULL)",
          )
        : db.prepare(
            "UPDATE classes SET parent_id = ?, sort_order = ? WHERE id = ?",
          );
      const txn = db.transaction((rows: any[]) => {
        for (const row of rows) {
          if (!row?.id) continue;
          const pid = row.parent_id ?? null;
          const orderVal =
            typeof row.sort_order === "number"
              ? row.sort_order
              : Number(row.sort_order);
          const sort = Number.isFinite(orderVal) ? orderVal : null;
          if (hasProjectScope)
            stmt.run(pid, sort, scopedProjectId, row.id, scopedProjectId);
          else stmt.run(pid, sort, row.id);
        }
      });
      txn(updates);
      return Response.json({ ok: true, updated: updates.length });
    } catch (e) {
      console.error(e);
      return new Response("Error reordering classes", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/classes" && method === "DELETE") {
    const id = url.searchParams.get("id");
    if (!id) return new Response("Missing id", { status: 400 });

    try {
      if (hasProjectScope) {
        db.run(
          "DELETE FROM classes WHERE id = ? AND (project_id = ? OR project_id IS NULL)",
          [id, scopedProjectId],
        );
      } else {
        db.run("DELETE FROM classes WHERE id = ?", [id]);
      }
      return Response.json({ ok: true });
    } catch (e) {
      console.error(e);
      return new Response("Error deleting class", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/properties" && method === "GET") {
    const props = db
      .query("SELECT * FROM properties")
      .all()
      .map((row: any) => ({
        id: row.id,
        name: row.name,
        label: row.name,
        datatype: row.datatype,
        valuetype: row.valuetype,
        types: parseTypes(row.types),
        description: row.description,
      }));
    return Response.json(props);
  }

  if (url.pathname === "/api/kb/property_create" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const name = body.name;
      const datatype = body.datatype || "string";
      const valuetype = body.valuetype || null;
      const types = parseTypes(body.types);

      if (!name) return new Response("Missing name", { status: 400 });

      const maxIdResult = db
        .query(
          "SELECT MAX(CAST(id AS INTEGER)) as maxId FROM properties WHERE id GLOB '[0-9]*'",
        )
        .get() as any;
      const nextId = (maxIdResult?.maxId || 0) + 1;
      const id = nextId.toString();

      db.run(
        "INSERT INTO properties (id, name, datatype, valuetype, types, description) VALUES (?, ?, ?, ?, ?, '')",
        [id, name, datatype, valuetype, JSON.stringify(types)],
      );

      const newProp = db
        .query("SELECT * FROM properties WHERE id = ?")
        .get(id) as any;
      return Response.json({
        id: newProp.id,
        name: newProp.name,
        label: newProp.name,
        datatype: newProp.datatype,
        valuetype: newProp.valuetype,
        types: parseTypes(newProp.types),
        description: newProp.description,
      });
    } catch (e) {
      console.error(e);
      return new Response("Error creating property", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/property_update" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const id = body.id;
      const name = body.name;
      const datatype = body.datatype;
      const valuetype = body.valuetype;
      const types = body.types;

      if (!id) return new Response("Missing id", { status: 400 });

      const updates = [];
      const params = [];

      if (name !== undefined) {
        updates.push("name = ?");
        params.push(name);
      }
      if (datatype !== undefined) {
        updates.push("datatype = ?");
        params.push(datatype);
      }
      if (valuetype !== undefined) {
        updates.push("valuetype = ?");
        params.push(valuetype);
      }
      if (types !== undefined) {
        updates.push("types = ?");
        params.push(JSON.stringify(parseTypes(types)));
      }

      if (updates.length > 0) {
        params.push(id);
        db.run(
          `UPDATE properties SET ${updates.join(", ")} WHERE id = ?`,
          params,
        );
      }

      return Response.json({ ok: true });
    } catch (e) {
      console.error(e);
      return new Response("Error updating property", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/property_delete" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const id = body.id;
      if (!id) return new Response("Missing id", { status: 400 });

      db.run("DELETE FROM properties WHERE id = ?", [id]);
      return Response.json({ ok: true });
    } catch (e) {
      console.error(e);
      return new Response("Error deleting property", { status: 500 });
    }
  }

  if (url.pathname === "/api/kb/property_search" && method === "GET") {
    const q = url.searchParams.get("q") || "";
    const classIdRaw = (url.searchParams.get("class_id") || "").trim();
    const typeNameRaw = (url.searchParams.get("type_name") || "").trim();
    const limit = parseInt(url.searchParams.get("limit") || "20");
    const offset = parseInt(url.searchParams.get("offset") || "0");

    const whereParts: string[] = ["(name LIKE ? OR id LIKE ?)"];
    const params: any[] = [`%${q}%`, `%${q}%`];
    const countParams: any[] = [`%${q}%`, `%${q}%`];

    if (classIdRaw) {
      whereParts.push(
        "id IN (SELECT property_id FROM class_properties WHERE class_id = ?)",
      );
      params.push(classIdRaw);
      countParams.push(classIdRaw);
    }

    if (typeNameRaw) {
      const normalizedType = typeNameRaw
        .toLowerCase()
        .replace(/["%_]/g, "")
        .trim();
      if (normalizedType) {
        whereParts.push(
          "(EXISTS (SELECT 1 FROM json_each(COALESCE(types, '[]')) jt WHERE LOWER(TRIM(CAST(jt.value AS TEXT))) = ?))",
        );
        params.push(normalizedType);
        countParams.push(normalizedType);
      }
    }

    const whereSql = whereParts.length
      ? `WHERE ${whereParts.join(" AND ")}`
      : "";

    const props = db
      .query(`SELECT * FROM properties ${whereSql} LIMIT ? OFFSET ?`)
      .all(...params, limit, offset)
      .map((row: any) => ({
        id: row.id,
        name: row.name,
        label: row.name,
        datatype: row.datatype,
        valuetype: row.valuetype,
        types: parseTypes(row.types),
        description: row.description,
      }));

    const total = db
      .query(`SELECT COUNT(*) as count FROM properties ${whereSql}`)
      .get(...countParams) as any;

    return Response.json({ items: props, total: total.count });
  }

  if (url.pathname === "/api/kb/subclass") {
    return Response.json([]);
  }

  if (url.pathname === "/api/kb/class/schema") {
    if (method === "POST") {
      try {
        const body = (await req.json()) as any;
        const classId = body.class_id;
        const propertyId = body.property_id;

        if (!classId || !propertyId) {
          return new Response("Missing class_id or property_id", {
            status: 400,
          });
        }

        db.run(
          "INSERT OR IGNORE INTO class_properties (class_id, property_id) VALUES (?, ?)",
          [classId, propertyId],
        );
        return Response.json({ ok: true });
      } catch (e) {
        console.error(e);
        return new Response("Error adding class schema", { status: 500 });
      }
    }

    if (method === "GET") {
      const classId = url.searchParams.get("class_id");
      if (!classId) return new Response("Missing class_id", { status: 400 });

      const props = db
        .query(
          `
            WITH RECURSIVE ancestors(id) AS (
                SELECT id FROM classes WHERE id = ?
                UNION ALL
                SELECT c.parent_id FROM classes c, ancestors a WHERE c.id = a.id AND c.parent_id IS NOT NULL
            )
            SELECT p.*, MAX(CASE WHEN cp.class_id = ? THEN 1 ELSE 0 END) as is_local
            FROM properties p
            JOIN class_properties cp ON p.id = cp.property_id
            WHERE cp.class_id IN (SELECT id FROM ancestors)
            GROUP BY p.id
          `,
        )
        .all(classId, classId)
        .map((row: any) => {
          const qualifiers = db
            .query(
              `
                SELECT p.* 
                FROM properties p
                JOIN property_properties pp ON p.id = pp.child_property_id
                WHERE pp.parent_property_id = ?
              `,
            )
            .all(row.id)
            .map((q: any) => ({
              id: q.id,
              name: q.name,
              label: q.name,
              datatype: q.datatype,
              valuetype: q.valuetype,
              types: parseTypes(q.types),
              description: q.description,
            }));

          return {
            id: row.id,
            name: row.name,
            label: row.name,
            datatype: row.datatype,
            valuetype: row.valuetype,
            types: parseTypes(row.types),
            description: row.description,
            is_local: row.is_local === 1,
            qualifiers,
          };
        });

      return Response.json({ items: props });
    }

    if (method === "DELETE") {
      const classId = url.searchParams.get("class_id");
      const propertyId = url.searchParams.get("property_id");

      if (!classId || !propertyId) {
        return new Response("Missing class_id or property_id", { status: 400 });
      }

      db.run(
        "DELETE FROM class_properties WHERE class_id = ? AND property_id = ?",
        [classId, propertyId],
      );
      return Response.json({ ok: true });
    }
  }

  if (url.pathname === "/api/kb/property/qualifier") {
    if (method === "POST") {
      try {
        const body = (await req.json()) as any;
        const parentId = body.parent_id;
        const childId = body.child_id;

        if (!parentId || !childId) {
          return new Response("Missing parent_id or child_id", { status: 400 });
        }

        db.run(
          "INSERT OR IGNORE INTO property_properties (parent_property_id, child_property_id) VALUES (?, ?)",
          [parentId, childId],
        );
        return Response.json({ ok: true });
      } catch (e) {
        console.error(e);
        return new Response("Error adding property qualifier", { status: 500 });
      }
    }

    if (method === "DELETE") {
      const parentId = url.searchParams.get("parent_id");
      const childId = url.searchParams.get("child_id");

      if (!parentId || !childId) {
        return new Response("Missing parent_id or child_id", { status: 400 });
      }

      db.run(
        "DELETE FROM property_properties WHERE parent_property_id = ? AND child_property_id = ?",
        [parentId, childId],
      );
      return Response.json({ ok: true });
    }
  }

  if (url.pathname === "/api/kb/entity/class") {
    if (method === "POST") {
      try {
        const body = (await req.json()) as any;
        const entityId = body.entity_id;
        const classId = body.class_id;

        if (!entityId || !classId) {
          return new Response("Missing entity_id or class_id", { status: 400 });
        }

        const dbEntityId = entityId.replace("entity/", "");
        db.run(
          "INSERT OR IGNORE INTO entity_classes (entity_id, class_id) VALUES (?, ?)",
          [dbEntityId, classId],
        );

        try {
          const attrs = db
            .query("SELECT key FROM attributes WHERE node_id = ?")
            .all(dbEntityId) as any[];
          for (const attr of attrs) {
            if (attr.key) {
              db.run(
                "INSERT OR IGNORE INTO class_properties (class_id, property_id) VALUES (?, ?)",
                [classId, attr.key],
              );
            }
          }
        } catch (err) {
          console.warn(
            "Failed to auto-associate attributes to class schema",
            err,
          );
        }

        return Response.json({ ok: true });
      } catch (e) {
        console.error(e);
        return new Response("Error setting entity class", { status: 500 });
      }
    }

    if (method === "DELETE") {
      const entityId = url.searchParams.get("entity_id");
      const classId = url.searchParams.get("class_id");
      if (!entityId) return new Response("Missing entity_id", { status: 400 });

      const dbEntityId = entityId.replace("entity/", "");
      if (classId) {
        db.run(
          "DELETE FROM entity_classes WHERE entity_id = ? AND class_id = ?",
          [dbEntityId, classId],
        );
      } else {
        db.run("DELETE FROM entity_classes WHERE entity_id = ?", [dbEntityId]);
      }
      return Response.json({ ok: true });
    }

    const entityId =
      url.searchParams.get("id") || url.searchParams.get("entity_id");
    if (!entityId) return new Response("Missing entity_id", { status: 400 });

    const dbEntityId = entityId.replace("entity/", "");
    const classes = db
      .query(
        `
          SELECT c.* FROM classes c
          JOIN entity_classes ec ON c.id = ec.class_id
          WHERE ec.entity_id = ?
        `,
      )
      .all(dbEntityId)
      .map((row: any) => ({
        id: row.id,
        name: row.name,
        label: row.name,
        description: row.description,
        color: row.color,
      }));

    return Response.json({ items: classes });
  }

  if (url.pathname.startsWith("/api/kb/clean/tasks")) {
    if (method === "GET") return Response.json([]);
    return Response.json({});
  }

  if (
    url.pathname === "/api/kb/property/value_suggestions" &&
    method === "GET"
  ) {
    const prop = url.searchParams.get("property") || "";
    const entityIdParam = url.searchParams.get("entity_id") || "";
    let limit = parseInt(url.searchParams.get("limit") || "20");
    if (limit < 1) limit = 1;
    if (limit > 100) limit = 100;

    if (!prop) {
      return Response.json({ items: [] });
    }

    const keysToCheck = new Set<string>();
    keysToCheck.add(prop);

    if (prop.startsWith("property/")) {
      keysToCheck.add(prop.substring(9));
    }

    Array.from(keysToCheck).forEach((k) => {
      if (/^\d+$/.test(k)) {
        keysToCheck.add("P" + k);
      } else if (/^P\d+$/.test(k)) {
        keysToCheck.add(k.substring(1));
      }
    });

    const propKeys = Array.from(keysToCheck);
    const propPlaceholders = propKeys.map((_, i) => `$p${i}`).join(",");

    let entityId = entityIdParam;
    if (entityId.startsWith("entity/")) {
      entityId = entityId.substring(7);
    }

    let classIds: string[] = [];
    if (entityId) {
      const classes = db
        .query("SELECT class_id FROM entity_classes WHERE entity_id = ?")
        .all(entityId) as any[];
      classIds = classes.map((c) => c.class_id);
    }

    let sql = "";
    const params: Record<string, any> = {};
    propKeys.forEach((k, i) => {
      params[`$p${i}`] = k;
    });
    params.$limit = limit;

    if (classIds.length > 0) {
      const classPlaceholders = classIds.map((_, i) => `$c${i}`).join(",");
      classIds.forEach((id, i) => {
        params[`$c${i}`] = id;
      });

      sql = `
        SELECT target_id as id, MAX(target_label) as label, COUNT(*) as count
        FROM (
            SELECT REPLACE(json_extract(a.value, '$.id'), 'entity/', '') as target_id, n.name as target_label
            FROM attributes a
            JOIN entity_classes ec ON REPLACE(a.node_id, 'entity/', '') = ec.entity_id
            LEFT JOIN nodes n ON REPLACE(json_extract(a.value, '$.id'), 'entity/', '') = n.id
            WHERE a.key IN (${propPlaceholders})
              AND a.datatype = 'wikibase-entityid'
              AND ec.class_id IN (${classPlaceholders})

            UNION ALL

            SELECT json_extract(a.value, '$') as target_id, json_extract(a.value, '$') as target_label
            FROM attributes a
            JOIN entity_classes ec ON REPLACE(a.node_id, 'entity/', '') = ec.entity_id
            WHERE a.key IN (${propPlaceholders})
              AND a.datatype = 'string'
              AND ec.class_id IN (${classPlaceholders})
        ) as combined
        WHERE target_id IS NOT NULL
        GROUP BY target_id
        ORDER BY count DESC
        LIMIT $limit
      `;
    } else {
      sql = `
        SELECT target_id as id, MAX(target_label) as label, COUNT(*) as count
        FROM (
            SELECT REPLACE(json_extract(value, '$.id'), 'entity/', '') as target_id, n.name as target_label
            FROM attributes a
            LEFT JOIN nodes n ON REPLACE(json_extract(a.value, '$.id'), 'entity/', '') = n.id
            WHERE key IN (${propPlaceholders})
              AND datatype = 'wikibase-entityid'

            UNION ALL

            SELECT json_extract(value, '$') as target_id, json_extract(value, '$') as target_label
            FROM attributes
            WHERE key IN (${propPlaceholders})
              AND datatype = 'string'
        ) as combined
        WHERE target_id IS NOT NULL
        GROUP BY target_id
        ORDER BY count DESC
        LIMIT $limit
      `;
    }

    const items = db.query(sql).all(params);
    return Response.json({ items });
  }

  // 分类图片上传 API
  if (url.pathname === "/api/kb/classes/upload-image" && method === "POST") {
    try {
      const formData = await req.formData();
      const file = formData.get("file") as File | null;
      if (!file) {
        return new Response("No file uploaded", { status: 400 });
      }

      // 生成唯一文件名
      const ext = file.name.split(".").pop()?.toLowerCase() || "png";
      const allowedExts = ["png", "jpg", "jpeg", "gif", "webp", "svg", "ico"];
      if (!allowedExts.includes(ext)) {
        return new Response("Invalid file type", { status: 400 });
      }
      const filename = `${crypto.randomUUID()}.${ext}`;
      const filePath = resolve(CLASS_IMAGES_DIR, filename);

      // 保存文件
      const arrayBuffer = await file.arrayBuffer();
      writeFileSync(filePath, Buffer.from(arrayBuffer));

      // 返回访问 URL
      const imageUrl = `/static/uploads/class-images/${filename}`;
      return Response.json({ url: imageUrl });
    } catch (e) {
      console.error(e);
      return new Response("Error uploading image", { status: 500 });
    }
  }

  return null;
}
