import { Database } from "bun:sqlite";
import { copyFileSync, existsSync, mkdirSync, readdirSync, statSync } from "fs";
import { join, resolve } from "path";

const KNOWLEDGE_GRAPH_ROOT = resolve(import.meta.dir, "..", "..");
const WORKSPACE_ROOT = resolve(KNOWLEDGE_GRAPH_ROOT, "..");
const ROOT_DATA_DIR = resolve(WORKSPACE_ROOT, "data");
const ROOT_UPLOADS_DIR = resolve(WORKSPACE_ROOT, "uploads");
const APP_DB_FILENAME = "app.sqlite";
const APP_DB_PATH = resolve(ROOT_DATA_DIR, APP_DB_FILENAME);
const LEGACY_ADMIN_DB_PATH = resolve(ROOT_DATA_DIR, "admin.sqlite");
const LEGACY_KB_DB_PATH = resolve(ROOT_DATA_DIR, "kb.sqlite");
const LEGACY_ANY_STORE_DB_PATH = resolve(ROOT_DATA_DIR, "links.db");
const APP_UPLOADS_DIR = ROOT_UPLOADS_DIR;
const LEGACY_ANY_STORE_UPLOADS_DIR = resolve(
  WORKSPACE_ROOT,
  "any-store",
  "public",
  "uploads",
);
const LEGACY_KNOWLEDGE_GRAPH_UPLOADS_DIR = resolve(
  KNOWLEDGE_GRAPH_ROOT,
  "public",
  "uploads",
);
const OLD_SHARED_DB_PATH = resolve(WORKSPACE_ROOT, "shared.sqlite");
const OLD_APP_DB_PATH = resolve(ROOT_DATA_DIR, "shared.sqlite");
const OLD_SHARED_UPLOADS_DIR = resolve(WORKSPACE_ROOT, "shared_uploads");
const OLD_KNOWLEDGE_GRAPH_DATA_DIR = resolve(KNOWLEDGE_GRAPH_ROOT, "data");
const OLD_ANY_STORE_DATA_DIR = resolve(WORKSPACE_ROOT, "any-store", "data");

mkdirSync(ROOT_DATA_DIR, { recursive: true });
mkdirSync(APP_UPLOADS_DIR, { recursive: true });

if (!existsSync(APP_DB_PATH)) {
  if (existsSync(OLD_SHARED_DB_PATH)) {
    try {
      copyFileSync(OLD_SHARED_DB_PATH, APP_DB_PATH);
    } catch {}
  } else if (existsSync(OLD_APP_DB_PATH)) {
    try {
      copyFileSync(OLD_APP_DB_PATH, APP_DB_PATH);
    } catch {}
  }
}

const appDb = new Database(APP_DB_PATH);

export let adminDb: any = appDb;
export let db = appDb;

function runSafe(sql: string) {
  try {
    appDb.run(sql);
  } catch {}
}

function queryAllSafe(sql: string, ...params: any[]) {
  try {
    return appDb.query(sql).all(...params) as any[];
  } catch {
    return [];
  }
}

function queryGetSafe(sql: string, ...params: any[]) {
  try {
    return appDb.query(sql).get(...params) as any;
  } catch {
    return null;
  }
}

function slugifyProjectName(value: string, fallback: string) {
  const normalized = (value || "")
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || fallback;
}

function ensureUniqueProjectName(baseName: string) {
  let candidate = baseName;
  let attempt = 1;
  while (
    queryGetSafe("SELECT id FROM projects WHERE name = ? LIMIT 1", candidate)
  ) {
    attempt += 1;
    candidate = `${baseName}-${attempt}`;
  }
  return candidate;
}

function ensureSharedTables() {
  appDb.run(`
    CREATE TABLE IF NOT EXISTS projects (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE,
      file TEXT,
      title TEXT,
      description TEXT,
      image TEXT,
      theme_color TEXT DEFAULT '#ff7a2b',
      tags TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  runSafe("ALTER TABLE projects ADD COLUMN image TEXT");
  runSafe("ALTER TABLE projects ADD COLUMN theme_color TEXT DEFAULT '#ff7a2b'");
  runSafe("ALTER TABLE projects ADD COLUMN tags TEXT");
  runSafe("ALTER TABLE projects ADD COLUMN file TEXT");
  runSafe("ALTER TABLE projects ADD COLUMN title TEXT");
  runSafe(
    "UPDATE projects SET title = name WHERE (title IS NULL OR title = '') AND name IS NOT NULL AND name <> ''",
  );
  runSafe(
    `UPDATE projects SET file = '${APP_DB_FILENAME}' WHERE file IS NULL OR file = '' OR file = 'shared.sqlite'`,
  );

  appDb.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE NOT NULL,
      display_name TEXT,
      password TEXT,
      password_hash TEXT,
      password_salt TEXT,
      avatar TEXT,
      is_admin INTEGER DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  runSafe("ALTER TABLE users ADD COLUMN display_name TEXT");
  runSafe("ALTER TABLE users ADD COLUMN password TEXT");
  runSafe("ALTER TABLE users ADD COLUMN password_hash TEXT");
  runSafe("ALTER TABLE users ADD COLUMN password_salt TEXT");
  runSafe("ALTER TABLE users ADD COLUMN avatar TEXT");
  runSafe("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0");
  runSafe(
    "ALTER TABLE users ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP",
  );
  runSafe(
    "ALTER TABLE users ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP",
  );
  runSafe(
    "UPDATE users SET display_name = username WHERE display_name IS NULL OR display_name = ''",
  );

  appDb.run(`
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      token TEXT UNIQUE,
      username TEXT,
      user_id INTEGER,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      expires_at DATETIME
    )
  `);
  runSafe("ALTER TABLE sessions ADD COLUMN token TEXT");
  runSafe("ALTER TABLE sessions ADD COLUMN username TEXT");
  runSafe("ALTER TABLE sessions ADD COLUMN user_id INTEGER");
  runSafe(
    "ALTER TABLE sessions ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP",
  );
  runSafe("ALTER TABLE sessions ADD COLUMN expires_at DATETIME");
  runSafe("UPDATE sessions SET token = id WHERE token IS NULL OR token = ''");

  appDb.run(`
    CREATE TABLE IF NOT EXISTS links (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      url TEXT NOT NULL,
      image TEXT,
      tags TEXT,
      description TEXT,
      owner_id INTEGER,
      source TEXT,
      screenshots TEXT,
      short_description TEXT,
      first_comment TEXT,
      approved INTEGER DEFAULT 0,
      approved_by INTEGER,
      approved_at INTEGER,
      featured INTEGER DEFAULT 0,
      product_id INTEGER,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `);
  runSafe("ALTER TABLE links ADD COLUMN source TEXT");
  runSafe("ALTER TABLE links ADD COLUMN screenshots TEXT");
  runSafe("ALTER TABLE links ADD COLUMN short_description TEXT");
  runSafe("ALTER TABLE links ADD COLUMN first_comment TEXT");
  runSafe("ALTER TABLE links ADD COLUMN owner_id INTEGER");
  runSafe("ALTER TABLE links ADD COLUMN approved INTEGER DEFAULT 0");
  runSafe("ALTER TABLE links ADD COLUMN approved_by INTEGER");
  runSafe("ALTER TABLE links ADD COLUMN approved_at INTEGER");
  runSafe("ALTER TABLE links ADD COLUMN featured INTEGER DEFAULT 0");
  runSafe("ALTER TABLE links ADD COLUMN product_id INTEGER");

  appDb.run(`
    CREATE TABLE IF NOT EXISTS link_likes (
      link_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      PRIMARY KEY (link_id, user_id)
    )
  `);

  appDb.run(`
    CREATE TABLE IF NOT EXISTS comments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      link_id INTEGER NOT NULL,
      user_id INTEGER,
      username TEXT,
      content TEXT NOT NULL,
      parent_id INTEGER,
      created_at INTEGER NOT NULL
    )
  `);
  runSafe("ALTER TABLE comments ADD COLUMN parent_id INTEGER");

  appDb.run(`
    CREATE TABLE IF NOT EXISTS comment_likes (
      comment_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      PRIMARY KEY (comment_id, user_id)
    )
  `);

  appDb.run(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `);

  appDb.run(`
    CREATE TABLE IF NOT EXISTS nodes (
      id TEXT PRIMARY KEY,
      name TEXT,
      type TEXT,
      description TEXT,
      wiki_md TEXT,
      aliases TEXT,
      tags TEXT,
      data TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  runSafe("ALTER TABLE nodes ADD COLUMN wiki_md TEXT");
  runSafe("ALTER TABLE nodes ADD COLUMN project_id INTEGER");
  runSafe(
    "UPDATE nodes SET wiki_md = description WHERE (wiki_md IS NULL OR wiki_md = '') AND description IS NOT NULL AND description <> ''",
  );
  runSafe(
    "CREATE INDEX IF NOT EXISTS idx_nodes_project_id ON nodes(project_id)",
  );
  runSafe(
    "CREATE INDEX IF NOT EXISTS idx_attributes_node_id ON attributes(node_id)",
  );

  appDb.run(`
    CREATE TABLE IF NOT EXISTS attributes (
      id TEXT PRIMARY KEY,
      node_id TEXT,
      key TEXT,
      value TEXT,
      datatype TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE
    )
  `);

  appDb.run(`
    CREATE TABLE IF NOT EXISTS entity_classes (
      entity_id TEXT,
      class_id TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY(entity_id, class_id),
      FOREIGN KEY(entity_id) REFERENCES nodes(id) ON DELETE CASCADE,
      FOREIGN KEY(class_id) REFERENCES classes(id) ON DELETE CASCADE
    )
  `);

  appDb.run(`
    CREATE TABLE IF NOT EXISTS class_properties (
      class_id TEXT,
      property_id TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY(class_id, property_id),
      FOREIGN KEY(class_id) REFERENCES classes(id) ON DELETE CASCADE,
      FOREIGN KEY(property_id) REFERENCES properties(id) ON DELETE CASCADE
    )
  `);

  appDb.run(`
    CREATE TABLE IF NOT EXISTS property_properties (
      parent_property_id TEXT,
      child_property_id TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY(parent_property_id, child_property_id),
      FOREIGN KEY(parent_property_id) REFERENCES properties(id) ON DELETE CASCADE,
      FOREIGN KEY(child_property_id) REFERENCES properties(id) ON DELETE CASCADE
    )
  `);

  appDb.run(`
    CREATE TABLE IF NOT EXISTS properties (
      id TEXT PRIMARY KEY,
      name TEXT,
      datatype TEXT,
      valuetype TEXT,
      types TEXT,
      description TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  runSafe("ALTER TABLE properties ADD COLUMN valuetype TEXT");
  runSafe("ALTER TABLE properties ADD COLUMN types TEXT");
  runSafe(
    "UPDATE properties SET types = '[]' WHERE types IS NULL OR TRIM(types) = ''",
  );

  appDb.run(`
    CREATE TABLE IF NOT EXISTS classes (
      id TEXT PRIMARY KEY,
      name TEXT,
      description TEXT,
      parent_id TEXT,
      project_id INTEGER,
      color TEXT,
      sort_order INTEGER,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  runSafe("ALTER TABLE classes ADD COLUMN project_id INTEGER");
  runSafe("ALTER TABLE classes ADD COLUMN color TEXT");
  runSafe("ALTER TABLE classes ADD COLUMN image TEXT");
  runSafe("ALTER TABLE classes ADD COLUMN sort_order INTEGER");
  runSafe("ALTER TABLE classes ADD COLUMN tags TEXT");
  runSafe("UPDATE classes SET sort_order = rowid WHERE sort_order IS NULL");
}

function getProjectByIdentifier(
  identifier: string | number | null | undefined,
) {
  const raw = (identifier ?? "").toString().trim();
  if (!raw) return null;
  const byId = /^\d+$/.test(raw)
    ? queryGetSafe(
        "SELECT * FROM projects WHERE id = ? LIMIT 1",
        Number.parseInt(raw, 10),
      )
    : null;
  if (byId) return byId;
  return (
    queryGetSafe("SELECT * FROM projects WHERE name = ? LIMIT 1", raw) ||
    queryGetSafe(
      "SELECT * FROM projects WHERE lower(title) = lower(?) LIMIT 1",
      raw,
    )
  );
}

function rewriteImportedEntityReferences(
  value: any,
  idMap: Map<string, string>,
): any {
  if (Array.isArray(value)) {
    return value.map((item) => rewriteImportedEntityReferences(item, idMap));
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  const next = { ...value } as Record<string, any>;
  const candidates = ["id", "entity-id", "entity_id", "entityId"];
  for (const key of candidates) {
    const current = next[key];
    if (!current && current !== 0) continue;
    const raw = current.toString().trim();
    if (!raw) continue;
    const normalized = raw.startsWith("entity/") ? raw.slice(7) : raw;
    const mapped = idMap.get(normalized);
    if (!mapped) continue;
    next[key] = raw.startsWith("entity/") ? `entity/${mapped}` : mapped;
  }
  if (next.value && typeof next.value === "object") {
    next.value = rewriteImportedEntityReferences(next.value, idMap);
  }
  return next;
}

function importLegacyProjectKnowledge() {
  const projects = queryAllSafe(
    "SELECT id, name, title FROM projects WHERE name IS NOT NULL AND name <> ''",
  );
  for (const project of projects) {
    const projectId = Number(project.id || 0);
    const projectSlug = (project.name || "").toString().trim();
    if (!projectId || !projectSlug) continue;
    const existingCount = queryGetSafe(
      "SELECT COUNT(*) AS count FROM nodes WHERE project_id = ?",
      projectId,
    );
    if (Number(existingCount?.count || 0) > 0) continue;

    const legacyPath = resolve(ROOT_DATA_DIR, `${projectSlug}.sqlite`);
    if (!existsSync(legacyPath) || legacyPath === APP_DB_PATH) continue;

    let legacyDb: Database | null = null;
    try {
      legacyDb = new Database(legacyPath, { readonly: true });
      const legacyTables = legacyDb
        .query("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all() as any[];
      const hasNodes = legacyTables.some((row) => row?.name === "nodes");
      if (!hasNodes) continue;

      const legacyNodes = legacyDb.query("SELECT * FROM nodes").all() as any[];
      if (!legacyNodes.length) continue;

      const idMap = new Map<string, string>();
      for (const row of legacyNodes) {
        const oldId = (row.id ?? "").toString();
        if (!oldId) continue;
        idMap.set(oldId, `${projectSlug}:${oldId}`);
      }

      const legacyProperties = legacyTables.some(
        (row) => row?.name === "properties",
      )
        ? (legacyDb.query("SELECT * FROM properties").all() as any[])
        : [];
      for (const row of legacyProperties) {
        try {
          appDb.run(
            "INSERT OR IGNORE INTO properties (id, name, datatype, valuetype, types, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))",
            [
              row.id,
              row.name || row.id,
              row.datatype || "string",
              row.valuetype || null,
              row.types || "[]",
              row.description || "",
              row.created_at || null,
              row.updated_at || null,
            ],
          );
        } catch {}
      }

      const legacyClasses = legacyTables.some((row) => row?.name === "classes")
        ? (legacyDb.query("SELECT * FROM classes").all() as any[])
        : [];
      for (const row of legacyClasses) {
        try {
          appDb.run(
            "INSERT OR IGNORE INTO classes (id, name, description, parent_id, color, sort_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))",
            [
              row.id,
              row.name || row.id,
              row.description || "",
              row.parent_id || null,
              row.color || null,
              row.sort_order || null,
              row.created_at || null,
              row.updated_at || null,
            ],
          );
        } catch {}
      }

      for (const row of legacyNodes) {
        const mappedId = idMap.get((row.id ?? "").toString());
        if (!mappedId) continue;
        appDb.run(
          "INSERT OR IGNORE INTO nodes (id, name, type, description, wiki_md, aliases, tags, data, project_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))",
          [
            mappedId,
            row.name || mappedId,
            row.type || "entity",
            row.description || "",
            row.wiki_md || row.description || "",
            row.aliases || "[]",
            row.tags || "[]",
            row.data || "{}",
            projectId,
            row.created_at || null,
            row.updated_at || null,
          ],
        );
      }

      const legacyAttributes = legacyTables.some(
        (row) => row?.name === "attributes",
      )
        ? (legacyDb.query("SELECT * FROM attributes").all() as any[])
        : [];
      for (const row of legacyAttributes) {
        const mappedNodeId = idMap.get((row.node_id ?? "").toString());
        if (!mappedNodeId) continue;
        let nextValue = row.value;
        if ((row.datatype || "").trim() === "wikibase-entityid" && row.value) {
          try {
            nextValue = JSON.stringify(
              rewriteImportedEntityReferences(JSON.parse(row.value), idMap),
            );
          } catch {}
        }
        const attrId = `${projectSlug}:${row.id}`;
        appDb.run(
          "INSERT OR IGNORE INTO attributes (id, node_id, key, value, datatype, created_at) VALUES (?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))",
          [
            attrId,
            mappedNodeId,
            row.key,
            nextValue,
            row.datatype || "string",
            row.created_at || null,
          ],
        );
      }

      const legacyEntityClasses = legacyTables.some(
        (row) => row?.name === "entity_classes",
      )
        ? (legacyDb.query("SELECT * FROM entity_classes").all() as any[])
        : [];
      for (const row of legacyEntityClasses) {
        const mappedEntityId = idMap.get((row.entity_id ?? "").toString());
        if (!mappedEntityId) continue;
        appDb.run(
          "INSERT OR IGNORE INTO entity_classes (entity_id, class_id, created_at) VALUES (?, ?, COALESCE(?, CURRENT_TIMESTAMP))",
          [mappedEntityId, row.class_id, row.created_at || null],
        );
      }
    } catch (err) {
      console.warn("importLegacyProjectKnowledge failed", projectSlug, err);
    } finally {
      try {
        legacyDb?.close();
      } catch {}
    }
  }
}

function migrateLegacyAnyStore() {
  if (!existsSync(LEGACY_ANY_STORE_DB_PATH)) return;

  const legacyAny = new Database(LEGACY_ANY_STORE_DB_PATH, { readonly: true });
  const userIdMap = new Map<number, number>();
  const projectIdMap = new Map<number, number>();

  try {
    const users = legacyAny
      .query(
        "SELECT id, username, password, avatar, is_admin, created_at FROM users",
      )
      .all() as any[];
    for (const row of users) {
      const username = (row.username || "").toString().trim().toLowerCase();
      if (!username) continue;
      const existing = queryGetSafe(
        "SELECT id FROM users WHERE username = ? LIMIT 1",
        username,
      );
      if (existing?.id) {
        userIdMap.set(Number(row.id), Number(existing.id));
        appDb.run(
          "UPDATE users SET avatar = COALESCE(NULLIF(avatar, ''), ?), is_admin = MAX(COALESCE(is_admin, 0), ?), password = COALESCE(password, ?), password_salt = COALESCE(password_salt, ?), password_hash = COALESCE(password_hash, ?), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
          [
            row.avatar || "",
            row.is_admin ? 1 : 0,
            row.password || "",
            String(row.password || "").split("$")[0] || "",
            String(row.password || "").split("$")[1] || "",
            existing.id,
          ],
        );
        continue;
      }

      const password = (row.password || "").toString();
      const [salt, hash] = password.includes("$")
        ? password.split("$", 2)
        : ["", ""];
      appDb.run(
        "INSERT INTO users (id, username, display_name, password, password_hash, password_salt, avatar, is_admin, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          row.id,
          username,
          username,
          password,
          hash || "",
          salt || "",
          row.avatar || "",
          row.is_admin ? 1 : 0,
          row.created_at || Date.now(),
          row.created_at || Date.now(),
        ],
      );
      userIdMap.set(Number(row.id), Number(row.id));
    }

    const products = legacyAny
      .query(
        "SELECT id, name, description, logo, theme_color, tags, created_at, updated_at FROM products",
      )
      .all() as any[];
    for (const row of products) {
      const title = (row.name || "").toString().trim();
      if (!title) continue;

      const existing =
        queryGetSafe(
          "SELECT id FROM projects WHERE title = ? LIMIT 1",
          title,
        ) ||
        queryGetSafe("SELECT id FROM projects WHERE name = ? LIMIT 1", title);
      if (existing?.id) {
        projectIdMap.set(Number(row.id), Number(existing.id));
        appDb.run(
          `UPDATE projects SET description = COALESCE(NULLIF(description, ''), ?), image = COALESCE(NULLIF(image, ''), ?), theme_color = COALESCE(theme_color, ?), tags = COALESCE(tags, ?), file = COALESCE(NULLIF(file, ''), '${APP_DB_FILENAME}'), updated_at = COALESCE(updated_at, ?) WHERE id = ?`,
          [
            row.description || "",
            row.logo || "",
            row.theme_color || "#ff7a2b",
            row.tags || "[]",
            row.updated_at || Date.now(),
            existing.id,
          ],
        );
        continue;
      }

      const baseName = slugifyProjectName(title, `product-${row.id}`);
      const projectName = ensureUniqueProjectName(baseName);
      appDb.run(
        "INSERT INTO projects (id, name, file, title, description, image, theme_color, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          row.id,
          projectName,
          APP_DB_FILENAME,
          title,
          row.description || "",
          row.logo || "",
          row.theme_color || "#ff7a2b",
          row.tags || "[]",
          row.created_at || Date.now(),
          row.updated_at || Date.now(),
        ],
      );
      projectIdMap.set(Number(row.id), Number(row.id));
    }

    const links = legacyAny.query("SELECT * FROM links").all() as any[];
    for (const row of links) {
      appDb.run(
        "INSERT OR IGNORE INTO links (id, name, url, image, tags, description, owner_id, source, screenshots, short_description, first_comment, approved, approved_by, approved_at, featured, product_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          row.id,
          row.name,
          row.url,
          row.image || "",
          row.tags || "[]",
          row.description || "",
          row.owner_id ? userIdMap.get(Number(row.owner_id)) || null : null,
          row.source || "",
          row.screenshots || "[]",
          row.short_description || "",
          row.first_comment || "",
          row.approved ? 1 : 0,
          row.approved_by
            ? userIdMap.get(Number(row.approved_by)) || null
            : null,
          row.approved_at || null,
          row.featured ? 1 : 0,
          row.product_id
            ? projectIdMap.get(Number(row.product_id)) || null
            : null,
          row.created_at || Date.now(),
          row.updated_at || Date.now(),
        ],
      );
    }

    const comments = legacyAny.query("SELECT * FROM comments").all() as any[];
    for (const row of comments) {
      appDb.run(
        "INSERT OR IGNORE INTO comments (id, link_id, user_id, username, content, parent_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
          row.id,
          row.link_id,
          row.user_id ? userIdMap.get(Number(row.user_id)) || null : null,
          row.username || "",
          row.content || "",
          row.parent_id || null,
          row.created_at || Date.now(),
        ],
      );
    }

    const linkLikes = legacyAny
      .query("SELECT link_id, user_id FROM link_likes")
      .all() as any[];
    for (const row of linkLikes) {
      const userId = userIdMap.get(Number(row.user_id));
      if (!userId) continue;
      appDb.run(
        "INSERT OR IGNORE INTO link_likes (link_id, user_id) VALUES (?, ?)",
        [row.link_id, userId],
      );
    }

    const commentLikes = legacyAny
      .query("SELECT comment_id, user_id FROM comment_likes")
      .all() as any[];
    for (const row of commentLikes) {
      const userId = userIdMap.get(Number(row.user_id));
      if (!userId) continue;
      appDb.run(
        "INSERT OR IGNORE INTO comment_likes (comment_id, user_id) VALUES (?, ?)",
        [row.comment_id, userId],
      );
    }

    const sessions = legacyAny
      .query("SELECT id, user_id, expires_at FROM sessions")
      .all() as any[];
    for (const row of sessions) {
      const userId = userIdMap.get(Number(row.user_id));
      if (!userId) continue;
      const user = queryGetSafe(
        "SELECT username FROM users WHERE id = ? LIMIT 1",
        userId,
      );
      appDb.run(
        "INSERT OR IGNORE INTO sessions (id, token, username, user_id, created_at, expires_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)",
        [row.id, row.id, user?.username || null, userId, row.expires_at],
      );
    }

    const settingsRows = legacyAny
      .query("SELECT key, value FROM settings")
      .all() as any[];
    for (const row of settingsRows) {
      if (row.key === "currentProduct") {
        try {
          const currentProduct = JSON.parse(row.value || "null");
          if (currentProduct?.id) {
            const mappedId = projectIdMap.get(Number(currentProduct.id));
            if (mappedId) {
              appDb.run(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                [
                  row.key,
                  JSON.stringify({
                    ...currentProduct,
                    id: mappedId,
                  }),
                ],
              );
              continue;
            }
          }
        } catch {}
      }
      appDb.run("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", [
        row.key,
        row.value,
      ]);
    }
  } catch (error) {
    console.warn("Failed to migrate legacy any-store data:", error);
  } finally {
    legacyAny.close();
  }
}

function migrateLegacyAdminDatabase() {
  if (!existsSync(LEGACY_ADMIN_DB_PATH)) return;

  const legacyAdmin = new Database(LEGACY_ADMIN_DB_PATH, { readonly: true });
  try {
    const projects = legacyAdmin
      .query(
        "SELECT name, file, title, description, image, created_at, updated_at FROM projects",
      )
      .all() as any[];
    for (const row of projects) {
      const name = (row.name || "").toString().trim();
      if (!name) continue;
      appDb.run(
        "INSERT OR IGNORE INTO projects (name, file, title, description, image, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
          name,
          row.file === "shared.sqlite"
            ? APP_DB_FILENAME
            : row.file || APP_DB_FILENAME,
          row.title || name,
          row.description || "",
          row.image || "",
          row.created_at || new Date().toISOString(),
          row.updated_at || new Date().toISOString(),
        ],
      );
    }

    const users = legacyAdmin
      .query(
        "SELECT username, display_name, password_hash, password_salt, avatar, created_at, updated_at FROM users",
      )
      .all() as any[];
    for (const row of users) {
      const username = (row.username || "").toString().trim().toLowerCase();
      if (!username) continue;
      appDb.run(
        "INSERT OR IGNORE INTO users (username, display_name, password, password_hash, password_salt, avatar, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
          username,
          row.display_name || username,
          row.password_salt && row.password_hash
            ? `${row.password_salt}$${row.password_hash}`
            : null,
          row.password_hash || "",
          row.password_salt || "",
          row.avatar || "",
          row.created_at || new Date().toISOString(),
          row.updated_at || new Date().toISOString(),
        ],
      );
    }

    const sessions = legacyAdmin
      .query("SELECT token, username, created_at, expires_at FROM sessions")
      .all() as any[];
    for (const row of sessions) {
      const user = queryGetSafe(
        "SELECT id FROM users WHERE username = ? LIMIT 1",
        row.username,
      );
      appDb.run(
        "INSERT OR IGNORE INTO sessions (id, token, username, user_id, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
        [
          row.token,
          row.token,
          row.username,
          user?.id || null,
          row.created_at || new Date().toISOString(),
          row.expires_at || null,
        ],
      );
    }
  } catch (error) {
    console.warn("Failed to migrate legacy admin data:", error);
  } finally {
    legacyAdmin.close();
  }
}

function migrateLegacyKnowledgeGraphDatabase() {
  if (!existsSync(LEGACY_KB_DB_PATH)) return;

  const legacyKb = new Database(LEGACY_KB_DB_PATH, { readonly: true });
  try {
    const tableCopies = [
      {
        name: "nodes",
        columns:
          "id, name, type, description, wiki_md, aliases, tags, data, created_at, updated_at",
      },
      {
        name: "attributes",
        columns: "id, node_id, key, value, datatype, created_at",
      },
      {
        name: "entity_classes",
        columns: "entity_id, class_id, created_at",
      },
      {
        name: "class_properties",
        columns: "class_id, property_id, created_at",
      },
      {
        name: "property_properties",
        columns: "parent_property_id, child_property_id, created_at",
      },
      {
        name: "properties",
        columns:
          "id, name, datatype, valuetype, description, created_at, updated_at",
      },
      {
        name: "classes",
        columns:
          "id, name, description, parent_id, color, sort_order, created_at, updated_at",
      },
    ];

    for (const table of tableCopies) {
      const rows = legacyKb
        .query(`SELECT ${table.columns} FROM ${table.name}`)
        .all();
      for (const row of rows as any[]) {
        const values = table.columns.split(",").map((column) => {
          const key = column.trim();
          return row[key];
        });
        appDb.run(
          `INSERT OR IGNORE INTO ${table.name} (${table.columns}) VALUES (${table.columns
            .split(",")
            .map(() => "?")
            .join(", ")})`,
          values,
        );
      }
    }
  } catch (error) {
    console.warn("Failed to migrate legacy knowledge graph data:", error);
  } finally {
    legacyKb.close();
  }
}

function migrateLegacyDatabases() {
  const migrated = queryGetSafe(
    "SELECT value FROM settings WHERE key = ? LIMIT 1",
    "__shared_data_migrated_v1__",
  );
  if (migrated?.value === "1") return;

  migrateLegacyAnyStore();
  migrateLegacyAdminDatabase();
  migrateLegacyKnowledgeGraphDatabase();

  appDb.run("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", [
    "__shared_data_migrated_v1__",
    "1",
  ]);
}

function copyDirectoryIntoSharedUploads(sourceDir: string, relativeDir = "") {
  if (!existsSync(sourceDir)) return;
  const entries = readdirSync(sourceDir, { withFileTypes: true });
  for (const entry of entries) {
    const nextRelative = relativeDir
      ? join(relativeDir, entry.name)
      : entry.name;
    const sourcePath = resolve(sourceDir, entry.name);
    const targetPath = resolve(APP_UPLOADS_DIR, nextRelative);
    if (entry.isDirectory()) {
      mkdirSync(targetPath, { recursive: true });
      copyDirectoryIntoSharedUploads(sourcePath, nextRelative);
      continue;
    }
    try {
      const sourceStat = statSync(sourcePath);
      if (!sourceStat.isFile()) continue;
      if (!existsSync(resolve(targetPath, ".."))) {
        mkdirSync(resolve(targetPath, ".."), { recursive: true });
      }
      if (!existsSync(targetPath)) {
        copyFileSync(sourcePath, targetPath);
      }
    } catch {}
  }
}

function copyDirectoryIntoRootData(sourceDir: string, relativeDir = "") {
  if (!existsSync(sourceDir)) return;
  const entries = readdirSync(sourceDir, { withFileTypes: true });
  for (const entry of entries) {
    const nextRelative = relativeDir
      ? join(relativeDir, entry.name)
      : entry.name;
    const sourcePath = resolve(sourceDir, entry.name);
    const targetPath = resolve(ROOT_DATA_DIR, nextRelative);
    if (entry.isDirectory()) {
      mkdirSync(targetPath, { recursive: true });
      copyDirectoryIntoRootData(sourcePath, nextRelative);
      continue;
    }
    try {
      const sourceStat = statSync(sourcePath);
      if (!sourceStat.isFile()) continue;
      if (!existsSync(resolve(targetPath, ".."))) {
        mkdirSync(resolve(targetPath, ".."), { recursive: true });
      }
      if (!existsSync(targetPath)) {
        copyFileSync(sourcePath, targetPath);
      }
    } catch {}
  }
}

function migrateLegacyDataFilesToRoot() {
  if (existsSync(OLD_SHARED_DB_PATH) && !existsSync(APP_DB_PATH)) {
    try {
      copyFileSync(OLD_SHARED_DB_PATH, APP_DB_PATH);
    } catch {}
  }
  if (existsSync(OLD_APP_DB_PATH) && !existsSync(APP_DB_PATH)) {
    try {
      copyFileSync(OLD_APP_DB_PATH, APP_DB_PATH);
    } catch {}
  }
  copyDirectoryIntoRootData(OLD_KNOWLEDGE_GRAPH_DATA_DIR);
  copyDirectoryIntoRootData(OLD_ANY_STORE_DATA_DIR);
}

function migrateLegacyUploads() {
  copyDirectoryIntoSharedUploads(OLD_SHARED_UPLOADS_DIR);
  copyDirectoryIntoSharedUploads(LEGACY_ANY_STORE_UPLOADS_DIR);
  copyDirectoryIntoSharedUploads(LEGACY_KNOWLEDGE_GRAPH_UPLOADS_DIR);
}

function cleanupPlaceholderProjects() {
  try {
    const sharedProject = queryGetSafe(
      "SELECT id FROM projects WHERE name = 'shared' LIMIT 1",
    );
    if (sharedProject?.id) {
      const sharedNodeIds = queryAllSafe(
        "SELECT id FROM nodes WHERE project_id = ?",
        sharedProject.id,
      ).map((row) => row.id);
      if (sharedNodeIds.length) {
        const placeholders = sharedNodeIds.map(() => "?").join(",");
        appDb.run(
          `DELETE FROM attributes WHERE node_id IN (${placeholders})`,
          sharedNodeIds,
        );
        appDb.run(
          `DELETE FROM entity_classes WHERE entity_id IN (${placeholders})`,
          sharedNodeIds,
        );
        appDb.run(
          `DELETE FROM nodes WHERE id IN (${placeholders})`,
          sharedNodeIds,
        );
      }
      appDb.run("DELETE FROM projects WHERE id = ?", [sharedProject.id]);
    }
    appDb.run(
      `DELETE FROM projects
       WHERE name = 'shared'
         AND (title IS NULL OR title = '' OR title = 'shared')
         AND (description IS NULL OR description = '')
         AND (image IS NULL OR image = '')
         AND NOT EXISTS (SELECT 1 FROM links WHERE product_id = projects.id LIMIT 1)`,
    );
    const orphanSharedNodeIds = queryAllSafe(
      `SELECT id
       FROM nodes
       WHERE id LIKE 'shared:%'
         OR (project_id IS NOT NULL AND project_id NOT IN (SELECT id FROM projects))`,
    ).map((row) => row.id);
    if (orphanSharedNodeIds.length) {
      const placeholders = orphanSharedNodeIds.map(() => "?").join(",");
      appDb.run(
        `DELETE FROM attributes WHERE node_id IN (${placeholders})`,
        orphanSharedNodeIds,
      );
      appDb.run(
        `DELETE FROM entity_classes WHERE entity_id IN (${placeholders})`,
        orphanSharedNodeIds,
      );
      appDb.run(
        `DELETE FROM nodes WHERE id IN (${placeholders})`,
        orphanSharedNodeIds,
      );
    }
    appDb.run(
      `UPDATE projects
       SET file = '${APP_DB_FILENAME}'`,
    );
  } catch {}
}

export async function hashPassword(password: string, salt?: string) {
  try {
    const s = salt || crypto.randomUUID().slice(0, 8);
    const enc = new TextEncoder();
    const data = enc.encode(s + password);
    const buf = await (crypto as any).subtle.digest("SHA-256", data);
    const arr = Array.from(new Uint8Array(buf));
    const hex = arr.map((b) => b.toString(16).padStart(2, "0")).join("");
    return { salt: s, hash: hex };
  } catch {
    try {
      const c = require("crypto");
      const s = salt || c.randomBytes(4).toString("hex");
      const h = c
        .createHash("sha256")
        .update(s + password)
        .digest("hex");
      return { salt: s, hash: h };
    } catch {
      return { salt: salt || "", hash: "" };
    }
  }
}

export function ensureTables() {
  ensureSharedTables();
}

export function initializeKnowledgeBaseDatabase() {
  migrateLegacyDataFilesToRoot();
  ensureSharedTables();
  migrateLegacyDatabases();
  importLegacyProjectKnowledge();
  migrateLegacyUploads();
  cleanupPlaceholderProjects();
}

export function switchDatabase(_filename: string) {
  db = appDb;
  adminDb = appDb;
  ensureSharedTables();
}

export { APP_DB_FILENAME, getProjectByIdentifier };

initializeKnowledgeBaseDatabase();
