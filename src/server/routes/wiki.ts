import { db } from "../db.ts";
import { marked } from "marked";

marked.setOptions({
  gfm: true,
  breaks: false,
});

function normalizeMarkdownTables(md: string) {
  const source = String(md || "");
  if (!source) return "";
  const lines = source.split(/\r?\n/);
  const out: string[] = [];

  const normalizeTableLine = (line: string) =>
    String(line || "")
      .replace(/\uFF5C/g, "|")
      .replace(/[\uFF0D\u2014\u2013]/g, "-");

  const isTableLine = (line: string) => {
    const normalized = normalizeTableLine(line).trim();
    return normalized.includes("|");
  };

  const isDelimiterLine = (line: string) => {
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
    const current = lines[i] ?? "";
    const next = i + 1 < lines.length ? lines[i + 1] ?? "" : "";
    const isTableStart = isTableLine(current) && isDelimiterLine(next);
    if (isTableStart) {
      if (out.length && (out[out.length - 1] ?? "").trim() !== "") {
        out.push("");
      }
      out.push(normalizeTableLine(current));
      out.push(normalizeTableLine(next));
      i += 2;
      while (i < lines.length && isTableLine(lines[i] ?? "")) {
        out.push(normalizeTableLine(lines[i] ?? ""));
        i += 1;
      }
      if (out.length && (out[out.length - 1] ?? "").trim() !== "") {
        out.push("");
      }
      i -= 1;
      continue;
    }
    out.push(current);
  }

  return out.join("\n");
}

export async function handleWikiRoutes(
  req: Request,
  url: URL,
  method: string
) {
  if (url.pathname === "/api/wiki/page/save" && method === "POST") {
    try {
      const body = (await req.json()) as any;
      const entityId = body.entityId || body.entity_id;
      const content = body.md || body.content;

      if (!entityId) {
        return new Response("Missing entity_id", { status: 400 });
      }

      const dbId = entityId.replace("entity/", "");
      db.run("UPDATE nodes SET wiki_md = ? WHERE id = ?", [content, dbId]);

      return Response.json({ ok: true });
    } catch (e) {
      console.error(e);
      return new Response("Error saving wiki", { status: 500 });
    }
  }

  if (url.pathname === "/api/wiki/page" && method === "GET") {
    const entityId =
      url.searchParams.get("entityId") || url.searchParams.get("entity_id");
    if (!entityId) {
      return new Response("Missing entity_id", { status: 400 });
    }

    const dbId = entityId.replace("entity/", "");
    const node = db
      .query("SELECT wiki_md, description FROM nodes WHERE id = ?")
      .get(dbId) as any;

    const md = node?.wiki_md ?? node?.description ?? "";
    const normalizedMd = normalizeMarkdownTables(md);
    const html = await marked.parse(normalizedMd);

    return Response.json({
      page: {
        md,
        html,
      },
    });
  }

  if (url.pathname === "/api/wiki/page/revisions") {
    return Response.json([]);
  }

  if (url.pathname === "/api/wiki/backlinks") {
    return Response.json([]);
  }

  if (url.pathname === "/api/wiki/page/mentions") {
    return Response.json([]);
  }

  return null;
}
