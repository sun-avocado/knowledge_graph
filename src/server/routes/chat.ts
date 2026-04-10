import { db } from "../db.ts";
import { formatNode } from "../utils.ts";

const CHAT_MODEL_URL =
  process.env.CHAT_MODEL_URL || "http://10.117.1.238:8104/brief_or_profound";
const CHAT_MODEL_TIMEOUT = parseInt(process.env.CHAT_MODEL_TIMEOUT || "200000");
const CHAT_MODEL_PING_TIMEOUT = parseInt(
  process.env.CHAT_MODEL_PING_TIMEOUT || "5000",
);
const LANGGRAPH_TOP_K = parseInt(process.env.LANGGRAPH_TOP_K || "5");

function getSessions(): Map<string, any> {
  (globalThis as any)._kb_chat_sessions =
    (globalThis as any)._kb_chat_sessions || new Map();
  return (globalThis as any)._kb_chat_sessions;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function tokenizeQuery(text: string): string[] {
  if (!text || typeof text !== "string") return [];
  const trimmed = text.trim();
  if (!trimmed) return [];
  const parts = trimmed.split(/[\s,，;；、\.。\?\!！？\n\t]+/);
  const out: string[] = [];
  const seen = new Set<string>();
  for (const p of parts) {
    const s = (p || "").trim();
    if (!s) continue;
    if (!seen.has(s)) {
      seen.add(s);
      out.push(s);
    }
  }
  if (!seen.has(trimmed)) out.push(trimmed);
  return out.slice(0, 32);
}

function pickBestTextFromRow(row: any): string {
  try {
    const label = (row.name || row.label || "").toString().trim();
    const desc = (row.description || row.desc || "").toString().trim();
    let parts: string[] = [];
    if (label) parts.push(label);
    if (desc) parts.push(desc);
    try {
      const tagsRaw = row.tags || row.tag || "";
      let tags = [] as any[];
      if (typeof tagsRaw === "string") {
        try {
          tags = JSON.parse(tagsRaw);
        } catch (e) {
          tags = [];
        }
      } else if (Array.isArray(tagsRaw)) tags = tagsRaw;
      if (tags && tags.length) {
        parts.push("标签：" + tags.filter(Boolean).slice(0, 6).join("、"));
      }
    } catch (e) {}
    return parts.length
      ? parts.join(" ｜ ")
      : `Document ${row.id || row._id || row._key || "unknown"}`;
  } catch (e) {
    return String(row.id || row._id || row._key || "unknown");
  }
}

function sqlLikeWildcard(s: string) {
  return `%${(s || "").replace(/%/g, "")}%`;
}

function retrieve_sqlite(message: string, topK = 6) {
  const q = (message || "").toString().trim();
  const docs: any[] = [];
  try {
    if (q) {
      try {
        const rows = db
          .query(
            `SELECT id, name, description, aliases, tags FROM nodes WHERE name LIKE ? OR description LIKE ? OR aliases LIKE ? LIMIT ?`,
          )
          .all(
            sqlLikeWildcard(q),
            sqlLikeWildcard(q),
            sqlLikeWildcard(q),
            topK,
          ) as any[];
        for (const r of rows)
          docs.push({
            _key: r.id,
            id: r.id,
            label_zh: r.name,
            desc_zh: r.description,
            aliases_zh: [],
            tags: r.tags
              ? typeof r.tags === "string"
                ? (() => {
                    try {
                      return JSON.parse(r.tags);
                    } catch (e) {
                      return [];
                    }
                  })()
                : r.tags
              : [],
          });
      } catch (e) {}
    }

    if (docs.length < Math.min(3, topK)) {
      const tokens = tokenizeQuery(q);
      if (tokens.length) {
        const clauses: string[] = [];
        const params: any[] = [];
        for (const t of tokens) {
          clauses.push("name LIKE ?");
          params.push(sqlLikeWildcard(t));
          clauses.push("description LIKE ?");
          params.push(sqlLikeWildcard(t));
          clauses.push("aliases LIKE ?");
          params.push(sqlLikeWildcard(t));
        }
        const where = clauses.length ? `WHERE (${clauses.join(" OR ")})` : "";
        const sql = `SELECT id, name, description, aliases, tags FROM nodes ${where} LIMIT 200`;
        try {
          const rows = db.query(sql).all(...(params as any)) as any[];
          const scored = [] as any[];
          for (const r of rows) {
            let score = 0;
            const name = (r.name || "").toString();
            const desc = (r.description || "").toString();
            const aliases = (r.aliases || "").toString();
            for (const t of tokens) {
              if (name.includes(t)) score += 3;
              if (desc.includes(t)) score += 1;
              if (aliases.includes(t)) score += 1;
            }
            if (score > 0) scored.push({ row: r, score });
          }
          scored.sort((a, b) => b.score - a.score);
          for (const s of scored) {
            const r = s.row;
            if (docs.find((d) => d._key === r.id)) continue;
            docs.push({
              _key: r.id,
              id: r.id,
              label_zh: r.name,
              desc_zh: r.description,
              aliases_zh: [],
              tags: r.tags
                ? typeof r.tags === "string"
                  ? (() => {
                      try {
                        return JSON.parse(r.tags);
                      } catch (e) {
                        return [];
                      }
                    })()
                  : r.tags
                : [],
              _score: s.score,
            });
            if (docs.length >= topK) break;
          }
        } catch (e) {}
      }
    }
  } catch (e) {
    console.warn("retrieve_sqlite failed", e);
  }
  const evidences: string[] = [];
  for (const d of docs.slice(0, topK)) {
    evidences.push(pickBestTextFromRow(d));
  }
  return { docs: docs.slice(0, topK), evidences };
}

function detectDemandIntent(message: string) {
  const text = String(message || "").trim();
  if (!text) return "general";
  if (/[推荐|适合|怎么学|路线|方案]/.test(text)) return "recommend";
  if (/[比较|区别|对比]/.test(text)) return "compare";
  if (/[关系|关联|影响]/.test(text)) return "relation";
  return "general";
}

function dedupeTerms(terms: string[], max = 8) {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const term of terms) {
    const value = String(term || "").trim();
    if (!value) continue;
    if (seen.has(value)) continue;
    seen.add(value);
    out.push(value);
    if (out.length >= max) break;
  }
  return out;
}

function decomposeDemandGraph(message: string) {
  const text = String(message || "").trim();
  const tokens = tokenizeQuery(text).filter(
    (token) => token && token.length >= 2,
  );
  const intent = detectDemandIntent(text);
  const relationLexicon = [
    "推荐",
    "适合",
    "学习路径",
    "比较",
    "区别",
    "关系",
    "关联",
    "应用",
    "能力",
    "课程",
    "方案",
  ];
  const relationTerms = dedupeTerms(
    relationLexicon.filter((item) => text.includes(item)),
    5,
  );
  const stopwords = new Set([
    "什么",
    "哪些",
    "怎么",
    "如何",
    "需要",
    "相关",
    "推荐",
    "适合",
    "一个",
    "一些",
    "请问",
    "帮我",
  ]);
  const entityTerms = dedupeTerms(
    tokens.filter((token) => !stopwords.has(token)),
    6,
  );
  const edges: any[] = [];
  if (entityTerms.length >= 2) {
    const relation =
      relationTerms[0] || (intent === "compare" ? "比较" : "关联");
    for (let i = 1; i < entityTerms.length; i++) {
      edges.push({
        source: entityTerms[0],
        relation,
        target: entityTerms[i],
      });
    }
  } else if (entityTerms.length === 1 && relationTerms.length) {
    edges.push({
      source: entityTerms[0],
      relation: relationTerms[0],
      target: "知识推荐",
    });
  }
  return {
    query: text,
    intent,
    entities: entityTerms.map((name) => ({ name })),
    relations: relationTerms.map((name) => ({ name })),
    edges,
  };
}

function searchPropertyCandidates(term: string, limit = 5) {
  const q = String(term || "").trim();
  if (!q) return [];
  try {
    const rows = db
      .query(
        `SELECT id, name, description FROM properties
         WHERE name LIKE ? OR description LIKE ? LIMIT ?`,
      )
      .all(sqlLikeWildcard(q), sqlLikeWildcard(q), limit) as any[];
    return rows.map((row) => ({
      id: row.id,
      name: row.name || row.id,
      description: row.description || "",
    }));
  } catch (e) {
    return [];
  }
}

function searchClassCandidates(term: string, limit = 5) {
  const q = String(term || "").trim();
  if (!q) return [];
  try {
    const rows = db
      .query(
        `SELECT id, name, description FROM classes
         WHERE name LIKE ? OR description LIKE ? LIMIT ?`,
      )
      .all(sqlLikeWildcard(q), sqlLikeWildcard(q), limit) as any[];
    return rows.map((row) => ({
      id: row.id,
      name: row.name || row.id,
      description: row.description || "",
    }));
  } catch (e) {
    return [];
  }
}

function alignDemandToKnowledge(plan: any, topK = 5) {
  const entityAlignments = (plan?.entities || []).map((entity: any) => {
    const term = String(entity?.name || "").trim();
    const retrieved = retrieve_sqlite(term, topK).docs || [];
    return {
      term,
      matches: retrieved.map((doc: any) => ({
        id: doc._key || doc.id,
        label: doc.label_zh || doc.id,
        description: doc.desc_zh || "",
        score: Number(doc._score || 0),
      })),
    };
  });
  const relationAlignments = (plan?.relations || []).map((relation: any) => {
    const term = String(relation?.name || "").trim();
    return {
      term,
      properties: searchPropertyCandidates(term, topK),
      classes: searchClassCandidates(term, topK),
    };
  });
  const matchedNodeIds = Array.from(
    new Set(
      entityAlignments.flatMap((item: any) =>
        (item.matches || [])
          .slice(0, 3)
          .map((match: any) => String(match.id || "")),
      ),
    ),
  ).filter(Boolean);
  return {
    entities: entityAlignments,
    relations: relationAlignments,
    matched_node_ids: matchedNodeIds,
  };
}

function buildRecommendationReason(
  row: any,
  sourceNames: string[],
  edgeLabels: string[],
) {
  const parts: string[] = [];
  if (sourceNames.length) {
    parts.push(`与 ${sourceNames.slice(0, 2).join("、")} 直接相关`);
  }
  if (edgeLabels.length) {
    parts.push(
      `命中关系 ${Array.from(new Set(edgeLabels)).slice(0, 3).join(" / ")}`,
    );
  }
  const desc = String(row?.description || "").trim();
  if (desc) parts.push(desc.slice(0, 60));
  return parts.join("；") || "与当前需求语义接近";
}

function recommendKnowledgeFromAlignment(
  message: string,
  alignment: any,
  topK = 6,
) {
  const matchedNodeIds = Array.isArray(alignment?.matched_node_ids)
    ? alignment.matched_node_ids.slice(0, 8)
    : [];
  const recommendMap = new Map<string, any>();
  const graphNodes = new Map<string, any>();
  const graphEdges = new Map<string, any>();

  const ensureGraphNode = (id: string, label: string, extra: any = {}) => {
    if (!id) return;
    graphNodes.set(id, { data: { id, label: label || id, ...extra } });
  };

  const pushRecommendation = (row: any, meta: any = {}) => {
    if (!row?.id) return;
    const key = String(row.id);
    const existing = recommendMap.get(key) || {
      id: key,
      title: row.name || key,
      description: row.description || "",
      score: 0,
      sourceNames: [] as string[],
      edgeLabels: [] as string[],
    };
    existing.score += Number(meta.score || 0);
    if (meta.sourceName) existing.sourceNames.push(String(meta.sourceName));
    if (meta.edgeLabel) existing.edgeLabels.push(String(meta.edgeLabel));
    existing.reason = buildRecommendationReason(
      row,
      existing.sourceNames,
      existing.edgeLabels,
    );
    recommendMap.set(key, existing);
  };

  try {
    for (const nodeId of matchedNodeIds) {
      const sourceRaw = db
        .query("SELECT * FROM nodes WHERE id = ?")
        .get(nodeId) as any;
      const sourceRow = sourceRaw ? formatNode(sourceRaw) : null;
      if (!sourceRow) continue;
      ensureGraphNode(
        String(sourceRow.id),
        String(sourceRow.label_zh || sourceRow.name || sourceRow.id),
        {
          root: true,
          kind: "aligned",
          image: sourceRow.image || "",
          color: sourceRow.color || "",
        },
      );
      pushRecommendation(sourceRow, {
        score: 8,
        sourceName: sourceRow.label_zh || sourceRow.name || sourceRow.id,
        edgeLabel: "对齐实体",
      });

      const outAttrs = db
        .query(
          "SELECT key, value FROM attributes WHERE node_id = ? AND datatype = ? LIMIT 48",
        )
        .all(nodeId, "wikibase-entityid") as any[];
      for (const attr of outAttrs) {
        let values: any[] = [];
        try {
          const parsed = JSON.parse(attr.value || "[]");
          values = Array.isArray(parsed) ? parsed : parsed ? [parsed] : [];
        } catch (e) {
          values = [];
        }
        for (const raw of values) {
          let targetId =
            raw?.id ||
            raw?.["entity-id"] ||
            raw?.entity_id ||
            raw?.target ||
            raw?.value?.id ||
            null;
          if (!targetId) continue;
          targetId = String(targetId).replace(/^entity\//, "");
          const targetRaw = db
            .query("SELECT * FROM nodes WHERE id = ?")
            .get(targetId) as any;
          const targetRow = targetRaw ? formatNode(targetRaw) : null;
          if (!targetRow) continue;
          ensureGraphNode(
            String(targetRow.id),
            String(targetRow.label_zh || targetRow.name || targetRow.id),
            {
              kind: "recommended",
              image: targetRow.image || "",
              color: targetRow.color || "",
            },
          );
          const sourceLabel = String(
            sourceRow.label_zh || sourceRow.name || sourceRow.id,
          );
          const edgeId = `${nodeId}->${targetRow.id}:${attr.key || "related"}`;
          graphEdges.set(edgeId, {
            data: {
              id: edgeId,
              source: String(sourceRow.id),
              target: String(targetRow.id),
              label: String(attr.key || "关联"),
            },
          });
          pushRecommendation(targetRow, {
            score: 5,
            sourceName: sourceLabel,
            edgeLabel: attr.key || "关联",
          });
        }
      }

      const reverseAttrs = db
        .query(
          "SELECT node_id, key, value FROM attributes WHERE datatype = ? LIMIT 200",
        )
        .all("wikibase-entityid") as any[];
      for (const attr of reverseAttrs) {
        let values: any[] = [];
        try {
          const parsed = JSON.parse(attr.value || "[]");
          values = Array.isArray(parsed) ? parsed : parsed ? [parsed] : [];
        } catch (e) {
          values = [];
        }
        const hasTarget = values.some((raw: any) => {
          const candidate =
            raw?.id ||
            raw?.["entity-id"] ||
            raw?.entity_id ||
            raw?.target ||
            raw?.value?.id ||
            null;
          return (
            String(candidate || "").replace(/^entity\//, "") === String(nodeId)
          );
        });
        if (!hasTarget) continue;
        const source2Raw = db
          .query("SELECT * FROM nodes WHERE id = ?")
          .get(attr.node_id) as any;
        const source2 = source2Raw ? formatNode(source2Raw) : null;
        if (!source2) continue;
        ensureGraphNode(
          String(source2.id),
          String(source2.label_zh || source2.name || source2.id),
          {
            kind: "recommended",
            image: source2.image || "",
            color: source2.color || "",
          },
        );
        const edgeId = `${source2.id}->${nodeId}:${attr.key || "关联"}`;
        graphEdges.set(edgeId, {
          data: {
            id: edgeId,
            source: String(source2.id),
            target: String(nodeId),
            label: String(attr.key || "关联"),
          },
        });
        pushRecommendation(source2, {
          score: 4,
          sourceName: sourceRow.label_zh || sourceRow.name || sourceRow.id,
          edgeLabel: `反向:${attr.key || "关联"}`,
        });
      }
    }
  } catch (e) {
    console.warn("recommendKnowledgeFromAlignment failed", e);
  }

  if (!recommendMap.size) {
    const fallback = retrieve_sqlite(message, topK).docs || [];
    for (const doc of fallback) {
      pushRecommendation(
        {
          id: doc._key || doc.id,
          name: doc.label_zh || doc.id,
          description: doc.desc_zh || "",
        },
        { score: Number(doc._score || 3), edgeLabel: "语义检索" },
      );
      ensureGraphNode(
        String(doc._key || doc.id),
        String(doc.label_zh || doc.id),
        { kind: "recommended", image: doc.image || "", color: doc.color || "" },
      );
    }
  }

  const items = Array.from(recommendMap.values())
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, topK)
    .map((item, index) => ({ ...item, rank: index + 1 }));
  const keepIds = new Set(items.map((item) => String(item.id)));
  matchedNodeIds.forEach((id: string) => keepIds.add(String(id)));
  const nodes = Array.from(graphNodes.values()).filter((node: any) =>
    keepIds.has(String(node?.data?.id || "")),
  );
  const edges = Array.from(graphEdges.values()).filter((edge: any) => {
    const source = String(edge?.data?.source || "");
    const target = String(edge?.data?.target || "");
    return keepIds.has(source) && keepIds.has(target);
  });
  const evidenceDocs = items.map((item) => ({
    id: item.id,
    _key: item.id,
    label_zh: item.title,
    desc_zh: item.reason || item.description || "",
    _score: item.score,
  }));
  const evidences = items.map(
    (item) =>
      `${item.title} ｜ ${item.reason || item.description || "知识推荐结果"}`,
  );
  return {
    items,
    graph_elements: { nodes, edges },
    evidence_docs: evidenceDocs,
    evidences,
  };
}

function buildAlgorithmRecommendationReply(
  message: string,
  plan: any,
  alignment: any,
  recommendations: any,
) {
  const entityNames = (plan?.entities || [])
    .map((item: any) => String(item?.name || "").trim())
    .filter(Boolean);
  const relationNames = (plan?.relations || [])
    .map((item: any) => String(item?.name || "").trim())
    .filter(Boolean);
  const alignedEntityLines = (alignment?.entities || [])
    .map((item: any) => {
      const term = String(item?.term || "").trim();
      const matches = (item?.matches || [])
        .slice(0, 3)
        .map((match: any) => String(match?.label || match?.id || "").trim())
        .filter(Boolean);
      if (!term) return "";
      return matches.length
        ? `- ${term} 对齐到：${matches.join("、")}`
        : `- ${term} 暂未对齐到明确节点`;
    })
    .filter(Boolean);
  const items = Array.isArray(recommendations?.items)
    ? recommendations.items.slice(0, 5)
    : [];

  const introParts: string[] = [];
  if (entityNames.length)
    introParts.push(`围绕 ${entityNames.join("、")} 展开`);
  if (relationNames.length)
    introParts.push(`重点关注 ${relationNames.join("、")} 相关知识`);
  const intro = introParts.length
    ? `我已根据你的需求 ${introParts.join("，")}，并结合知识图谱中的已有关联生成推荐。`
    : `我已根据你的需求，结合知识图谱中的结构化关联生成推荐。`;

  const strategy = alignedEntityLines.length
    ? `本次推荐先完成需求拆解与知识对齐，再沿图谱关系扩展候选知识。`
    : `本次推荐主要依据需求关键词与图谱邻接关系进行候选扩展。`;

  const recommendationLines = items.length
    ? items.map(
        (item: any, index: number) =>
          `${index + 1}. ${item.title || item.id || "未命名知识"}：${item.reason || item.description || "与当前需求相关"} [${index + 1}]`,
      )
    : [`1. 暂未找到足够稳定的推荐结果，建议换一个更具体的需求描述继续查询。`];

  const alignmentBlock = alignedEntityLines.length
    ? `\n\n知识对齐：\n${alignedEntityLines.join("\n")}`
    : "";

  return `${intro}\n${strategy}\n\n推荐结果：\n${recommendationLines.join("\n")}${alignmentBlock}\n\n如果你愿意，我还可以继续把这些推荐结果整理成学习路径、课程清单或能力图谱。`;
}

async function callExternalModel(
  messages: any[],
  isConcise = true,
  replyDirect = false,
) {
  let modelReply: string | null = null;
  let modelError: string | null = null;
  try {
    const controller = new AbortController();
    const to = setTimeout(
      () => controller.abort(),
      CHAT_MODEL_TIMEOUT || 20000,
    );
    try {
      const headers: any = { "Content-Type": "application/json" };
      if (process.env.CHAT_MODEL_AUTH)
        headers["Authorization"] = process.env.CHAT_MODEL_AUTH;
      const resp = await fetch(CHAT_MODEL_URL, {
        method: "POST",
        headers,
        body: JSON.stringify({
          messages,
          is_concise: isConcise,
          reply_direct: replyDirect,
        }),
        signal: controller.signal,
      });
      if (resp.ok) {
        try {
          const j: any = await resp.json().catch(() => null);
          if (j && typeof j.reply === "string") modelReply = j.reply;
          else if (j && typeof j.data === "string") modelReply = j.data;
          else if (typeof j === "string") modelReply = j;
          else {
            const t = await resp.text().catch(() => "");
            modelReply = t || null;
          }
        } catch (e) {
          const t = await resp.text().catch(() => "");
          modelReply = t || null;
        }
      } else {
        const t = await resp.text().catch(() => "");
        modelError = `HTTP ${resp.status}${t ? ": " + t.slice(0, 1000) : ""}`;
      }
    } finally {
      clearTimeout(to);
    }
  } catch (err) {
    modelError =
      err && (err as any).message ? (err as any).message : String(err);
    modelReply = null;
  }
  return { reply: modelReply, error: modelError };
}

function selectBestDocByReply(docs: any[], reply: string) {
  if (!docs || !docs.length) return null;
  try {
    if (reply && typeof reply === "string") {
      const matches = Array.from(reply.matchAll(/\[(\d+)\]/g))
        .map((m) => parseInt(m[1] || "0", 10))
        .filter((n) => Number.isFinite(n) && n >= 1 && n <= docs.length);
      if (matches.length) {
        const freq: any = {};
        for (const n of matches) freq[n] = (freq[n] || 0) + 1;
        const vals = Object.values(freq).map((v) => Number(v) || 0);
        const maxCount = vals.length ? Math.max(...vals) : 0;
        const candidates = Object.keys(freq)
          .filter((k) => freq[k] === maxCount)
          .map((k) => parseInt(k, 10));
        const chosenIdx = Math.min(...candidates) - 1;
        return docs[chosenIdx];
      }
    }
  } catch (e) {}
  try {
    return docs.reduce((a, b) => ((b._score || 0) > (a._score || 0) ? b : a));
  } catch (e) {
    return docs[0];
  }
}

function buildGraphForSelectedSqlite(sel: any) {
  if (!sel || !sel._key) return { nodes: [], edges: [] };
  const key = sel._key;
  const centerId = key;
  const nodes: any[] = [];
  const edges: any[] = [];
  try {
    const centerRow = db
      .query("SELECT * FROM nodes WHERE id = ?")
      .get(centerId) as any;
    if (centerRow) {
      const centerNode = formatNode(centerRow);
      nodes.push({
        data: {
          id: centerNode.id,
          label: centerNode.label_zh || centerNode.label || centerNode.name,
          root: true,
          image: centerNode.image || "",
          color: centerNode.color || "",
        },
      });
    }
    const attrs = db
      .query("SELECT * FROM attributes WHERE node_id = ? AND datatype = ?")
      .all(centerId, "wikibase-entityid") as any[];
    const targets = new Set<string>();
    for (const a of attrs) {
      try {
        const vals = JSON.parse(a.value || "null");
        const list = Array.isArray(vals) ? vals : vals ? [vals] : [];
        for (const v of list) {
          const tidRaw =
            v && (v.id || v["entity-id"] || v.target)
              ? v.id || v["entity-id"] || v.target
              : null;
          if (tidRaw) {
            let tid = tidRaw;
            if (typeof tid === "string" && tid.startsWith("entity/"))
              tid = tid.replace("entity/", "");
            if (!tid) continue;
            targets.add(tid);
            edges.push({
              data: {
                id: `${a.id}:${tid}`,
                source: centerId,
                target: tid,
                label: a.key || a.property || "",
              },
            });
          }
        }
      } catch (e) {}
    }
    if (targets.size) {
      const placeholders = Array.from(targets)
        .map(() => "?")
        .join(",");
      const rows = db
        .query(`SELECT * FROM nodes WHERE id IN (${placeholders})`)
        .all(...(Array.from(targets) as any)) as any[];
      for (const r of rows) {
        const node = formatNode(r);
        nodes.push({
          data: {
            id: node.id,
            label: node.label_zh || node.label || node.name,
            image: node.image || "",
            color: node.color || "",
          },
        });
      }
    }
  } catch (e) {
    console.warn("buildGraphForSelectedSqlite failed", e);
  }
  return { nodes, edges };
}

// ── Route handler ─────────────────────────────────────────────────────────────

export async function handleChatRoutes(
  req: Request,
  url: URL,
  method: string,
): Promise<Response | null> {
  const _sessions = getSessions();

  // POST /sessions
  if (url.pathname === "/sessions" && method === "POST") {
    try {
      const body: any = await req.json().catch(() => ({}));
      const name =
        body && body.name
          ? String(body.name).toString()
          : `会话 ${_sessions.size + 1}`;
      const sid = crypto.randomUUID();
      const now = Date.now();
      _sessions.set(sid, {
        id: sid,
        name,
        messages: [],
        created_at: now,
        updated_at: now,
      });
      return Response.json({ ok: true, id: sid, name });
    } catch (e) {
      return Response.json({ ok: false }, { status: 500 });
    }
  }

  // GET /sessions
  if (url.pathname === "/sessions" && method === "GET") {
    try {
      const items: any[] = [];
      for (const [k, v] of _sessions) {
        items.push({ id: k, name: v.name, updated_at: v.updated_at });
      }
      return Response.json({ ok: true, items });
    } catch (e) {
      return Response.json({ ok: false }, { status: 500 });
    }
  }

  // GET /api/ai/ping
  if (url.pathname === "/api/ai/ping" && method === "GET") {
    try {
      const controller = new AbortController();
      const to = setTimeout(
        () => controller.abort(),
        CHAT_MODEL_PING_TIMEOUT || 5000,
      );
      const start = Date.now();
      try {
        const resp = await fetch(CHAT_MODEL_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: [{ role: "user", content: "ping" }],
            is_concise: true,
            reply_direct: true,
          }),
          signal: controller.signal,
        });
        const duration = Date.now() - start;
        let bodyText = "";
        try {
          bodyText = await resp.text();
        } catch (e) {
          bodyText = String(e);
        }
        clearTimeout(to);
        return Response.json({
          ok: true,
          model_url: CHAT_MODEL_URL,
          reachable: true,
          status: resp.status,
          body: (bodyText || "").slice(0, 1000),
          time_ms: duration,
        });
      } catch (err) {
        clearTimeout(to);
        const message =
          err && (err as any).message ? (err as any).message : String(err);
        console.warn("CHAT_MODEL ping failed", CHAT_MODEL_URL, message);
        return Response.json(
          {
            ok: false,
            model_url: CHAT_MODEL_URL,
            reachable: false,
            error: message,
          },
          { status: 502 },
        );
      }
    } catch (e) {
      return Response.json(
        { ok: false, error: "ping_failed" },
        { status: 500 },
      );
    }
  }

  // POST /api/ai/forward
  if (url.pathname === "/api/ai/forward" && method === "POST") {
    try {
      let payload: any = null;
      try {
        payload = await req.json().catch(() => null);
      } catch (e) {
        try {
          payload = await req.text();
        } catch (e) {
          payload = null;
        }
      }
      const controller = new AbortController();
      const to = setTimeout(
        () => controller.abort(),
        CHAT_MODEL_TIMEOUT || 200000,
      );
      try {
        const resp = await fetch(CHAT_MODEL_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body:
            typeof payload === "string"
              ? payload
              : JSON.stringify(payload || {}),
          signal: controller.signal,
        });
        const status = resp.status;
        const headers: Record<string, string> = {};
        resp.headers.forEach((v, k) => {
          headers[k] = v;
        });
        const ctype = resp.headers.get("content-type") || "";
        let body: any = null;
        if (ctype.includes("application/json")) {
          body = await resp.json().catch(() => null);
        } else {
          body = await resp.text().catch(() => null);
        }
        clearTimeout(to);
        return Response.json({ ok: true, status, headers, body });
      } catch (err) {
        clearTimeout(to);
        const message =
          err && (err as any).message ? (err as any).message : String(err);
        console.warn("CHAT_MODEL forward failed", CHAT_MODEL_URL, message);
        return Response.json({ ok: false, error: message }, { status: 502 });
      }
    } catch (e) {
      return Response.json(
        { ok: false, error: "invalid_payload" },
        { status: 400 },
      );
    }
  }

  // GET /sessions/:id
  if (
    url.pathname.startsWith("/sessions/") &&
    method === "GET" &&
    !url.pathname.endsWith("/rename") &&
    !url.pathname.endsWith("/delete")
  ) {
    try {
      const sid = url.pathname.slice("/sessions/".length);
      const s = _sessions.get(sid);
      if (!s)
        return Response.json(
          { ok: false, message: "not_found" },
          { status: 404 },
        );
      return Response.json({ ok: true, session: s });
    } catch (e) {
      return Response.json({ ok: false }, { status: 500 });
    }
  }

  // POST /sessions/:id/rename
  if (
    url.pathname.startsWith("/sessions/") &&
    url.pathname.endsWith("/rename") &&
    method === "POST"
  ) {
    try {
      const sid = url.pathname.slice("/sessions/".length, -"/rename".length);
      const body: any = await req.json().catch(() => ({}));
      const name = body && body.name ? String(body.name) : null;
      const s = _sessions.get(sid);
      if (!s)
        return Response.json(
          { ok: false, message: "not_found" },
          { status: 404 },
        );
      if (name) {
        s.name = name;
        s.updated_at = Date.now();
        _sessions.set(sid, s);
      }
      return Response.json({ ok: true });
    } catch (e) {
      return Response.json({ ok: false }, { status: 500 });
    }
  }

  // POST /sessions/:id/delete
  if (
    url.pathname.startsWith("/sessions/") &&
    url.pathname.endsWith("/delete") &&
    method === "POST"
  ) {
    try {
      const sid = url.pathname.slice("/sessions/".length, -"/delete".length);
      _sessions.delete(sid);
      return Response.json({ ok: true });
    } catch (e) {
      return Response.json({ ok: false }, { status: 500 });
    }
  }

  // POST /langgraph
  if (url.pathname === "/langgraph" && method === "POST") {
    try {
      const body: any = await req.json().catch(() => ({}));
      const message = body && body.message ? String(body.message).trim() : "";
      const sessionId =
        body && (body.session_id || body.sessionId)
          ? String(body.session_id || body.sessionId)
          : crypto.randomUUID();
      if (!message)
        return Response.json(
          { ok: false, error: "empty_message" },
          { status: 400 },
        );

      const sess = _sessions.get(sessionId) || {
        id: sessionId,
        name: `会话 ${_sessions.size + 1}`,
        messages: [],
        created_at: Date.now(),
        updated_at: Date.now(),
      };
      sess.messages = sess.messages || [];
      sess.messages.push({ role: "user", text: message, ts: Date.now() });

      const demandPlan = decomposeDemandGraph(message);
      const alignment = alignDemandToKnowledge(demandPlan, LANGGRAPH_TOP_K);
      const recommendations = recommendKnowledgeFromAlignment(
        message,
        alignment,
        Math.max(LANGGRAPH_TOP_K, 6),
      );
      const docs = recommendations.evidence_docs || [];
      const evidences = recommendations.evidences || [];

      const finalReply = buildAlgorithmRecommendationReply(
        message,
        demandPlan,
        alignment,
        recommendations,
      );
      const refs = evidences.length
        ? "\n\n参考证据:\n" +
          evidences.map((e, i) => `[${i + 1}] ${e}`).join("\n")
        : "\n\n参考证据: (无)";
      const final_output = finalReply + refs;

      sess.messages.push({
        role: "assistant",
        text: final_output,
        ts: Date.now(),
        evidences: evidences,
      });
      sess.updated_at = Date.now();
      _sessions.set(sessionId, sess);

      const selected_card =
        (recommendations.items && recommendations.items[0]
          ? {
              _key: recommendations.items[0].id,
              id: recommendations.items[0].id,
              label_zh: recommendations.items[0].title,
              desc_zh:
                recommendations.items[0].reason ||
                recommendations.items[0].description ||
                "",
            }
          : selectBestDocByReply(docs, finalReply)) || null;
      const graph_elements =
        recommendations.graph_elements ||
        (selected_card
          ? buildGraphForSelectedSqlite(selected_card)
          : { nodes: [], edges: [] });

      return Response.json({
        session_id: sessionId,
        reply: final_output,
        evidences,
        evidence_docs: docs,
        decomposition: demandPlan,
        alignment,
        recommendations: recommendations.items || [],
        selected_card,
        graph_elements,
      });
    } catch (e) {
      console.warn("langgraph failed", e);
      return Response.json(
        { ok: false, error: "server_error" },
        { status: 500 },
      );
    }
  }

  // GET /langgraph/stream
  if (url.pathname === "/langgraph/stream" && method === "GET") {
    try {
      const message = (url.searchParams.get("message") || "").trim();
      const sessionId =
        url.searchParams.get("session_id") ||
        url.searchParams.get("sessionId") ||
        crypto.randomUUID();
      if (!message) return new Response("message required", { status: 400 });

      const sess = _sessions.get(sessionId) || {
        id: sessionId,
        name: `会话 ${_sessions.size + 1}`,
        messages: [],
        created_at: Date.now(),
        updated_at: Date.now(),
      };
      sess.messages = sess.messages || [];
      sess.messages.push({ role: "user", text: message, ts: Date.now() });

      const encoder = new TextEncoder();
      const stream = new ReadableStream({
        async start(controller) {
          const send = (s: string) => controller.enqueue(encoder.encode(s));
          const sse = (eventName: string, data: any) =>
            `${eventName ? `event: ${eventName}\n` : ""}data: ${JSON.stringify(data)}\n\n`;
          const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
          try {
            send(
              sse("stage", {
                key: "bootstrap",
                label: "初始化",
                status: "running",
              }),
            );
            send(sse("progress", { pct: 2 }));
            send(
              sse("delta", {
                text: "正在连接知识推荐智能体，开始分析你的需求...\n\n",
              }),
            );

            const demandPlan = decomposeDemandGraph(message);
            const alignment = alignDemandToKnowledge(
              demandPlan,
              LANGGRAPH_TOP_K,
            );
            const recommendations = recommendKnowledgeFromAlignment(
              message,
              alignment,
              Math.max(LANGGRAPH_TOP_K, 6),
            );
            const docs = recommendations.evidence_docs || [];
            const finalReply = buildAlgorithmRecommendationReply(
              message,
              demandPlan,
              alignment,
              recommendations,
            );

            send(
              sse("stage", {
                key: "bootstrap",
                label: "初始化",
                status: "completed",
              }),
            );
            send(
              sse("stage", {
                key: "decompose",
                label: "需求拆解",
                status: "running",
              }),
            );
            send(sse("progress", { pct: 8 }));
            await sleep(420);
            send(sse("decomposition", demandPlan));
            await sleep(240);
            send(
              sse("stage", {
                key: "decompose",
                label: "需求拆解",
                status: "completed",
              }),
            );
            send(
              sse("stage", {
                key: "align",
                label: "知识对齐",
                status: "running",
              }),
            );
            send(sse("progress", { pct: 25 }));
            await sleep(460);
            send(sse("alignment", alignment));
            await sleep(260);
            send(
              sse("stage", {
                key: "align",
                label: "知识对齐",
                status: "completed",
              }),
            );
            send(
              sse("stage", {
                key: "recommend",
                label: "图谱推荐",
                status: "running",
              }),
            );
            send(sse("progress", { pct: 42 }));
            await sleep(520);
            send(
              sse("recommendations", { items: recommendations.items || [] }),
            );
            if (recommendations.graph_elements) {
              send(sse("graph", { items: recommendations.graph_elements }));
            }
            await sleep(220);
            const parts = finalReply.match(/.{1,40}/g) || [finalReply];
            for (let i = 0; i < parts.length; i++) {
              send(sse("delta", { text: parts[i] }));
              const pct = Math.min(
                95,
                10 + Math.round((i / parts.length) * 80),
              );
              send(sse("progress", { pct }));
              await sleep(90 + Math.random() * 140);
            }
            if (docs && docs.length) send(sse("evidences", { docs }));
            const selected =
              (recommendations.items && recommendations.items[0]
                ? {
                    _key: recommendations.items[0].id,
                    id: recommendations.items[0].id,
                    label_zh: recommendations.items[0].title,
                    desc_zh:
                      recommendations.items[0].reason ||
                      recommendations.items[0].description ||
                      "",
                  }
                : selectBestDocByReply(docs, finalReply)) || null;
            if (selected) {
              send(
                sse("card", {
                  item: {
                    title:
                      selected.label_zh || selected.title || selected.id || "",
                    id: selected._key || selected.id,
                  },
                }),
              );
            }
            send(
              sse("stage", {
                key: "recommend",
                label: "图谱推荐",
                status: "completed",
              }),
            );
            send(sse("progress", { pct: 100 }));
            await sleep(180);
            send(
              sse("done", {
                reply: finalReply,
                decomposition: demandPlan,
                alignment,
                recommendations: recommendations.items || [],
                graph_elements: recommendations.graph_elements || {
                  nodes: [],
                  edges: [],
                },
              }),
            );
            try {
              sess.messages.push({
                role: "assistant",
                text: finalReply,
                ts: Date.now(),
                evidences: docs || [],
                decomposition: demandPlan,
                alignment,
                recommendations: recommendations.items || [],
                graph: recommendations.graph_elements || {
                  nodes: [],
                  edges: [],
                },
              });
              sess.updated_at = Date.now();
              _sessions.set(sessionId, sess);
            } catch (e) {}
          } catch (err) {
            try {
              send(
                sse("error", {
                  error:
                    err && (err as any).message
                      ? (err as any).message
                      : String(err),
                }),
              );
            } catch (e) {}
          } finally {
            try {
              controller.close();
            } catch (e) {}
          }
        },
      });

      return new Response(stream, {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          Connection: "keep-alive",
          "X-Accel-Buffering": "no",
        },
      });
    } catch (e) {
      console.warn("langgraph stream failed", e);
      return Response.json({ ok: false }, { status: 500 });
    }
  }

  // POST /chat
  if (url.pathname === "/chat" && method === "POST") {
    try {
      const body: any = await req.json().catch(() => ({}));
      const message = body && body.message ? String(body.message).trim() : "";
      const sessionId =
        body && (body.session_id || body.sessionId)
          ? String(body.session_id || body.sessionId)
          : crypto.randomUUID();
      if (!message)
        return Response.json(
          { ok: false, error: "empty_message" },
          { status: 400 },
        );
      try {
        const lgUrl = new URL("/langgraph", url.origin);
        const forwardResp = await fetch(lgUrl.toString(), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...body, message, session_id: sessionId }),
        });
        const j = await forwardResp.json().catch(() => null);
        return Response.json(j || { ok: false }, {
          status: forwardResp.status,
        });
      } catch (e) {
        console.warn("/chat -> /langgraph forward failed", e);
        return Response.json(
          { ok: false, error: "forward_failed" },
          { status: 502 },
        );
      }
    } catch (e) {
      return Response.json(
        { ok: false, error: "server_error" },
        { status: 500 },
      );
    }
  }

  // GET /chat/stream
  if (url.pathname === "/chat/stream" && method === "GET") {
    try {
      const message = (url.searchParams.get("message") || "").trim();
      const sessionId =
        url.searchParams.get("session_id") ||
        url.searchParams.get("sessionId") ||
        crypto.randomUUID();
      if (!message) return new Response("message required", { status: 400 });
      try {
        const lgUrl = new URL("/langgraph/stream", url.origin);
        lgUrl.searchParams.set("message", message);
        lgUrl.searchParams.set("session_id", sessionId);
        const isC =
          url.searchParams.get("is_concise") ||
          url.searchParams.get("isConcise");
        if (isC) lgUrl.searchParams.set("is_concise", String(isC));
        const rd =
          url.searchParams.get("reply_direct") ||
          url.searchParams.get("replyDirect");
        if (rd) lgUrl.searchParams.set("reply_direct", String(rd));
        const innerResp = await fetch(lgUrl.toString(), {
          headers: req.headers,
        });
        return new Response(innerResp.body, {
          status: innerResp.status,
          headers: {
            "Content-Type":
              innerResp.headers.get("content-type") || "text/event-stream",
            "Cache-Control": "no-cache",
            Connection: "keep-alive",
            "X-Accel-Buffering": "no",
          },
        });
      } catch (e) {
        console.warn("/chat/stream -> /langgraph/stream forward failed", e);
        return Response.json(
          { ok: false, error: "forward_failed" },
          { status: 502 },
        );
      }
    } catch (e) {
      return Response.json({ ok: false }, { status: 500 });
    }
  }

  return null;
}
