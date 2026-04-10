import { switchDatabase } from "./src/server/db.ts";
import { handleCoreKbRoutes } from "./src/server/routes/core-kb.ts";
import { handleAuthRoutes } from "./src/server/routes/auth.ts";
import { handleProjectRoutes } from "./src/server/routes/projects.ts";
import { handleSchemaRoutes } from "./src/server/routes/schema.ts";
import { serveStaticRoute } from "./src/server/static.ts";
import { handleWikiRoutes } from "./src/server/routes/wiki.ts";
import { handleChatRoutes } from "./src/server/routes/chat.ts";

const port = parseInt(process.env.PORT || "5200");

const server = Bun.serve({
  port: port,
  idleTimeout: 255,
  async fetch(req) {
    const method = req.method;
    let url: URL | null = null;

    try {
      const host = req.headers.get("host") || "localhost";
      url = new URL(req.url, `http://${host}`);
    } catch (e) {
      const safePath =
        typeof req.url === "string" ? req.url.split("?")[0] : String(req.url);
      console.warn(`${method} - ${safePath} failed to parse URL:`, e);
      return new Response("Bad Request", { status: 400 });
    }

    try {
      const dbParam = url.searchParams.get("db");
      const skipSwitch = ["/api/kb/list_projects", "/api/kb/create_project"];
      if (dbParam && !skipSwitch.includes(url.pathname)) {
        try {
          switchDatabase(`${dbParam}.sqlite`);
        } catch (e) {
          // ignore if file not found
        }
      }

      const projectRes = await handleProjectRoutes(req, url, method);
      if (projectRes) return projectRes;

      const authRes = await handleAuthRoutes(req, url, method);
      if (authRes) return authRes;

      const chatRes = await handleChatRoutes(req, url, method);
      if (chatRes) return chatRes;

      const staticResponse = await serveStaticRoute(url.pathname);
      if (staticResponse) return staticResponse;

      const coreKbRes = await handleCoreKbRoutes(req, url, method);
      if (coreKbRes) return coreKbRes;

      const schemaRes = await handleSchemaRoutes(req, url, method);
      if (schemaRes) return schemaRes;

      const wikiRes = await handleWikiRoutes(req, url, method);
      if (wikiRes) return wikiRes;

      return new Response("Not Found", { status: 404 });
    } catch (e) {
      console.warn(
        `${method} - ${url && url.pathname ? url.pathname : "unknown"} failed`,
        e,
      );
      return new Response("Internal Server Error", { status: 500 });
    }
  },
});

console.log(`Listening on http://localhost:${server.port} ...`);
