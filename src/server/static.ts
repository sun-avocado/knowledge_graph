import { resolve } from "path";

const PUBLIC_INDEX_FILE = Bun.file("public/index.html");
const DEMO_CHAT_FILE = Bun.file("demo/chat.html");
const SHARED_UPLOADS_DIR = resolve(
  import.meta.dir,
  "..",
  "..",
  "..",
  "uploads",
);

export async function serveStaticRoute(pathname: string) {
  if (pathname === "/kb" || pathname === "/") {
    return new Response(PUBLIC_INDEX_FILE);
  }

  if (pathname === "/demo/chat.html" || pathname === "/chat") {
    if (await DEMO_CHAT_FILE.exists()) {
      return new Response(DEMO_CHAT_FILE);
    }
    return null;
  }

  if (pathname.startsWith("/static/")) {
    const sharedUploadPrefix = "/static/uploads/";
    if (pathname.startsWith(sharedUploadPrefix)) {
      const relativePath = pathname.slice(sharedUploadPrefix.length);
      const file = Bun.file(resolve(SHARED_UPLOADS_DIR, relativePath));
      if (await file.exists()) {
        return new Response(file);
      }
      return null;
    }

    const file = Bun.file(`.${pathname}`);
    if (await file.exists()) {
      return new Response(file);
    }
    return null;
  }

  if (pathname.startsWith("/assets/")) {
    const file = Bun.file(`public${pathname}`);
    if (await file.exists()) {
      return new Response(file);
    }
    return null;
  }

  if (pathname.startsWith("/js/")) {
    const file = Bun.file(`public${pathname}`);
    if (await file.exists()) {
      return new Response(file);
    }
    return null;
  }

  if (pathname.startsWith("/css/")) {
    const file = Bun.file(`public${pathname}`);
    if (await file.exists()) {
      return new Response(file);
    }
    return null;
  }

  return null;
}
