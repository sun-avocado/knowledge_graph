import { adminDb, hashPassword } from "../db.ts";

function getSessionToken(req: Request) {
  const cookie = req.headers.get("cookie") || "";
  return cookie.match(/kb_session=([^;\s]+)/)?.[1] || null;
}

export async function handleAuthRoutes(req: Request, url: URL, method: string) {
  if (url.pathname === "/api/auth/register" && method === "POST") {
    try {
      const body: any = await req.json();
      const username = (body.username || "").toString().trim().toLowerCase();
      const password = (body.password || "").toString();
      const displayName = (body.displayName || username).toString().trim();
      const avatar = (body.avatar || "").toString().trim();

      if (!username || !password) {
        return Response.json(
          { success: false, message: "用户名或密码不能为空" },
          { status: 400 }
        );
      }
      if (!/^[\w\-\.@]+$/.test(username)) {
        return Response.json(
          { success: false, message: "用户名含有不支持的字符" },
          { status: 400 }
        );
      }

      try {
        const ex = adminDb.query("SELECT 1 FROM users WHERE username = ?").get(username);
        if (ex) {
          return Response.json(
            { success: false, message: "用户名已存在" },
            { status: 409 }
          );
        }
      } catch {}

      const ph = await hashPassword(password);
      adminDb.run(
        "INSERT INTO users (username, display_name, password_hash, password_salt, avatar) VALUES (?, ?, ?, ?, ?)",
        [username, displayName || username, ph.hash || "", ph.salt || "", avatar || ""]
      );
      const user = adminDb
        .query(
          "SELECT username, display_name, avatar, created_at FROM users WHERE username = ?"
        )
        .get(username);
      return Response.json({ success: true, user });
    } catch {
      return Response.json(
        { success: false, message: "无效的请求体" },
        { status: 400 }
      );
    }
  }

  if (url.pathname === "/api/auth/login" && method === "POST") {
    try {
      const body: any = await req.json();
      const username = (body.username || "").toString().trim().toLowerCase();
      const password = (body.password || "").toString();

      if (!username || !password) {
        return Response.json(
          { success: false, message: "用户名或密码不能为空" },
          { status: 400 }
        );
      }

      const u = adminDb
        .query(
          "SELECT username, display_name, password_hash, password_salt, avatar FROM users WHERE username = ?"
        )
        .get(username);
      if (!u) {
        return Response.json(
          { success: false, message: "用户不存在或密码错误" },
          { status: 401 }
        );
      }

      const ph = await hashPassword(password, u.password_salt || "");
      if (!ph.hash || ph.hash !== (u.password_hash || "")) {
        return Response.json(
          { success: false, message: "用户不存在或密码错误" },
          { status: 401 }
        );
      }

      const token = crypto.randomUUID();
      const expires = new Date(Date.now() + 7 * 24 * 3600 * 1000).toISOString();
      const userRow = adminDb
        .query("SELECT id FROM users WHERE username = ?")
        .get(username) as any;
      adminDb.run(
        "INSERT OR REPLACE INTO sessions (id, token, username, user_id, created_at, expires_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)",
        [token, token, username, userRow?.id || null, expires]
      );

      return new Response(
        JSON.stringify({
          success: true,
          user: {
            username: u.username,
            displayName: u.display_name,
            avatar: u.avatar,
          },
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Set-Cookie": `kb_session=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${7 * 24 * 3600}`,
          },
        }
      );
    } catch {
      return Response.json({ success: false, message: "登录失败" }, { status: 400 });
    }
  }

  if (url.pathname === "/api/auth/whoami" && method === "GET") {
    try {
      const token = getSessionToken(req);
      if (!token) return Response.json({ user: null });

      const s = adminDb.query("SELECT username FROM sessions WHERE token = ? OR id = ?").get(token, token);
      if (!s) return Response.json({ user: null });

      const u = adminDb
        .query("SELECT username, display_name, avatar FROM users WHERE username = ?")
        .get(s.username);
      if (!u) return Response.json({ user: null });

      return Response.json({
        user: {
          username: u.username,
          displayName: u.display_name,
          avatar: u.avatar,
        },
      });
    } catch {
      return Response.json({ user: null });
    }
  }

  if (url.pathname === "/api/auth/logout" && method === "POST") {
    try {
      const token = getSessionToken(req);
      if (token) {
        try {
          adminDb.run("DELETE FROM sessions WHERE token = ? OR id = ?", [token, token]);
        } catch {}
      }

      return new Response(JSON.stringify({ success: true }), {
        status: 200,
        headers: {
          "Set-Cookie": "kb_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
        },
      });
    } catch {
      return Response.json({ success: false, message: "登出失败" }, { status: 400 });
    }
  }

  if (url.pathname === "/api/auth/update_profile" && method === "POST") {
    try {
      const token = getSessionToken(req);
      if (!token) {
        return Response.json({ success: false, message: "未登录" }, { status: 401 });
      }

      const s = adminDb.query("SELECT username FROM sessions WHERE token = ? OR id = ?").get(token, token);
      if (!s) {
        return Response.json({ success: false, message: "未登录" }, { status: 401 });
      }

      const body: any = await req.json();
      const displayName = (body.displayName || "").toString().trim();
      const avatar = (body.avatar || "").toString().trim();

      try {
        adminDb.run(
          "UPDATE users SET display_name = ?, avatar = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
          [displayName || s.username, avatar || "", s.username]
        );
      } catch {}

      const u = adminDb
        .query("SELECT username, display_name, avatar FROM users WHERE username = ?")
        .get(s.username);
      return Response.json({ success: true, user: u });
    } catch {
      return Response.json({ success: false, message: "保存失败" }, { status: 400 });
    }
  }

  if (url.pathname === "/api/auth/users" && method === "GET") {
    try {
      const rows = adminDb
        .query("SELECT username, display_name, avatar FROM users ORDER BY created_at DESC")
        .all();
      const users = Array.isArray(rows)
        ? rows.map((r: any) => ({
            username: r.username,
            displayName: r.display_name,
            avatar: r.avatar,
          }))
        : [];
      return Response.json({ users });
    } catch {
      return Response.json({ users: [] });
    }
  }

  return null;
}
