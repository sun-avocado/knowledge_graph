(function () {
  const btnAuth = document.getElementById("btnAuth");
  const authModal = document.getElementById("authModal");
  const btnCloseAuthModal = document.getElementById("btnCloseAuthModal");
  const loginForm = document.getElementById("loginForm");
  const registerForm = document.getElementById("registerForm");
  const btnOpenRegister = document.getElementById("btnOpenRegister");
  const btnOpenLogin = document.getElementById("btnOpenLogin");
  const btnSubmitLogin = document.getElementById("btnSubmitLogin");
  const btnSubmitRegister = document.getElementById("btnSubmitRegister");
  const inputAuthLoginUser = document.getElementById("inputAuthLoginUser");
  const inputAuthLoginPass = document.getElementById("inputAuthLoginPass");
  const inputAuthRegisterUser = document.getElementById("inputAuthRegisterUser");
  const inputAuthRegisterPass = document.getElementById("inputAuthRegisterPass");
  const inputAuthRegisterPassConfirm = document.getElementById(
    "inputAuthRegisterPassConfirm"
  );
  const authLoginError = document.getElementById("authLoginError");
  const authRegisterError = document.getElementById("authRegisterError");
  const profileModal = document.getElementById("profileModal");
  const inputProfileDisplay = document.getElementById("inputProfileDisplay");
  const inputProfileAvatar = document.getElementById("inputProfileAvatar");
  const btnClearProfileAvatar = document.getElementById("btnClearProfileAvatar");
  const profilePreview = document.getElementById("profilePreview");
  const profilePreviewImg = document.getElementById("profilePreviewImg");
  const profilePreviewStatus = document.getElementById("profilePreviewStatus");
  const profileError = document.getElementById("profileError");
  const btnSaveProfile = document.getElementById("btnSaveProfile");
  const btnCancelProfile = document.getElementById("btnCancelProfile");
  const btnLogout = document.getElementById("btnLogout");

  let authUser = null;

  function setAuthUser(user) {
    authUser = user || null;
    window.authUser = authUser;
    if (btnAuth) {
      try {
        if (authUser && authUser.avatar) {
          btnAuth.innerHTML = `<span style="width:24px;height:24px;border-radius:6px;display:inline-block;background-image:url(${authUser.avatar});background-position:center;background-size:cover;border:1px solid var(--border);"></span>`;
          btnAuth.title = authUser.displayName || authUser.username || "用户";
        } else if (authUser) {
          const initials = (authUser.displayName ||
            authUser.username ||
            "?")
            .toString()
            .replace(/\s+/g, "")
            .slice(0, 2)
            .toUpperCase();
          btnAuth.innerHTML = `<span style="display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:6px;background:linear-gradient(135deg,var(--accent),#7c5cff);color:#fff;font-weight:600">${initials}</span>`;
          btnAuth.title = authUser.displayName || authUser.username || "用户";
        } else {
          btnAuth.innerHTML = '<i class="fa-regular fa-user"></i>';
          btnAuth.title = "登录";
        }
      } catch (e) {
        console.warn("setAuthUser failed", e);
      }
    }
    try {
      if (typeof loadUsersToSidebar === "function") loadUsersToSidebar();
    } catch {}
  }

  async function whoami() {
    try {
      const resp = await fetch("/api/auth/whoami", { credentials: "include" });
      if (!resp.ok) return setAuthUser(null);
      const data = await resp.json();
      if (data && data.user) setAuthUser(data.user);
      else setAuthUser(null);
    } catch {
      setAuthUser(null);
    }
  }

  async function logout() {
    try {
      await fetch("/api/auth/logout", {
        method: "POST",
        credentials: "include",
      });
      setAuthUser(null);
      try {
        closeProfileModal();
      } catch {}
      try {
        showToast("已登出");
      } catch {}
    } catch (e) {
      console.warn("logout failed", e);
      try {
        showToast("登出失败", "error");
      } catch {}
    }
  }

  function openAuthModal(showRegister = false) {
    try {
      if (!authModal) return;
      try {
        if (authModal.parentElement !== document.body) {
          document.body.appendChild(authModal);
        }
      } catch {}
      try {
        authModal.style.zIndex = "99999";
      } catch {}
      authModal.style.display = "flex";
      if (showRegister) {
        if (loginForm) loginForm.style.display = "none";
        if (registerForm) registerForm.style.display = "block";
        try {
          if (inputAuthRegisterUser) inputAuthRegisterUser.value = "";
          if (inputAuthRegisterPass) inputAuthRegisterPass.value = "";
          if (inputAuthRegisterPassConfirm) inputAuthRegisterPassConfirm.value = "";
        } catch {}
      } else {
        if (loginForm) loginForm.style.display = "block";
        if (registerForm) registerForm.style.display = "none";
      }
      try {
        if (!showRegister && inputAuthLoginUser) {
          setTimeout(() => inputAuthLoginUser.focus(), 40);
        }
        if (showRegister && inputAuthRegisterUser) {
          setTimeout(() => inputAuthRegisterUser.focus(), 40);
        }
      } catch {}
    } catch (e) {
      console.warn("openAuthModal failed", e);
    }
  }

  function closeAuthModal() {
    if (!authModal) return;
    authModal.style.display = "none";
  }

  function updateProfilePreview(url) {
    if (!profilePreview || !profilePreviewImg || !profilePreviewStatus) return;
    const trimmed = typeof url === "string" ? url.trim() : "";
    if (!trimmed) {
      profilePreview.style.display = "none";
      profilePreviewImg.style.backgroundImage = "";
      profilePreviewStatus.textContent = "";
      return;
    }
    profilePreview.style.display = "flex";
    profilePreviewStatus.textContent = "加载中…";
    profilePreviewImg.style.backgroundImage = "";
    const img = new Image();
    img.onload = () => {
      profilePreviewImg.style.backgroundImage = `url(${trimmed})`;
      profilePreviewStatus.textContent = "预览";
    };
    img.onerror = () => {
      profilePreviewImg.style.backgroundImage = "";
      profilePreviewStatus.textContent = "加载失败";
    };
    img.src = trimmed;
  }

  function openProfileModal() {
    if (!profileModal) return;
    try {
      if (profileModal.parentElement !== document.body) {
        document.body.appendChild(profileModal);
      }
    } catch {}
    try {
      profileModal.style.zIndex = "99999";
    } catch {}
    profileModal.style.display = "flex";
    if (authUser) {
      if (inputProfileDisplay) inputProfileDisplay.value = authUser.displayName || "";
      if (inputProfileAvatar) inputProfileAvatar.value = authUser.avatar || "";
      try {
        updateProfilePreview(authUser.avatar || "");
      } catch {}
    }
    try {
      if (inputProfileDisplay) setTimeout(() => inputProfileDisplay.focus(), 40);
    } catch {}
  }

  function closeProfileModal() {
    if (!profileModal) return;
    profileModal.style.display = "none";
  }

  async function submitLogin() {
    try {
      if (!inputAuthLoginUser || !inputAuthLoginPass) return;
      const username = inputAuthLoginUser.value.trim();
      const password = inputAuthLoginPass.value;
      if (!username || !password) {
        if (authLoginError) {
          authLoginError.textContent = "请输入用户名与密码";
          authLoginError.style.display = "block";
        }
        return;
      }
      if (authLoginError) authLoginError.style.display = "none";
      const resp = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
        credentials: "include",
      });
      const text = await resp.text();
      let data = null;
      try {
        data = text ? JSON.parse(text) : null;
      } catch {}
      if (!resp.ok) {
        if (authLoginError) {
          authLoginError.textContent =
            (data && data.message) || text || `请求失败: HTTP ${resp.status}`;
          authLoginError.style.display = "block";
        }
        return;
      }
      if (data && data.success) {
        setAuthUser(data.user);
        closeAuthModal();
        try {
          showToast("登录成功");
        } catch {}
      } else if (authLoginError) {
        authLoginError.textContent = (data && data.message) || "登录失败";
        authLoginError.style.display = "block";
      }
    } catch {
      if (authLoginError) {
        authLoginError.textContent = "请求失败";
        authLoginError.style.display = "block";
      }
    }
  }

  async function submitRegister() {
    try {
      if (!inputAuthRegisterUser || !inputAuthRegisterPass) return;
      const username = inputAuthRegisterUser.value.trim();
      const password = inputAuthRegisterPass.value;
      const confirm = inputAuthRegisterPassConfirm
        ? inputAuthRegisterPassConfirm.value
        : "";
      if (!username || !password) {
        if (authRegisterError) {
          authRegisterError.textContent = "请输入用户名与密码";
          authRegisterError.style.display = "block";
        }
        return;
      }
      if (password !== confirm) {
        if (authRegisterError) {
          authRegisterError.textContent = "两次输入的密码不一致";
          authRegisterError.style.display = "block";
        }
        return;
      }
      if (password.length < 6) {
        if (authRegisterError) {
          authRegisterError.textContent = "密码需至少6位";
          authRegisterError.style.display = "block";
        }
        return;
      }
      if (authRegisterError) authRegisterError.style.display = "none";
      const resp = await fetch("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username,
          password,
          displayName: "",
          avatar: "",
        }),
      });
      const text = await resp.text();
      let data = null;
      try {
        data = text ? JSON.parse(text) : null;
      } catch {}
      if (!resp.ok) {
        if (authRegisterError) {
          authRegisterError.textContent =
            (data && data.message) || text || `请求失败: HTTP ${resp.status}`;
          authRegisterError.style.display = "block";
        }
        return;
      }
      try {
        const resp2 = await fetch("/api/auth/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ username, password }),
          credentials: "include",
        });
        const t2 = await resp2.text();
        let d2 = null;
        try {
          d2 = t2 ? JSON.parse(t2) : null;
        } catch {}
        if (resp2.ok && d2 && d2.success) {
          setAuthUser(d2.user);
          closeAuthModal();
          return;
        }
      } catch {}
      if (data && data.user) {
        setAuthUser(data.user);
        closeAuthModal();
      }
    } catch {
      if (authRegisterError) {
        authRegisterError.textContent = "请求失败";
        authRegisterError.style.display = "block";
      }
    }
  }

  if (btnAuth) {
    btnAuth.addEventListener("click", (e) => {
      e.preventDefault();
      try {
        const dm = document.getElementById("dbSelectModal");
        if (dm && dm.style.display && dm.style.display !== "none") {
          hideDbSelectModal();
        }
      } catch {}
      if (authUser) openProfileModal();
      else openAuthModal(false);
    });
  }
  if (btnCloseAuthModal) btnCloseAuthModal.addEventListener("click", closeAuthModal);
  if (authModal) {
    authModal.addEventListener("click", (e) => {
      if (e.target === authModal) closeAuthModal();
    });
  }
  if (btnOpenRegister) btnOpenRegister.addEventListener("click", () => openAuthModal(true));
  if (btnOpenLogin) btnOpenLogin.addEventListener("click", () => openAuthModal(false));
  if (inputProfileAvatar) {
    inputProfileAvatar.addEventListener("input", (e) => {
      try {
        updateProfilePreview(e.target.value || "");
      } catch {}
    });
  }
  if (btnClearProfileAvatar) {
    btnClearProfileAvatar.addEventListener("click", (e) => {
      e.preventDefault();
      try {
        if (inputProfileAvatar) {
          inputProfileAvatar.value = "";
          updateProfilePreview("");
        }
      } catch {}
    });
  }
  if (btnSubmitLogin) btnSubmitLogin.addEventListener("click", (e) => {
    e.preventDefault();
    submitLogin();
  });
  if (btnSubmitRegister) btnSubmitRegister.addEventListener("click", (e) => {
    e.preventDefault();
    submitRegister();
  });
  if (inputAuthLoginPass) {
    inputAuthLoginPass.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        submitLogin();
      }
    });
  }
  if (inputAuthRegisterPass) {
    inputAuthRegisterPass.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        submitRegister();
      }
    });
  }
  if (inputAuthRegisterPassConfirm) {
    inputAuthRegisterPassConfirm.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        submitRegister();
      }
    });
  }
  if (btnSaveProfile) {
    btnSaveProfile.addEventListener("click", async () => {
      try {
        const displayName = inputProfileDisplay ? inputProfileDisplay.value.trim() : "";
        const avatar = inputProfileAvatar ? inputProfileAvatar.value.trim() : "";
        try {
          if (avatar) new URL(avatar);
        } catch {
          if (profileError) {
            profileError.textContent = "头像 URL 格式不正确";
            profileError.style.display = "block";
          }
          return;
        }
        if (profileError) {
          profileError.style.display = "none";
          profileError.textContent = "";
        }
        btnSaveProfile.disabled = true;
        const resp = await fetch("/api/auth/update_profile", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ displayName, avatar }),
          credentials: "include",
        });
        const text = await resp.text();
        let data = null;
        try {
          data = text ? JSON.parse(text) : null;
        } catch {}
        if (!resp.ok) {
          if (profileError) {
            profileError.textContent =
              (data && data.message) || text || `请求失败: HTTP ${resp.status}`;
            profileError.style.display = "block";
          }
          return;
        }
        if (data && data.success) {
          setAuthUser(data.user);
          closeProfileModal();
        }
      } catch {
        if (profileError) {
          profileError.textContent = "请求失败";
          profileError.style.display = "block";
        }
      } finally {
        try {
          btnSaveProfile.disabled = false;
        } catch {}
      }
    });
  }
  if (btnCancelProfile) {
    btnCancelProfile.addEventListener("click", (e) => {
      e.preventDefault();
      closeProfileModal();
    });
  }
  if (btnLogout) {
    btnLogout.addEventListener("click", (e) => {
      e.preventDefault();
      logout();
    });
  }
  if (profileModal) {
    profileModal.addEventListener("click", (e) => {
      if (e.target === profileModal) closeProfileModal();
    });
  }

  window.setAuthUser = setAuthUser;
  window.whoami = whoami;
  window.logout = logout;
  window.openAuthModal = openAuthModal;
  window.closeAuthModal = closeAuthModal;
  window.openProfileModal = openProfileModal;
  window.closeProfileModal = closeProfileModal;

  try {
    whoami();
  } catch {}
})();
