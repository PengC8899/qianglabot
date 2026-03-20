const apiBase =
  import.meta.env.VITE_API_BASE ||
  `${window.location.protocol}//${window.location.hostname}:8005`;

export async function apiGet(path) {
  const res = await fetch(`${apiBase}${path}`);
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export async function apiPost(path, payload) {
  const res = await fetch(`${apiBase}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export async function apiUpload(path, formData) {
  // Do NOT set Content-Type header manually for FormData, 
  // browser will set it with boundary automatically.
  const res = await fetch(`${apiBase}${path}`, {
    method: "POST",
    body: formData
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export const getSessions = () => apiGet("/sessions");
export const getSessionOtp = (id) => apiGet(`/sessions/${id}/otp`);
export const checkSession = (id) => apiPost(`/sessions/check/${id}`);
export const batchCheckSessions = (ids) => apiPost("/sessions/batch_check", { ids });
export const batchDeleteSessions = (ids) => apiPost("/sessions/batch_delete", { ids });
export const updateProfile = (formData) => {
  return fetch(`${apiBase}/sessions/update_profile`, {
    method: "POST",
    body: formData
  }).then(res => {
    if (!res.ok) return res.text().then(t => { throw new Error(t) });
    return res.json();
  });
};

export const uploadSession = (formData) => apiUpload("/sessions/upload", formData);
export const sendCode = (phone, api_id, api_hash) => {
    const payload = { phone };
    if (api_id) payload.api_id = Number(api_id);
    if (api_hash) payload.api_hash = api_hash;
    return apiPost("/auth/send_code", payload);
};
export const login = (phone, code, phone_code_hash, api_id, api_hash, password, temp_session, as_manager = false) => {
    const payload = { phone, code, phone_code_hash };
    if (api_id) payload.api_id = Number(api_id);
    if (api_hash) payload.api_hash = api_hash;
    if (password) payload.password = password;
    if (temp_session) payload.temp_session = temp_session;
    if (as_manager) payload.as_manager = true;
    return apiPost("/auth/login", payload);
};
export const createTask = (payload) => apiPost("/tasks/create", payload);
export const createInviteTask = (payload) => apiPost("/tasks/invite/create", payload);
export const checkAccountsInGroup = async (group_link) => {
  const res = await fetch(`${apiBase}/tasks/invite/check_accounts`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ group_link })
  });
  return res.json();
};
export const joinAllSessionsToGroup = (group_link, options = {}) =>
  apiPost("/tasks/invite/join_all", { group_link, ...options });
export const promoteAllSessionsToAdmins = (group_link, options = {}) =>
  apiPost("/tasks/invite/promote_admins", { group_link, ...options });
export const runInviteOneClick = (payload) =>
  apiPost("/tasks/invite/one_click", payload);
export const getTasks = (taskType) => apiGet(taskType ? `/tasks?task_type=${encodeURIComponent(taskType)}` : "/tasks");
export const getTaskTargets = (taskId) => apiGet(`/tasks/${taskId}/targets`);
export const stopTask = (taskId) => apiPost(`/tasks/${taskId}/stop`);
export const restartTask = (taskId) => apiPost(`/tasks/${taskId}/restart`);
export const deleteTask = async (taskId) => {
  const res = await fetch(`${apiBase}/tasks/${taskId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}
export const getLogs = (taskId) => apiGet(taskId ? `/logs?task_id=${taskId}` : "/logs");
export const getLogStats = (taskId) => apiGet(taskId ? `/logs/stats?task_id=${taskId}` : "/logs/stats");

export function getWsUrl(taskId) {
  const wsBase = apiBase.replace(/^http/, "ws");
  if (taskId) {
    return `${wsBase}/ws/logs?task_id=${taskId}`;
  }
  return `${wsBase}/ws/logs`;
}

// Blacklist
export const getBlacklist = () => apiGet("/blacklist/list");

export const addToBlacklist = (username, reason) =>
  apiPost("/blacklist/add", { username, reason });

export const removeFromBlacklist = async (username) => {
  const res = await fetch(`${apiBase}/blacklist/remove/${username}`, {
    method: "DELETE"
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

// Proxies
export const getProxies = () => apiGet("/proxies/list");
export const addProxies = (urls) => apiPost("/proxies/add", { urls });
export const removeProxy = async (id) => {
  const res = await fetch(`${apiBase}/proxies/remove/${id}`, {
    method: "DELETE"
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

// Api Keys
export const getApiKeys = () => apiGet("/apikeys");
export const addApiKeys = (lines) => apiPost("/apikeys/add", { lines });
export const checkApiKey = (id) => apiPost(`/apikeys/check/${id}`);
export const batchCheckApiKeys = (ids) => apiPost("/apikeys/batch_check", { ids });
export const deleteApiKey = async (id) => {
  const res = await fetch(`${apiBase}/apikeys/${id}`, {
    method: "DELETE"
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return res.json();
}

export const getInviteAccounts = () => apiGet("/invite_v2/accounts");
export const refreshInviteAccounts = (group_link) => apiPost("/invite_v2/accounts/refresh", { group_link });
export const addInviteTask = (username, group_link) => apiPost("/invite_v2/invite", { username, group_link });
export const stopInviteTasks = () => apiPost("/invite_v2/stop_all", {});
export const getInviteLogs = () => apiGet("/invite_v2/logs");
export const joinAllAccounts = (group_link) => apiPost("/invite_v2/accounts/join_all", { group_link });
export const leaveAllAccounts = (group_link) => apiPost("/invite_v2/accounts/leave_all", { group_link });
