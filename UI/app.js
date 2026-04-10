const STORAGE_KEY = "hr-hunter-ui-v3";
const SESSION_KEY = "hr-hunter-session-token";
const SESSION_HANDOFF_KEY = "hr-hunter-session-handoff";
const SESSION_HASH_KEY = "session_token";
window.__HR_HUNTER_UI_READY = true;
const TAB_META = {
  projects: {
    title: "Projects",
    description: "Search existing projects, pick the active mandate, or start a new project.",
  },
  recruiter: {
    title: "Hunt",
    description: "Complete the hunt brief, assign teammates, and run searches for the current project.",
  },
  results: {
    title: "Results",
    description: "Review the latest ranked candidates for the selected project.",
  },
  candidates: {
    title: "Candidates",
    description: "Browse, filter, and review the latest candidate list for the selected project.",
  },
  feedback: {
    title: "Feedback",
    description: "See recruiter actions and use them to improve ranking over time.",
  },
  history: {
    title: "History",
    description: "Open previous runs and project activity for the selected mandate.",
  },
  settings: {
    title: "Settings",
    description: "Switch theme, change paths, and send support or feature requests.",
  },
};
const ANCHOR_LEVELS = [
  { id: "ignore", label: "Ignore" },
  { id: "preferred", label: "Preferred" },
  { id: "important", label: "Important" },
  { id: "critical", label: "Critical" },
];
const FEATURE_LABELS = {
  title_similarity: "Title Fit",
  company_match: "Company Fit",
  employment_status: "Employment Status",
  location_match: "Location Fit",
  skill_overlap: "Skills Fit",
  industry_fit: "Industry Fit",
  years_fit: "Years Fit",
  current_function_fit: "Function Fit",
  semantic_similarity: "Semantic Fit",
  parser_confidence: "Parser Confidence",
  evidence_quality: "Evidence Quality",
};
const state = {
  config: null,
  sessionToken: "",
  user: null,
  users: [],
  projects: [],
  selectedProjectId: "",
  selectedProject: null,
  currentReport: null,
  currentRuns: [],
  currentReviews: [],
  currentBreakdown: null,
  uploadedJobDescription: {
    name: "",
    text: "",
    extension: "",
    parser: "",
  },
  activeTab: "projects",
  ownerOpen: false,
  navOpen: false,
  activeJob: null,
  polledJobId: "",
  jobPollHandle: null,
  liveProgressHandle: null,
  tokenFields: {},
  projectSearchQuery: "",
  candidateSearchQuery: "",
  candidateStatusFilter: "all",
  candidateLocationFilter: "all",
  selectedCandidateRef: "",
  settings: {},
};

function emptyUploadedJobDescription() {
  return {
    name: "",
    text: "",
    extension: "",
    parser: "",
  };
}

class TokenField {
  constructor(root, suggestions = []) {
    this.root = root;
    this.tokens = [];
    this.suggestions = suggestions;
    this.root.innerHTML = "";
    this.input = document.createElement("input");
    this.input.className = "token-input";
    this.input.type = "text";
    this.input.placeholder = root.dataset.placeholder || "Add a value";
    if (suggestions.length) {
      const listId = `${root.id}-datalist`;
      const datalist = document.createElement("datalist");
      datalist.id = listId;
      suggestions.forEach((suggestion) => {
        const option = document.createElement("option");
        option.value = suggestion;
        datalist.appendChild(option);
      });
      root.appendChild(datalist);
      this.input.setAttribute("list", listId);
    }
    this.input.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === ",") {
        event.preventDefault();
        this.addFromInput();
      }
    });
    this.input.addEventListener("blur", () => this.addFromInput());
    root.appendChild(this.input);
  }

  parseValue(value) {
    return String(value || "")
      .split(/[\n,;]+/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  add(values) {
    this.parseValue(values).forEach((value) => {
      if (!this.tokens.some((token) => token.toLowerCase() === value.toLowerCase())) {
        this.tokens.push(value);
      }
    });
    this.render();
  }

  addFromInput() {
    const value = this.input.value.trim();
    if (!value) return;
    this.add(value);
    this.input.value = "";
  }

  commitPending() {
    this.addFromInput();
  }

  setTokens(values) {
    this.tokens = [];
    this.add(Array.isArray(values) ? values.join(",") : values);
  }

  getTokens() {
    return [...this.tokens];
  }

  remove(value) {
    this.tokens = this.tokens.filter((token) => token !== value);
    this.render();
  }

  render() {
    this.root.querySelectorAll(".token-chip").forEach((chip) => chip.remove());
    this.tokens.forEach((token) => {
      const chip = document.createElement("span");
      chip.className = "token-chip";
      chip.innerHTML = `<span>${escapeHtml(token)}</span>`;
      const removeButton = document.createElement("button");
      removeButton.type = "button";
      removeButton.textContent = "x";
      removeButton.addEventListener("click", () => this.remove(token));
      chip.appendChild(removeButton);
      this.root.insertBefore(chip, this.input);
    });
  }
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function safeNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function formatTimestamp(value) {
  if (!value) return "Unknown";
  const timestamp = new Date(value);
  return Number.isNaN(timestamp.getTime()) ? String(value) : timestamp.toLocaleString();
}

function formatScore(value) {
  return safeNumber(value).toFixed(2);
}

function formatPercent(value) {
  return `${Math.round(Math.max(0, safeNumber(value, 0) * 100))}%`;
}

function titleCaseWords(value) {
  return String(value || "")
    .split(/[\s_:-]+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
    .join(" ");
}

function humanizeNote(note) {
  const text = String(note || "")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : "";
}

function artifactHref(path) {
  return `/app/artifact?path=${encodeURIComponent(path)}`;
}

function sessionHeaders() {
  return state.sessionToken ? { "X-Session-Token": state.sessionToken } : {};
}

function authConfig() {
  return state.config?.auth || window.__HR_HUNTER_AUTH_CONFIG || {};
}

function loginEmailRequired() {
  return authConfig().email_required !== false;
}

function applyAuthConfig() {
  const emailRequired = loginEmailRequired();
  const emailGroup = document.getElementById("login-email-group");
  const emailInput = document.getElementById("login-email");
  const helpText = document.getElementById("login-help-text");
  const loginMessage = document.getElementById("login-message");

  window.__HR_HUNTER_AUTH_CONFIG = authConfig();

  if (emailGroup) {
    emailGroup.hidden = !emailRequired;
  }
  if (emailInput) {
    emailInput.required = emailRequired;
    emailInput.disabled = !emailRequired;
    if (!emailRequired) {
      emailInput.value = "";
    }
  }
  if (helpText) {
    helpText.textContent = emailRequired
      ? "Use your recruiter email and 6-digit authenticator code to open projects, run searches, and review candidates."
      : "Use the 6-digit code from Google Authenticator to open HR Hunter.";
  }
  if (loginMessage && loginMessage.textContent.includes("Only admin")) {
    loginMessage.textContent = emailRequired
      ? "Only admin can create recruiter accounts and issue authenticator setup keys."
      : "Enter your authenticator code to sign in.";
  }
}

async function fetchJSON(url, options = {}) {
  const headers = {
    ...(options.body ? { "Content-Type": "application/json" } : {}),
    ...sessionHeaders(),
    ...(options.headers || {}),
  };
  let timeoutHandle = null;
  const timeoutMs = Math.max(0, safeNumber(options.timeoutMs, 0));
  const controller = timeoutMs > 0 ? new AbortController() : null;
  if (controller) {
    timeoutHandle = window.setTimeout(() => controller.abort(), timeoutMs);
  }
  let response;
  try {
    response = await fetch(url, {
      method: options.method || "GET",
      headers,
      body: options.body ? JSON.stringify(options.body) : undefined,
      ...(controller ? { signal: controller.signal } : {}),
    });
  } finally {
    if (timeoutHandle) {
      window.clearTimeout(timeoutHandle);
    }
  }
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const detail = typeof payload === "string" ? payload : payload.detail || "Request failed.";
    throw new Error(detail);
  }
  return payload;
}

async function fetchFormData(url, formData, options = {}) {
  const headers = {
    ...sessionHeaders(),
    ...(options.headers || {}),
  };
  const response = await fetch(url, {
    method: options.method || "POST",
    headers,
    body: formData,
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const detail = typeof payload === "string" ? payload : payload.detail || "Request failed.";
    throw new Error(detail);
  }
  return payload;
}

function setUploadedJobDescription(payload = {}) {
  state.uploadedJobDescription = {
    ...emptyUploadedJobDescription(),
    name: String(payload.name || payload.uploaded_file_name || "").trim(),
    text: String(payload.text || payload.uploaded_job_description_text || "").trim(),
    extension: String(payload.extension || payload.uploaded_file_extension || "").trim(),
    parser: String(payload.parser || payload.uploaded_parser || "").trim(),
  };
  renderUploadedJdSummary();
}

function readStoredState() {
  try {
    return JSON.parse(window.localStorage.getItem(STORAGE_KEY) || "{}");
  } catch {
    return {};
  }
}

function persistStoredState() {
  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        settings: state.settings,
        selectedProjectId: state.selectedProjectId,
      }),
    );
  } catch {
    // Non-fatal: browser may block storage in managed profiles.
  }
}

function clearStoredState() {
  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Non-fatal.
  }
}

function setStatus(message, tone = "default", detail = "") {
  const statusLine = document.getElementById("status-line");
  const statusDetail = document.getElementById("status-detail");
  const statusCard = document.getElementById("status-card");
  statusLine.textContent = message;
  statusDetail.textContent = detail || TAB_META[state.activeTab]?.description || "";
  statusCard.dataset.tone = tone;
}

function applyTheme(themeId) {
  const resolved = themeId === "dark" ? "dark" : "bright";
  document.body.classList.remove("theme-bright", "theme-dark");
  document.body.classList.add(`theme-${resolved}`);
  state.settings.theme = resolved;
  document.querySelectorAll("[data-theme-button]").forEach((button) => {
    button.classList.toggle("active", button.dataset.themeButton === resolved);
  });
}

function defaultSettingsFromConfig() {
  const defaults = state.config?.defaults || {};
  const paths = state.config?.paths || {};
  return {
    theme: defaults.theme || "bright",
    limit: safeNumber(defaults.limit, 20),
    feedback_db: defaults.feedback_db || paths.feedback_db || "",
    model_dir: defaults.model_dir || paths.model_dir || "",
    output_dir: defaults.output_dir || paths.output_dir || "",
    providers: Array.isArray(defaults.providers) ? defaults.providers : ["scrapingbee_google"],
    reranker_enabled: defaults.reranker_enabled !== false,
    learned_ranker_enabled: Boolean(defaults.learned_ranker_enabled),
    include_history_slices: defaults.include_history_slices !== false,
    include_discovery_slices: defaults.include_discovery_slices !== false,
    registry_memory_enabled: defaults.registry_memory_enabled !== false,
    reranker_model_name: defaults.reranker_model_name || "BAAI/bge-reranker-v2-m3",
  };
}

function hydrateSettings() {
  const stored = readStoredState();
  state.selectedProjectId = String(stored.selectedProjectId || "").trim();
  const defaults = defaultSettingsFromConfig();
  const candidateProviders = Array.isArray(stored.settings?.providers) ? stored.settings.providers : defaults.providers;
  state.settings = {
    ...defaults,
    ...(stored.settings || {}),
    providers: candidateProviders.filter((provider) => (state.config?.providers || []).includes(provider)),
  };
  if (!state.settings.providers.length) {
    state.settings.providers = [...defaults.providers];
  }
  if (!Number.isFinite(Number(state.settings.limit)) || Number(state.settings.limit) < 1) {
    state.settings.limit = defaults.limit;
  }
  applyTheme(state.settings.theme);
}

function showAuthShell() {
  closeNav();
  closeOwnerDrawer();
  document.getElementById("auth-shell").hidden = false;
  document.getElementById("app-shell").hidden = true;
}

function showAppShell() {
  document.getElementById("auth-shell").hidden = true;
  document.getElementById("app-shell").hidden = false;
}

function showRestoringShell(detail = "Loading your projects and hunt workspace.") {
  showAppShell();
  setStatus("Restoring your session...", "default", detail);
}

function openNav() {
  state.navOpen = true;
  document.getElementById("nav-drawer").hidden = false;
  document.getElementById("nav-backdrop").hidden = false;
}

function closeNav() {
  state.navOpen = false;
  document.getElementById("nav-drawer").hidden = true;
  document.getElementById("nav-backdrop").hidden = true;
}

function openOwnerDrawer() {
  if (!state.user?.is_admin) return;
  state.ownerOpen = true;
  document.getElementById("owner-drawer").hidden = false;
  renderOwnerSnapshot();
}

function closeOwnerDrawer() {
  state.ownerOpen = false;
  document.getElementById("owner-drawer").hidden = true;
}

function switchTab(tabId) {
  state.activeTab = tabId;
  document.querySelectorAll("[data-tab-panel]").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.tabPanel === tabId);
  });
  document.querySelectorAll("[data-tab-target]").forEach((button) => {
    button.classList.toggle("active", button.dataset.tabTarget === tabId);
  });
  document.getElementById("view-title").textContent = TAB_META[tabId].title;
  document.getElementById("view-heading").textContent = TAB_META[tabId].title;
  document.getElementById("view-description").textContent = TAB_META[tabId].description;
  updateTopbarActions();
  closeNav();
  setStatus(state.selectedProject ? `${state.selectedProject.name} selected.` : "Ready", "default", TAB_META[tabId].description);
  syncLiveJobStatus();
}

function updateTopbarActions() {
  const saveButton = document.getElementById("top-save-button");
  const deleteButton = document.getElementById("top-delete-button");
  const runButton = document.getElementById("top-run-button");
  const onProjects = state.activeTab === "projects";
  const onHunt = state.activeTab === "recruiter";

  saveButton.hidden = !onProjects;
  deleteButton.hidden = !(onProjects && state.selectedProjectId);
  runButton.hidden = !onHunt;
}

function selectedProjectFromList() {
  return state.projects.find((project) => project.id === state.selectedProjectId) || null;
}

function currentAnchorValues() {
  const values = {};
  document.querySelectorAll("[data-anchor-select]").forEach((select) => {
    values[select.dataset.anchorSelect] = select.value || "ignore";
  });
  return values;
}

function defaultAnchorValues() {
  const values = {};
  (state.config?.anchors || []).forEach((anchor) => {
    values[anchor.id] = anchor.default || "preferred";
  });
  return values;
}

function renderAnchorGrid(values = {}) {
  const root = document.getElementById("anchor-grid");
  root.innerHTML = "";
  const merged = { ...defaultAnchorValues(), ...values };
  (state.config?.anchors || []).forEach((anchor) => {
    const wrapper = document.createElement("div");
    wrapper.className = "anchor-card";
    wrapper.innerHTML = `
      <div class="inline-heading">
        <span>${escapeHtml(anchor.label)}</span>
      </div>
      <p class="muted small">${escapeHtml(anchor.description || "")}</p>
    `;
    const select = document.createElement("select");
    select.dataset.anchorSelect = anchor.id;
    ANCHOR_LEVELS.forEach((level) => {
      const option = document.createElement("option");
      option.value = level.id;
      option.textContent = level.label;
      if (merged[anchor.id] === level.id) {
        option.selected = true;
      }
      select.appendChild(option);
    });
    wrapper.appendChild(select);
    root.appendChild(wrapper);
  });
}

function renderThemeToggle() {
  const root = document.getElementById("theme-toggle");
  root.innerHTML = "";
  (state.config?.themes || []).forEach((theme) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "theme-button";
    button.dataset.themeButton = theme.id;
    button.innerHTML = `<strong>${escapeHtml(theme.label)}</strong><span>${escapeHtml(theme.description || "")}</span>`;
    button.addEventListener("click", () => {
      applyTheme(theme.id);
      persistStoredState();
    });
    root.appendChild(button);
  });
  applyTheme(state.settings.theme);
}

function renderProjectStatuses() {
  const select = document.getElementById("project-status");
  select.innerHTML = "";
  (state.config?.project_statuses || []).forEach((status) => {
    const option = document.createElement("option");
    option.value = status.id;
    option.textContent = status.label;
    select.appendChild(option);
  });
}

function renderProviderOptions() {
  const root = document.getElementById("provider-options");
  if (!root) return;
  root.innerHTML = "";
}

function hideProvisioningCard() {
  const card = document.getElementById("owner-provision-card");
  if (card) {
    card.hidden = true;
  }
}

async function copyToClipboard(value) {
  const text = String(value || "").trim();
  if (!text) return false;
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}

function renderProvisioningCard(payload, detailMessage = "") {
  const card = document.getElementById("owner-provision-card");
  if (!card || !payload?.totp) return;
  document.getElementById("owner-provision-email").value = payload.user?.email || payload.totp.account_name || "";
  document.getElementById("owner-provision-secret").value = payload.totp.secret || "";
  document.getElementById("owner-provision-uri").value = payload.totp.provisioning_uri || "";
  document.getElementById("owner-provision-note").textContent =
    detailMessage || "Add the manual key to any authenticator app if the provisioning link cannot be opened directly.";
  card.hidden = false;
  card.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function setCheckboxIfPresent(id, value) {
  const input = document.getElementById(id);
  if (input) {
    input.checked = Boolean(value);
  }
}

function bindOwnerToggle(ownerId, settingsId, stateKey, detailMessage) {
  const input = document.getElementById(ownerId);
  if (!input) return;
  input.addEventListener("change", () => {
    state.settings[stateKey] = input.checked;
    if (settingsId) {
      setCheckboxIfPresent(settingsId, input.checked);
    }
    persistStoredState();
    setStatus("Developer controls updated.", "success", detailMessage);
  });
}

function renderMemberPicker(selectedIds = []) {
  const root = document.getElementById("assigned-recruiters");
  root.innerHTML = "";
  if (!state.users.length) {
    root.innerHTML = `<p class="muted">No recruiter accounts loaded yet.</p>`;
    return;
  }
  state.users.forEach((user) => {
    const row = document.createElement("label");
    row.className = "member-option";
    row.innerHTML = `
      <input type="checkbox" data-member-id="${escapeHtml(user.id)}" />
      <span>
        <strong>${escapeHtml(user.full_name || user.email)}</strong>
        <small>${escapeHtml(user.email)}${user.team_id ? ` | ${escapeHtml(user.team_id)}` : ""}${user.is_admin ? " | Admin" : ""}</small>
      </span>
    `;
    row.querySelector("input").checked = selectedIds.includes(user.id);
    root.appendChild(row);
  });
}

function renderBreakdown() {
  const root = document.getElementById("breakdown-panel");
  const breakdown = state.currentBreakdown;
  if (!breakdown || !Object.keys(breakdown).length) {
    root.innerHTML = `<p class="muted">No breakdown yet. Upload a JD file or paste a JD and click Break Down JD.</p>`;
    return;
  }
  const keyPoints = Array.isArray(breakdown.key_experience_points) ? breakdown.key_experience_points.slice(0, 10) : [];
  const required = Array.isArray(breakdown.required_keywords) ? breakdown.required_keywords : [];
  const preferred = Array.isArray(breakdown.preferred_keywords) ? breakdown.preferred_keywords : [];
  const industries = Array.isArray(breakdown.industry_keywords) ? breakdown.industry_keywords : [];
  const titles = Array.isArray(breakdown.titles) ? breakdown.titles : [];
  const seniority = Array.isArray(breakdown.seniority_levels) ? breakdown.seniority_levels : [];
  const years = breakdown.years || {};
  const uploadedSourceName = breakdown.uploaded_file_name || state.uploadedJobDescription?.name || "";
  const sourceLabel = breakdown.source === "uploaded_file" || uploadedSourceName
    ? `Uploaded file${uploadedSourceName ? `: ${uploadedSourceName}` : ""}`
    : "Typed description";
  root.innerHTML = `
    <p class="muted small"><strong>Source:</strong> ${escapeHtml(sourceLabel)}</p>
    <p>${escapeHtml(breakdown.summary || "Breakdown ready.")}</p>
    ${keyPoints.length
      ? `<div class="breakdown-section"><strong>Key Experience Points</strong><ul class="breakdown-list">${keyPoints.map((point) => `<li>${escapeHtml(point)}</li>`).join("")}</ul></div>`
      : ""}
    ${required.length ? `<p><strong>Required:</strong> ${escapeHtml(required.join(", "))}</p>` : ""}
    ${preferred.length ? `<p><strong>Preferred:</strong> ${escapeHtml(preferred.join(", "))}</p>` : ""}
    ${industries.length ? `<p><strong>Industry:</strong> ${escapeHtml(industries.join(", "))}</p>` : ""}
    ${titles.length ? `<p><strong>Titles:</strong> ${escapeHtml(titles.join(", "))}</p>` : ""}
    ${seniority.length ? `<p><strong>Seniority:</strong> ${escapeHtml(seniority.map(titleCaseWords).join(", "))}</p>` : ""}
    ${(years.min !== null && years.min !== undefined) || (years.max !== null && years.max !== undefined)
      ? `<p><strong>Years:</strong> ${escapeHtml(years.min ?? "0")} - ${escapeHtml(years.max ?? years.value ?? "")}</p>`
      : ""}
  `;
}

function renderUploadedJdSummary() {
  const root = document.getElementById("jd-upload-summary");
  if (!root) return;
  const upload = state.uploadedJobDescription || emptyUploadedJobDescription();
  if (!upload.name || !upload.text) {
    root.innerHTML = `<p class="muted">No JD file uploaded yet. If you upload a file, HR Hunter will use it as the main JD source and treat typed text as optional notes.</p>`;
    return;
  }
  const parserLabel = titleCaseWords(String(upload.parser || "uploaded file").replace(/_/g, " "));
  root.innerHTML = `
    <div class="jd-source-meta">
      <strong>${escapeHtml(upload.name)}</strong>
      ${upload.extension ? `<span class="info-chip">${escapeHtml(upload.extension.toUpperCase().replace(".", ""))}</span>` : ""}
      <span class="info-chip">${escapeHtml(parserLabel)}</span>
    </div>
    <small class="muted">This uploaded file is now the primary JD source. Any typed text below will be treated as optional recruiter notes and used to help extraction.</small>
    <div class="jd-source-actions">
      <button type="button" class="button button-secondary button-small" id="jd-clear-upload-button">Remove Uploaded File</button>
    </div>
  `;
  document.getElementById("jd-clear-upload-button")?.addEventListener("click", clearUploadedJobDescription);
}

function applyBreakdownToForm(breakdown, options = {}) {
  if (!breakdown || !Object.keys(breakdown).length) {
    return;
  }
  state.currentBreakdown = breakdown;
  renderBreakdown();
  renderAnchorGrid({ ...currentAnchorValues(), ...(breakdown.suggested_anchors || {}) });
  const titleInput = document.getElementById("role-title");
  if (options.fillRoleTitle !== false && !titleInput.value.trim() && Array.isArray(breakdown.titles) && breakdown.titles.length) {
    titleInput.value = breakdown.titles[0];
  }
  const years = breakdown.years || {};
  if (years.value !== null && years.value !== undefined && !document.getElementById("years-value").value) {
    document.getElementById("years-value").value = years.value;
  }
  if (years.tolerance !== null && years.tolerance !== undefined && !document.getElementById("years-tolerance").value) {
    document.getElementById("years-tolerance").value = years.tolerance;
  }
  if (years.min !== null && years.min !== undefined && !document.getElementById("min-years").value) {
    document.getElementById("min-years").value = years.min;
  }
  if (years.max !== null && years.max !== undefined && !document.getElementById("max-years").value) {
    document.getElementById("max-years").value = years.max;
  }
}

function clearUploadedJobDescription() {
  state.uploadedJobDescription = emptyUploadedJobDescription();
  const uploadInput = document.getElementById("jd-upload-input");
  if (uploadInput) {
    uploadInput.value = "";
  }
  if (state.currentBreakdown?.source === "uploaded_file") {
    state.currentBreakdown = null;
    renderBreakdown();
  }
  renderUploadedJdSummary();
  setStatus("Uploaded JD file removed.", "success", "The typed description will be used the next time you run JD Breakdown.");
}

function initialiseTokenFields() {
  const config = state.config || {};
  state.tokenFields = {
    titles: new TokenField(document.getElementById("titles-field")),
    countries: new TokenField(document.getElementById("countries-field"), config.countries || []),
    continents: new TokenField(document.getElementById("continents-field"), config.continents || []),
    cities: new TokenField(document.getElementById("cities-field")),
    companies: new TokenField(document.getElementById("companies-field")),
    mustHave: new TokenField(document.getElementById("must-have-field")),
    niceToHave: new TokenField(document.getElementById("nice-to-have-field")),
    industry: new TokenField(document.getElementById("industry-field")),
    excludeTitles: new TokenField(document.getElementById("exclude-titles-field")),
    excludeCompanies: new TokenField(document.getElementById("exclude-companies-field")),
  };
}

function getCheckedMemberIds() {
  return Array.from(document.querySelectorAll("[data-member-id]"))
    .filter((checkbox) => checkbox.checked)
    .map((checkbox) => checkbox.dataset.memberId);
}

function populateSettingsFields() {
  document.getElementById("settings-limit").value = String(state.settings.limit || 20);
  document.getElementById("settings-feedback-db").value = state.settings.feedback_db || "";
  document.getElementById("settings-model-dir").value = state.settings.model_dir || "";
  document.getElementById("settings-output-dir").value = state.settings.output_dir || "";
  document.getElementById("settings-semantic-enabled").checked = Boolean(state.settings.reranker_enabled);
  document.getElementById("settings-feedback-model-enabled").checked = Boolean(state.settings.learned_ranker_enabled);
  document.getElementById("settings-history-context-enabled").checked = Boolean(state.settings.include_history_slices);
  setCheckboxIfPresent("owner-semantic-enabled", state.settings.reranker_enabled);
  setCheckboxIfPresent("owner-feedback-model-enabled", state.settings.learned_ranker_enabled);
  setCheckboxIfPresent("owner-history-context-enabled", state.settings.include_history_slices);
  setCheckboxIfPresent("owner-discovery-enabled", state.settings.include_discovery_slices);
  setCheckboxIfPresent("owner-memory-enabled", state.settings.registry_memory_enabled);
  const modelLabel = document.getElementById("settings-model-display");
  if (modelLabel) {
    modelLabel.textContent = "HR Hunter Model V1";
  }
}

function collectSettingsFromInputs() {
  state.settings.limit = Math.max(1, safeNumber(document.getElementById("settings-limit").value, 20));
  state.settings.feedback_db = document.getElementById("settings-feedback-db").value.trim();
  state.settings.model_dir = document.getElementById("settings-model-dir").value.trim();
  state.settings.output_dir = document.getElementById("settings-output-dir").value.trim();
  state.settings.reranker_enabled = document.getElementById("settings-semantic-enabled").checked;
  state.settings.learned_ranker_enabled = document.getElementById("settings-feedback-model-enabled").checked;
  state.settings.include_history_slices = document.getElementById("settings-history-context-enabled").checked;
  state.settings.providers = Array.isArray(state.config?.defaults?.providers)
    ? [...state.config.defaults.providers]
    : ["scrapingbee_google"];
  state.settings.reranker_model_name = state.config?.defaults?.reranker_model_name || "BAAI/bge-reranker-v2-m3";
}

function resetProjectForm() {
  document.getElementById("project-name").value = "";
  document.getElementById("client-name").value = "";
  document.getElementById("project-status").value = "active";
  document.getElementById("role-title").value = "";
  document.getElementById("target-geography").value = "";
  document.getElementById("project-notes").value = "";
  document.getElementById("years-mode").value = "range";
  document.getElementById("years-value").value = "";
  document.getElementById("years-tolerance").value = "";
  document.getElementById("min-years").value = "";
  document.getElementById("max-years").value = "";
  document.getElementById("radius-miles").value = String(state.config?.defaults?.radius_miles || 25);
  document.getElementById("candidate-limit").value = String(state.settings.limit || state.config?.defaults?.limit || 20);
  document.getElementById("company-match-mode").value = state.config?.defaults?.company_match_mode || "both";
  document.getElementById("employment-status-mode").value = state.config?.defaults?.employment_status_mode || "any";
  document.getElementById("job-description").value = "";
  setUploadedJobDescription();
  Object.values(state.tokenFields).forEach((field) => field.setTokens([]));
  renderMemberPicker(state.user ? [state.user.id] : []);
  state.currentBreakdown = null;
  renderBreakdown();
  renderAnchorGrid();
}

function startNewProject() {
  state.selectedProjectId = "";
  state.selectedProject = null;
  state.currentReport = null;
  state.currentRuns = [];
  state.currentReviews = [];
  state.activeJob = null;
  state.candidateSearchQuery = "";
  state.candidateStatusFilter = "all";
  state.candidateLocationFilter = "all";
  state.selectedCandidateRef = "";
  resetProjectForm();
  renderProjectList();
  renderProjectSummary();
  renderResults();
  renderCandidates();
  renderFeedback();
  renderHistory();
  updateTopbarActions();
  persistStoredState();
  switchTab("recruiter");
  setStatus("New project form ready.", "default", "Fill the hunt brief, then save the project.");
}

function buildUiMeta() {
  Object.values(state.tokenFields).forEach((field) => field?.commitPending?.());
  return {
    titles: state.tokenFields.titles.getTokens(),
    countries: state.tokenFields.countries.getTokens(),
    continents: state.tokenFields.continents.getTokens(),
    cities: state.tokenFields.cities.getTokens(),
    company_targets: state.tokenFields.companies.getTokens(),
    must_have_keywords: state.tokenFields.mustHave.getTokens(),
    nice_to_have_keywords: state.tokenFields.niceToHave.getTokens(),
    industry_keywords: state.tokenFields.industry.getTokens(),
    exclude_title_keywords: state.tokenFields.excludeTitles.getTokens(),
    exclude_company_keywords: state.tokenFields.excludeCompanies.getTokens(),
    years_mode: document.getElementById("years-mode").value,
    years_value: document.getElementById("years-value").value,
    years_tolerance: document.getElementById("years-tolerance").value,
    minimum_years_experience: document.getElementById("min-years").value,
    maximum_years_experience: document.getElementById("max-years").value,
    radius_miles: document.getElementById("radius-miles").value,
    candidate_limit: document.getElementById("candidate-limit").value,
    company_match_mode: document.getElementById("company-match-mode").value,
    employment_status_mode: document.getElementById("employment-status-mode").value,
    job_description: document.getElementById("job-description").value,
    uploaded_job_description_name: state.uploadedJobDescription?.name || "",
    uploaded_job_description_text: state.uploadedJobDescription?.text || "",
    uploaded_job_description_extension: state.uploadedJobDescription?.extension || "",
    uploaded_job_description_parser: state.uploadedJobDescription?.parser || "",
    anchors: currentAnchorValues(),
  };
}

function buildBriefPayload() {
  collectSettingsFromInputs();
  const roleTitle = document.getElementById("role-title").value.trim();
  const uiMeta = buildUiMeta();
  const candidateLimit = Math.max(1, safeNumber(uiMeta.candidate_limit, state.settings.limit || 20));
  return {
    role_title: roleTitle,
    titles: uiMeta.titles,
    countries: uiMeta.countries,
    continents: uiMeta.continents,
    cities: uiMeta.cities,
    company_targets: uiMeta.company_targets,
    company_match_mode: uiMeta.company_match_mode,
    employment_status_mode: uiMeta.employment_status_mode,
    years_mode: uiMeta.years_mode,
    years_value: uiMeta.years_value,
    years_tolerance: uiMeta.years_tolerance,
    minimum_years_experience: uiMeta.minimum_years_experience,
    maximum_years_experience: uiMeta.maximum_years_experience,
    radius_miles: uiMeta.radius_miles,
    must_have_keywords: uiMeta.must_have_keywords,
    nice_to_have_keywords: uiMeta.nice_to_have_keywords,
    industry_keywords: uiMeta.industry_keywords,
    exclude_title_keywords: uiMeta.exclude_title_keywords,
    exclude_company_keywords: uiMeta.exclude_company_keywords,
    job_description: uiMeta.job_description,
    uploaded_job_description_name: uiMeta.uploaded_job_description_name,
    uploaded_job_description_text: uiMeta.uploaded_job_description_text,
    jd_breakdown: state.currentBreakdown,
    anchors: uiMeta.anchors,
    providers: state.settings.providers,
    limit: candidateLimit,
    csv_export_limit: candidateLimit,
    feedback_db: state.settings.feedback_db,
    model_dir: state.settings.model_dir,
    output_dir: state.settings.output_dir,
    reranker_enabled: state.settings.reranker_enabled,
    learned_ranker_enabled: state.settings.learned_ranker_enabled,
    include_history_slices: state.settings.include_history_slices,
    include_discovery_slices: state.settings.include_discovery_slices,
    registry_memory_enabled: state.settings.registry_memory_enabled,
    reranker_model_name: state.settings.reranker_model_name,
    ui_meta: uiMeta,
  };
}

function assessHuntReadiness(briefPayload) {
  const roleTitle = String(briefPayload?.role_title || "").trim();
  const countries = Array.isArray(briefPayload?.countries) ? briefPayload.countries.filter(Boolean) : [];
  const continents = Array.isArray(briefPayload?.continents) ? briefPayload.continents.filter(Boolean) : [];
  const cities = Array.isArray(briefPayload?.cities) ? briefPayload.cities.filter(Boolean) : [];
  const titles = Array.isArray(briefPayload?.titles) ? briefPayload.titles.filter(Boolean) : [];
  const mustHave = Array.isArray(briefPayload?.must_have_keywords) ? briefPayload.must_have_keywords.filter(Boolean) : [];
  const industry = Array.isArray(briefPayload?.industry_keywords) ? briefPayload.industry_keywords.filter(Boolean) : [];
  const companies = Array.isArray(briefPayload?.company_targets) ? briefPayload.company_targets.filter(Boolean) : [];
  const typedJD = String(briefPayload?.job_description || "").trim();
  const uploadedJD = String(briefPayload?.uploaded_job_description_text || "").trim();
  const jdText = uploadedJD || typedJD;

  let score = 0;
  let detailSignals = 0;
  const missing = [];

  if (roleTitle) {
    score += 2;
  } else {
    missing.push("role title");
  }

  const hasGeo = (countries.length + continents.length + cities.length) > 0;
  if (hasGeo) {
    score += 2;
    detailSignals += 1;
  } else {
    missing.push("target geography (country/city/continent)");
  }

  if (mustHave.length >= 2) {
    score += 3;
    detailSignals += 1;
  } else if (mustHave.length === 1) {
    score += 2;
    detailSignals += 1;
  }

  if (titles.length >= 2) {
    score += 1;
    detailSignals += 1;
  }

  if (industry.length > 0) {
    score += 1;
    detailSignals += 1;
  }

  if (companies.length >= 2) {
    score += 1;
    detailSignals += 1;
  }

  if (jdText.length >= 220) {
    score += 2;
    detailSignals += 1;
  } else if (jdText.length >= 100) {
    score += 1;
    detailSignals += 1;
  }

  const ok = Boolean(roleTitle && hasGeo && detailSignals >= 2 && score >= 5);
  const message = ok
    ? "Brief details look sufficient for search."
    : `Hunt details are not enough yet. Missing: ${missing.join(", ") || "more role detail"}. Add at least two detail sections (for example JD text + must-have skills).`;
  return { ok, score, message };
}

function summarizeTargetGeographyFromTokens({ targetGeography = "", countries = [], continents = [], cities = [] } = {}) {
  const explicit = String(targetGeography || "").trim();
  if (explicit) return explicit;

  const summarize = (values, limit = 3) => {
    const picked = values.filter(Boolean).slice(0, limit);
    const remainder = Math.max(0, values.filter(Boolean).length - picked.length);
    const label = picked.join(", ");
    return remainder > 0 ? `${label} (+${remainder} more)` : label;
  };

  if (countries.length > 1) {
    return summarize(countries, 3);
  }
  if (countries.length === 1 && cities.length) {
    const citySummary = summarize(cities, 2);
    return [citySummary, countries[0]].filter(Boolean).join(", ");
  }
  if (countries.length === 1) {
    return countries[0];
  }
  if (continents.length) {
    return summarize(continents, 3);
  }
  if (cities.length) {
    return summarize(cities, 3);
  }
  return "";
}

function projectPayloadForSave() {
  const briefPayload = buildBriefPayload();
  const projectName = document.getElementById("project-name").value.trim();
  const roleTitle = document.getElementById("role-title").value.trim();
  const countryTokens = state.tokenFields.countries.getTokens();
  const continentTokens = state.tokenFields.continents.getTokens();
  const cityTokens = state.tokenFields.cities.getTokens();
  const targetGeography = summarizeTargetGeographyFromTokens({
    targetGeography: document.getElementById("target-geography").value.trim(),
    countries: countryTokens,
    continents: continentTokens,
    cities: cityTokens,
  });
  return {
    name: projectName || `${roleTitle || "New Role"}${targetGeography ? ` - ${targetGeography}` : ""}`,
    client_name: document.getElementById("client-name").value.trim(),
    role_title: roleTitle,
    target_geography: targetGeography,
    status: document.getElementById("project-status").value || "active",
    notes: document.getElementById("project-notes").value.trim(),
    assigned_user_ids: getCheckedMemberIds(),
    brief_json: briefPayload,
  };
}

function formValuesFromProject(project) {
  const brief = project?.latest_brief_json || {};
  const meta = brief.ui_meta || {};
  const geography = brief.geography || {};
  return {
    projectName: project?.name || "",
    clientName: project?.client_name || "",
    status: project?.status || "active",
    roleTitle: project?.role_title || brief.role_title || "",
    targetGeography: project?.target_geography || "",
    notes: project?.notes || "",
    assignedRecruiterIds: (project?.assigned_recruiters || []).map((user) => user.id),
    titles: meta.titles || brief.titles || [],
    countries: meta.countries || (geography.country ? [geography.country] : []),
    continents: meta.continents || [],
    cities: meta.cities || (geography.location_name && geography.location_name !== geography.country ? [geography.location_name] : []),
    companyTargets: meta.company_targets || brief.company_targets || [],
    mustHaveKeywords: meta.must_have_keywords || brief.required_keywords || [],
    niceToHaveKeywords: meta.nice_to_have_keywords || brief.preferred_keywords || [],
    industryKeywords: meta.industry_keywords || brief.industry_keywords || [],
    excludeTitles: meta.exclude_title_keywords || brief.exclude_title_keywords || [],
    excludeCompanies: meta.exclude_company_keywords || brief.exclude_company_keywords || [],
    yearsMode: meta.years_mode || brief.years_mode || "range",
    yearsValue: meta.years_value ?? brief.years_target ?? "",
    yearsTolerance: meta.years_tolerance ?? brief.years_tolerance ?? "",
    minYears: meta.minimum_years_experience ?? brief.minimum_years_experience ?? "",
    maxYears: meta.maximum_years_experience ?? brief.maximum_years_experience ?? "",
    radiusMiles: meta.radius_miles ?? geography.radius_miles ?? state.config?.defaults?.radius_miles ?? 25,
    candidateLimit: meta.candidate_limit ?? brief.max_profiles ?? state.settings.limit ?? state.config?.defaults?.limit ?? 20,
    companyMatchMode: meta.company_match_mode || brief.company_match_mode || "both",
    employmentStatusMode: meta.employment_status_mode || brief.employment_status_mode || "any",
    jobDescription: meta.job_description || brief.job_description_source?.typed_text || "",
    uploadedJobDescriptionName: meta.uploaded_job_description_name || brief.uploaded_job_description_name || brief.job_description_source?.file_name || "",
    uploadedJobDescriptionText: meta.uploaded_job_description_text || brief.uploaded_job_description_text || brief.job_description_source?.uploaded_text || "",
    uploadedJobDescriptionExtension: meta.uploaded_job_description_extension || "",
    uploadedJobDescriptionParser: meta.uploaded_job_description_parser || "",
    anchors: meta.anchors || brief.anchors || {},
    breakdown: brief.jd_breakdown || null,
  };
}

function populateProjectForm(project) {
  const values = formValuesFromProject(project);
  document.getElementById("project-name").value = values.projectName;
  document.getElementById("client-name").value = values.clientName;
  document.getElementById("project-status").value = values.status;
  document.getElementById("role-title").value = values.roleTitle;
  document.getElementById("target-geography").value = values.targetGeography;
  document.getElementById("project-notes").value = values.notes;
  document.getElementById("years-mode").value = values.yearsMode;
  document.getElementById("years-value").value = values.yearsValue;
  document.getElementById("years-tolerance").value = values.yearsTolerance;
  document.getElementById("min-years").value = values.minYears;
  document.getElementById("max-years").value = values.maxYears;
  document.getElementById("radius-miles").value = values.radiusMiles;
  document.getElementById("candidate-limit").value = values.candidateLimit;
  document.getElementById("company-match-mode").value = values.companyMatchMode;
  document.getElementById("employment-status-mode").value = values.employmentStatusMode;
  document.getElementById("job-description").value = values.jobDescription;
  setUploadedJobDescription({
    name: values.uploadedJobDescriptionName,
    text: values.uploadedJobDescriptionText,
    extension: values.uploadedJobDescriptionExtension,
    parser: values.uploadedJobDescriptionParser,
  });
  state.tokenFields.titles.setTokens(values.titles);
  state.tokenFields.countries.setTokens(values.countries);
  state.tokenFields.continents.setTokens(values.continents);
  state.tokenFields.cities.setTokens(values.cities);
  state.tokenFields.companies.setTokens(values.companyTargets);
  state.tokenFields.mustHave.setTokens(values.mustHaveKeywords);
  state.tokenFields.niceToHave.setTokens(values.niceToHaveKeywords);
  state.tokenFields.industry.setTokens(values.industryKeywords);
  state.tokenFields.excludeTitles.setTokens(values.excludeTitles);
  state.tokenFields.excludeCompanies.setTokens(values.excludeCompanies);
  renderMemberPicker(values.assignedRecruiterIds.length ? values.assignedRecruiterIds : (state.user ? [state.user.id] : []));
  state.currentBreakdown = values.breakdown;
  renderBreakdown();
  renderAnchorGrid(values.anchors);
}

function renderProjectSummary() {
  const root = document.getElementById("project-summary");
  if (!state.selectedProject) {
    root.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>Select or Create a Project</h4>
        <p>Once you choose a project, its latest brief, results, and history will appear here.</p>
      </div>
    `;
    return;
  }
  const recruiters = state.selectedProject.assigned_recruiters || [];
  root.innerHTML = `
    <div class="project-summary-block">
      <div class="project-summary-row">
        <div>
          <h4>${escapeHtml(state.selectedProject.name)}</h4>
          <p class="muted">${escapeHtml(state.selectedProject.client_name || "No client set")} | ${escapeHtml(state.selectedProject.role_title || "No role title")}</p>
        </div>
        <span class="status-pill status-${escapeHtml(state.selectedProject.status)}">${escapeHtml(titleCaseWords(state.selectedProject.status))}</span>
      </div>
      <div class="chip-row">
        ${state.selectedProject.target_geography ? `<span class="info-chip">${escapeHtml(state.selectedProject.target_geography)}</span>` : ""}
        <span class="info-chip">${escapeHtml(String(state.selectedProject.run_count || 0))} Runs</span>
        <span class="info-chip">${escapeHtml(String(state.selectedProject.latest_run_candidate_count || 0))} Latest Candidates</span>
      </div>
      <p class="muted">${escapeHtml(state.selectedProject.notes || "No project notes yet.")}</p>
      <div class="stack-list compact-stack">
        ${recruiters.map((user) => `<div class="list-row"><strong>${escapeHtml(user.full_name || user.email)}</strong><span>${escapeHtml(user.email)}</span></div>`).join("")}
      </div>
        <div class="card-actions">
          <button type="button" class="button button-secondary" id="summary-open-recruiter">Open Hunt</button>
          <button type="button" class="button button-primary" id="summary-open-results">Open Results</button>
          <button type="button" class="button button-secondary" id="summary-open-candidates">Open Candidates</button>
        </div>
      </div>
    `;
    document.getElementById("summary-open-recruiter").addEventListener("click", () => switchTab("recruiter"));
    document.getElementById("summary-open-results").addEventListener("click", () => switchTab("results"));
    document.getElementById("summary-open-candidates").addEventListener("click", () => switchTab("candidates"));
  }

function projectCardMarkup(project, selected) {
  const runCount = safeNumber(project.run_count);
  const latestCandidates = safeNumber(project.latest_run_candidate_count);
  return `
    <button type="button" class="project-card ${selected ? "selected" : ""}" data-project-id="${escapeHtml(project.id)}">
      <div class="project-card-top">
        <div>
          <strong>${escapeHtml(project.name)}</strong>
          <p>${escapeHtml(project.client_name || "No client")} | ${escapeHtml(project.role_title || "No role")}</p>
        </div>
        <span class="status-pill status-${escapeHtml(project.status)}">${escapeHtml(titleCaseWords(project.status))}</span>
      </div>
      <div class="chip-row">
        ${project.target_geography ? `<span class="info-chip">${escapeHtml(project.target_geography)}</span>` : ""}
        <span class="info-chip">${runCount} Runs</span>
        <span class="info-chip">${latestCandidates} Latest Candidates</span>
      </div>
      <small class="muted">Updated ${escapeHtml(formatTimestamp(project.latest_run_at || project.updated_at))}</small>
    </button>
  `;
}

function renderProjectList() {
  const root = document.getElementById("project-list");
  if (!state.projects.length) {
    root.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Projects Yet</h4>
        <p>Create your first project to start saving recruiter briefs and search history.</p>
      </div>
    `;
    return;
  }
  root.innerHTML = state.projects.map((project) => projectCardMarkup(project, project.id === state.selectedProjectId)).join("");
  root.querySelectorAll("[data-project-id]").forEach((button) => {
    button.addEventListener("click", () => {
      loadProject(button.dataset.projectId);
      switchTab("projects");
    });
  });
}

function statusFromCandidate(candidate) {
  const normalized = String(candidate.verification_status || "").toLowerCase();
  if (normalized === "verified") return { key: "verified", label: "Verified" };
  if (normalized === "review" || normalized === "needs_review") return { key: "review", label: "Needs Review" };
  return { key: "reject", label: "Rejected" };
}

function jobProjectId(job) {
  return String(job?.payload?.project_id || job?.result?.project?.id || "").trim();
}

function isActiveJobStatus(status) {
  return ["queued", "running"].includes(String(status || "").toLowerCase());
}

function activeSearchJobForSelectedProject() {
  if (!state.activeJob || state.activeJob.job_type !== "search") {
    return null;
  }
  const projectId = jobProjectId(state.activeJob);
  if (!projectId || projectId !== state.selectedProjectId) {
    return null;
  }
  return isActiveJobStatus(state.activeJob.status) ? state.activeJob : null;
}

function latestSearchJobForSelectedProject() {
  if (!state.activeJob || state.activeJob.job_type !== "search") {
    return null;
  }
  const projectId = jobProjectId(state.activeJob);
  if (!projectId || projectId !== state.selectedProjectId) {
    return null;
  }
  return state.activeJob;
}

function failedSearchJobForSelectedProject() {
  const job = latestSearchJobForSelectedProject();
  return job && String(job.status || "").toLowerCase() === "failed" ? job : null;
}

function hasRunningBackgroundJob() {
  return Boolean(state.activeJob) && isActiveJobStatus(state.activeJob.status);
}

function backgroundJobExitMessage() {
  const jobType = titleCaseWords(state.activeJob?.job_type || "job").toLowerCase();
  return `A ${jobType} is still running in the background. Closing HR Hunter will not stop it, but you may lose live progress updates until you reopen the app.`;
}

function handleBeforeUnload(event) {
  if (!hasRunningBackgroundJob()) {
    return undefined;
  }
  const message = backgroundJobExitMessage();
  event.preventDefault();
  event.returnValue = message;
  return message;
}

function jobRequestedLimit(job) {
  return Math.max(
    1,
    safeNumber(job?.payload?.limit || job?.payload?.csv_export_limit || currentRequestedCandidateLimit(), currentRequestedCandidateLimit()),
  );
}

function parseTimestamp(value) {
  const timestamp = new Date(value || "");
  return Number.isNaN(timestamp.getTime()) ? null : timestamp;
}

function durationSecondsBetween(startValue, endValue) {
  const start = parseTimestamp(startValue);
  const end = parseTimestamp(endValue);
  if (!start || !end) return 0;
  return Math.max(0, (end.getTime() - start.getTime()) / 1000);
}

function formatDuration(seconds) {
  const totalSeconds = Math.max(0, Math.round(safeNumber(seconds, 0)));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainder = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${remainder}s`;
  }
  return `${remainder}s`;
}

function estimatedJobDurationSeconds(job, backendProgress = {}, elapsedSeconds = 0) {
  const requested = jobRequestedLimit(job);
  const stage = String(backendProgress.stage || job?.status || "").toLowerCase();
  if (job?.job_type === "train_ranker") {
    return Math.max(90, 45 + requested);
  }
  const backendEstimatedTotal = safeNumber(backendProgress.estimated_total_seconds, 0);
  if (backendEstimatedTotal > 0) {
    return Math.max(elapsedSeconds + (stage === "completed" ? 0 : 1), Math.round(backendEstimatedTotal));
  }
  const queriesTotal = safeNumber(backendProgress.queries_total, 0);
  const queriesCompleted = safeNumber(backendProgress.queries_completed, 0);
  const rawFound = safeNumber(backendProgress.raw_found, 0);
  const uniqueAfterDedupe = safeNumber(backendProgress.unique_after_dedupe, 0);
  const rerankedCount = safeNumber(backendProgress.reranked_count, 0);
  const rerankTarget = safeNumber(backendProgress.rerank_target, 0);
  const finalizedCount = safeNumber(backendProgress.finalized_count, 0);
  const explicitPercent = safeNumber(backendProgress.percent, NaN);
  if (stage === "completed") {
    return Math.max(1, Math.round(elapsedSeconds));
  }
  if (stage === "rerank") {
    if (elapsedSeconds > 5 && rerankTarget > 0 && rerankedCount > 0) {
      const rerankCoverage = Math.max(0.01, Math.min(0.995, rerankedCount / Math.max(1, rerankTarget)));
      const projectedTotal = elapsedSeconds / rerankCoverage;
      return Math.max(elapsedSeconds + 12, Math.min(4 * 3600, Math.round(projectedTotal)));
    }
    if (elapsedSeconds > 20 && rerankTarget > 0 && rerankedCount <= 0) {
      return Math.max(
        elapsedSeconds + 30,
        Math.max(
          240,
          Math.round(Math.min(4 * 3600, (rerankTarget * 1.25) + 120)),
        ),
      );
    }
  }
  if (stage === "finalizing" && elapsedSeconds > 5 && requested > 0 && finalizedCount > 0) {
    const finalCoverage = Math.max(0.01, Math.min(0.995, finalizedCount / Math.max(1, requested)));
    const projectedTotal = elapsedSeconds / finalCoverage;
    return Math.max(elapsedSeconds + 8, Math.min(4 * 3600, Math.round(projectedTotal)));
  }
  if (stage === "retrieval" && requested > 0 && elapsedSeconds > 5) {
    const queryCoverage = queriesTotal > 0 ? queriesCompleted / Math.max(1, queriesTotal) : 0;
    const uniqueCoverage = uniqueAfterDedupe > 0 ? uniqueAfterDedupe / Math.max(1, requested) : 0;
    const rawCoverage = rawFound > 0 ? rawFound / Math.max(1, requested * 1.2) : 0;
    const coverage = Math.max(queryCoverage, uniqueCoverage, rawCoverage);
    if (coverage >= 0.06) {
      const projectedRetrievalTotal = elapsedSeconds / Math.min(0.98, coverage);
      const retrievalTail = Math.max(25, Math.min(180, requested * 0.3));
      return Math.max(elapsedSeconds + 10, Math.min(4 * 3600, Math.round(projectedRetrievalTotal + retrievalTail)));
    }
  }
  if (stage === "retrieval" && queriesTotal > 0 && queriesCompleted >= 4 && elapsedSeconds > 3) {
    const remainingQueries = Math.max(0, queriesTotal - queriesCompleted);
    const perQuerySeconds = Math.max(0.1, elapsedSeconds / Math.max(1, queriesCompleted));
    const stageOverhead = Math.max(25, Math.min(220, requested * 0.4));
    const dynamicTotal = elapsedSeconds + (remainingQueries * perQuerySeconds) + stageOverhead;
    return Math.max(elapsedSeconds + 10, Math.min(4 * 3600, Math.round(dynamicTotal)));
  }
  const allowPercentProjection =
    !(stage === "rerank" && rerankTarget > 0 && rerankedCount <= 0)
    && !(stage === "finalizing" && finalizedCount <= 0);
  if (
    allowPercentProjection
    && Number.isFinite(explicitPercent)
    && explicitPercent >= 6
    && explicitPercent < 100
    && elapsedSeconds >= 6
  ) {
    const projectedTotal = elapsedSeconds / Math.max(0.06, Math.min(0.99, explicitPercent / 100));
    return Math.max(elapsedSeconds + 6, Math.min(4 * 3600, Math.round(projectedTotal)));
  }
  let estimate = queriesTotal > 0 ? Math.max(45, queriesTotal / 4.2) : 55 + requested * 1.2;
  estimate += Math.max(15, Math.min(90, requested * 0.12));
  if (job?.payload?.include_discovery_slices) estimate += Math.max(25, requested * 0.18);
  if (job?.payload?.include_history_slices) estimate += Math.max(15, requested * 0.1);
  if (job?.payload?.registry_memory_enabled) estimate += 15;
  if (job?.payload?.reranker_enabled) estimate += Math.max(30, Math.min(160, requested * 0.35));
  if (job?.payload?.learned_ranker_enabled) estimate += Math.max(10, requested * 0.08);
  return Math.max(90, Math.min(4 * 3600, Math.round(estimate)));
}

function jobProgressPayload(job) {
  if (!job || typeof job.progress !== "object" || Array.isArray(job.progress)) {
    return {};
  }
  return job.progress;
}

function formatCounterValue(value) {
  const number = safeNumber(value, 0);
  if (!Number.isFinite(number)) return "0";
  return String(Math.max(0, Math.round(number)));
}

function runningJobProgress(job) {
  const backendProgress = jobProgressPayload(job);
  const createdAt = parseTimestamp(job?.started_at || job?.created_at);
  const elapsedFromClock = createdAt ? Math.max(0, (Date.now() - createdAt.getTime()) / 1000) : 0;
  const backendElapsed = safeNumber(backendProgress.elapsed_seconds, 0);
  const elapsedSeconds = Math.max(elapsedFromClock, backendElapsed);
  const status = String(job?.status || "").toLowerCase();
  const stage = String(backendProgress.stage || status || "queued").toLowerCase();
  const lastProgressAt = parseTimestamp(backendProgress.updated_at || job?.heartbeat_at || job?.started_at || job?.created_at);
  const stalledForSeconds = lastProgressAt ? Math.max(0, (Date.now() - lastProgressAt.getTime()) / 1000) : 0;
  const rerankedCount = safeNumber(backendProgress.reranked_count, 0);
  const rerankTargetValue = safeNumber(backendProgress.rerank_target, 0);
  const rerankTarget = rerankTargetValue > 0 ? rerankTargetValue : 0;
  const finalizedCount = safeNumber(backendProgress.finalized_count, 0);
  const explicitPercent = safeNumber(backendProgress.percent, NaN);
  const explicitPercentUsable = Number.isFinite(explicitPercent)
    && !((stage === "rerank" && rerankTarget > 0 && rerankedCount <= 0)
      || (stage === "finalizing" && finalizedCount <= 0));
  let estimatedSeconds = estimatedJobDurationSeconds(job, backendProgress, elapsedSeconds);
  const canProjectFromPercent =
    explicitPercentUsable
    && explicitPercent >= 4
    && explicitPercent < 100
    && elapsedSeconds >= 8
    && !(
      stalledForSeconds >= 20
      && (stage === "rerank" || stage === "finalizing" || stage === "retrieval")
    );
  if (canProjectFromPercent) {
    const percentRatio = Math.max(0.04, Math.min(0.99, explicitPercent / 100));
    const projectedTotalFromPercent = elapsedSeconds / percentRatio;
    estimatedSeconds = Math.round((estimatedSeconds * 0.55) + (projectedTotalFromPercent * 0.45));
  }
  estimatedSeconds = Math.max(elapsedSeconds + (status === "completed" ? 0 : 2), estimatedSeconds);
  const rawPercent = estimatedSeconds > 0 ? (elapsedSeconds / estimatedSeconds) * 100 : 0;
  const elapsedFloorPercent = status === "queued" ? 4 : Math.max(3, Math.min(97, Math.round(rawPercent)));
  let progressPercent = explicitPercentUsable
    ? Math.max(1, Math.min(99, Math.round(explicitPercent)))
    : elapsedFloorPercent;
  if (stage === "rerank" && rerankTarget > 0 && rerankedCount <= 0) {
    progressPercent = Math.min(progressPercent, 88);
  }
  if (stage === "rerank" && rerankTarget > 0 && rerankedCount > 0) {
    const rerankPercent = 80 + Math.min(15, Math.round((rerankedCount / rerankTarget) * 15));
    progressPercent = Math.max(progressPercent, rerankPercent);
  }
  if (stage === "finalizing" && progressPercent < 95) {
    progressPercent = 95;
  }
  const queriesCompleted = safeNumber(backendProgress.queries_completed, 0);
  const queriesTotal = safeNumber(backendProgress.queries_total, 0);
  const queriesInFlight = stage === "rerank" || stage === "finalizing"
    ? 0
    : safeNumber(backendProgress.queries_in_flight, 0);
  const rawFound = safeNumber(backendProgress.raw_found, 0);
  const uniqueAfterDedupe = safeNumber(backendProgress.unique_after_dedupe, 0);
  const stageElapsedRaw = safeNumber(backendProgress.stage_elapsed_seconds, 0);
  const liveStageElapsed = isActiveJobStatus(status)
    ? stageElapsedRaw + stalledForSeconds
    : stageElapsedRaw;
  const stageElapsedFallback = ["completed", "failed"].includes(status) ? 0 : elapsedSeconds;
  const stageElapsedSeconds = Math.max(
    0,
    Math.min(
      elapsedSeconds,
      Math.round(liveStageElapsed > 0 ? liveStageElapsed : stageElapsedFallback),
    ),
  );
  const stageLabel = String(backendProgress.stage_label || titleCaseWords(stage || "queued"));
  const message = String(backendProgress.message || "")
    .replace(/\(\s*\d+\s*s\s*elapsed\s*\)\.?/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
  const etaKnown = !(stage === "rerank" && rerankTarget > 0 && rerankedCount <= 0);
  const displayedEstimatedSeconds = etaKnown ? estimatedSeconds : 0;
  const remainingSeconds = etaKnown ? Math.max(0, displayedEstimatedSeconds - elapsedSeconds) : 0;
  return {
    elapsedSeconds,
    estimatedSeconds: displayedEstimatedSeconds,
    remainingSeconds,
    etaKnown,
    progressPercent,
    statusLabel: titleCaseWords(status || "queued"),
    stage,
    stageLabel,
    stageElapsedSeconds,
    message,
    queriesCompleted,
    queriesTotal,
    queriesInFlight,
    rawFound,
    uniqueAfterDedupe,
    rerankedCount,
    rerankTarget,
    finalizedCount,
    stalledForSeconds,
    requested: jobRequestedLimit(job),
  };
}

function latestCompletedSearchJobForCurrentReport() {
  const job = latestSearchJobForSelectedProject();
  if (!job || String(job?.job_type || "").toLowerCase() !== "search") {
    return null;
  }
  if (String(job?.status || "").toLowerCase() !== "completed") {
    return null;
  }
  const activeRunId = String(state.currentReport?.run_id || "").trim();
  const jobRunId = String(job?.result?.run_id || "").trim();
  if (activeRunId && jobRunId && activeRunId !== jobRunId) {
    return null;
  }
  return job;
}

function resultsRunTiming() {
  const completedJob = latestCompletedSearchJobForCurrentReport();
  if (completedJob) {
    const runtimeSeconds = durationSecondsBetween(
      completedJob?.started_at || completedJob?.created_at,
      completedJob?.finished_at || state.currentReport?.generated_at,
    );
    const estimatedSeconds = estimatedJobDurationSeconds(
      completedJob,
      jobProgressPayload(completedJob),
      runtimeSeconds,
    );
    return {
      source: "completed",
      runtimeSeconds,
      estimatedSeconds: Math.max(runtimeSeconds, estimatedSeconds),
    };
  }
  const activeJob = activeSearchJobForSelectedProject();
  if (activeJob) {
    const progress = runningJobProgress(activeJob);
    return {
      source: "active",
      runtimeSeconds: progress.elapsedSeconds,
      estimatedSeconds: progress.estimatedSeconds,
    };
  }
  return {
    source: "none",
    runtimeSeconds: 0,
    estimatedSeconds: 0,
  };
}

function runningJobMarkup(job, options = {}) {
  const progress = runningJobProgress(job);
  const compact = Boolean(options.compact);
  const heading = options.heading || `${progress.statusLabel} Search`;
  const lead = options.lead || `Requested up to ${progress.requested} candidates.`;
  const note = options.note || "Live stage telemetry from the running backend job.";
  const queryCounter = progress.queriesTotal > 0
    ? `${formatCounterValue(progress.queriesCompleted)} / ${formatCounterValue(progress.queriesTotal)}`
    : formatCounterValue(progress.queriesCompleted);
  let rerankedCounter = progress.rerankTarget > 0
    ? `${formatCounterValue(progress.rerankedCount)} / ${formatCounterValue(progress.rerankTarget)}`
    : formatCounterValue(progress.rerankedCount);
  if (progress.stage === "rerank" && progress.rerankTarget > 0 && progress.rerankedCount <= 0) {
    rerankedCounter = `Processing (${formatCounterValue(progress.rerankTarget)} target)`;
  }
  const finalizedCounter = progress.finalizedCount > 0
    ? formatCounterValue(progress.finalizedCount)
    : (progress.stage === "completed" || progress.stage === "failed" ? "0" : "Pending");
  const etaValue = progress.etaKnown ? formatDuration(progress.remainingSeconds) : "Calculating...";
  const estimatedTotalValue = progress.etaKnown ? formatDuration(progress.estimatedSeconds) : "Calculating...";
  const stallNote = progress.stage === "retrieval" && progress.stalledForSeconds >= 20
    ? `No fresh completion for ${formatDuration(progress.stalledForSeconds)}. Still waiting on in-flight retrieval requests.`
    : "";
  return `
    <div class="job-progress-card ${compact ? "job-progress-card-compact" : ""}">
      <div class="job-progress-head">
        <div>
          <h4>${escapeHtml(heading)}</h4>
          <p>${escapeHtml(lead)}</p>
        </div>
        <div class="job-progress-badge">${escapeHtml(progress.statusLabel)}</div>
      </div>
      <div class="job-progress-bar">
        <span style="width:${escapeHtml(String(progress.progressPercent))}%"></span>
      </div>
      <div class="job-progress-timing">
        <span>
          <small>Elapsed</small>
          <strong>${escapeHtml(formatDuration(progress.elapsedSeconds))}</strong>
        </span>
        <span>
          <small>Stage Elapsed</small>
          <strong>${escapeHtml(formatDuration(progress.stageElapsedSeconds || 0))}</strong>
        </span>
        <span>
          <small>ETA</small>
          <strong>${escapeHtml(etaValue)}</strong>
        </span>
        <span>
          <small>Estimated Total</small>
          <strong>${escapeHtml(estimatedTotalValue)}</strong>
        </span>
      </div>
      <div class="job-progress-meta">
        <span><strong>Stage</strong> ${escapeHtml(progress.stageLabel)}</span>
        <span><strong>Progress</strong> ${escapeHtml(String(progress.progressPercent))}%</span>
        <span><strong>Target</strong> ${escapeHtml(String(progress.requested))} candidates</span>
      </div>
      <div class="job-progress-counters">
        <span><strong>Queries</strong> ${escapeHtml(queryCounter)}</span>
        <span><strong>In Flight</strong> ${escapeHtml(formatCounterValue(progress.queriesInFlight))}</span>
        <span><strong>Raw Found</strong> ${escapeHtml(formatCounterValue(progress.rawFound))}</span>
        <span><strong>Unique</strong> ${escapeHtml(formatCounterValue(progress.uniqueAfterDedupe))}</span>
        <span><strong>Reranked</strong> ${escapeHtml(rerankedCounter)}</span>
        <span><strong>Finalized</strong> ${escapeHtml(finalizedCounter)}</span>
      </div>
      ${progress.message ? `<p class="muted small">${escapeHtml(progress.message)}</p>` : ""}
      ${stallNote ? `<p class="muted small">${escapeHtml(stallNote)}</p>` : ""}
      <p class="muted small">${escapeHtml(note)}</p>
    </div>
  `;
}

function renderStatusJobPanel(job) {
  const panel = document.getElementById("status-job-panel");
  if (!panel) return;
  if (!job || !["queued", "running"].includes(String(job.status || "").toLowerCase())) {
    panel.hidden = true;
    panel.innerHTML = "";
    return;
  }
  panel.hidden = false;
  panel.innerHTML = runningJobMarkup(job, {
    compact: true,
    heading: `${titleCaseWords(job.status || "queued")} Search`,
    lead: `Requested up to ${jobRequestedLimit(job)} candidates for ${state.selectedProject?.role_title || state.selectedProject?.name || "the selected project"}.`,
    note: "The search is still working in the background. Larger runs with discovery and ranking enabled can take longer.",
  });
}

function renderActiveJobTabPanels() {
  if (state.activeTab === "results") {
    renderResults();
    return;
  }
  if (state.activeTab === "candidates") {
    renderCandidates();
  }
}

function syncLiveJobStatus() {
  const activeJob = activeSearchJobForSelectedProject();
  if (!activeJob) {
    renderStatusJobPanel(null);
    return false;
  }
  const running = runningJobProgress(activeJob);
  const tone = String(activeJob.status || "").toLowerCase() === "queued" ? "default" : "warning";
  const liveLine = running.message
    ? running.message
    : `${running.stageLabel}: ${formatCounterValue(running.queriesCompleted)} / ${formatCounterValue(running.queriesTotal)} queries, ${formatCounterValue(running.uniqueAfterDedupe)} unique.`;
  setStatus(
    `Search ${String(activeJob.status || "").toLowerCase() === "queued" ? "queued" : "running"}.`,
    tone,
    `${running.statusLabel} for up to ${running.requested} candidates. ${liveLine} Elapsed ${formatDuration(running.elapsedSeconds)}.`,
  );
  renderStatusJobPanel(activeJob);
  return true;
}

function searchFailureMarkup(job, buttonId) {
  return `
    <div class="empty-state compact-empty">
      <h4>Latest Search Failed</h4>
      <p>${escapeHtml(job?.error || "The latest background search did not complete successfully.")}</p>
      <div class="inline-actions">
        <button type="button" class="button button-secondary" id="${buttonId}">Retry Search</button>
      </div>
    </div>
  `;
}

function currentCandidates() {
  return Array.isArray(state.currentReport?.candidates) ? state.currentReport.candidates : [];
}

function candidateLocationLabel(candidate) {
  const location = String(candidate?.location_name || "").trim();
  return location || "Unknown Location";
}

function candidateLocationBuckets() {
  const counts = new Map();
  currentCandidates().forEach((candidate) => {
    const location = candidateLocationLabel(candidate);
    counts.set(location, (counts.get(location) || 0) + 1);
  });
  return Array.from(counts.entries())
    .map(([location, count]) => ({ location, count }))
    .sort((left, right) => (right.count - left.count) || left.location.localeCompare(right.location));
}

function currentRequestedCandidateLimit() {
  const formLimit = document.getElementById("candidate-limit")?.value;
  const savedLimit =
    state.selectedProject?.latest_brief_json?.ui_meta?.candidate_limit
    ?? state.selectedProject?.latest_brief_json?.max_profiles
    ?? state.settings.limit;
  return Math.max(1, safeNumber(formLimit || savedLimit, state.settings.limit || 20));
}

function filteredCandidates() {
  const query = String(state.candidateSearchQuery || "").trim().toLowerCase();
  const locationFilter = String(state.candidateLocationFilter || "all");
  return currentCandidates().filter((candidate) => {
    const status = statusFromCandidate(candidate).key;
    if (state.candidateStatusFilter !== "all" && status !== state.candidateStatusFilter) {
      return false;
    }
    const candidateLocation = candidateLocationLabel(candidate);
    if (locationFilter !== "all" && candidateLocation !== locationFilter) {
      return false;
    }
    if (!query) {
      return true;
    }
    const haystack = [
      candidate.full_name,
      candidate.current_title,
      candidate.current_company,
      candidate.location_name,
      candidate.summary,
    ]
      .filter(Boolean)
      .join(" | ")
      .toLowerCase();
    return haystack.includes(query);
  });
}

function bucketCountFor(statusKey) {
  return currentCandidates().filter((candidate) => statusFromCandidate(candidate).key === statusKey).length;
}

function candidateIdentityRef(candidate) {
  return candidate.linkedin_url || candidate.source_url || candidate.full_name || "";
}

function candidateSignalEntries(candidate) {
  const entries = Object.entries(candidate.feature_scores || {})
    .filter(([key, value]) => safeNumber(value) > 0.12 && key !== "company_interest")
    .map(([key, value]) => ({
      key,
      label: FEATURE_LABELS[key] || titleCaseWords(key),
      value: safeNumber(value),
    }))
    .sort((left, right) => right.value - left.value);
  if (safeNumber(candidate.reranker_score) > 0.12) {
    entries.push({
      key: "reranker",
      label: "Semantic Reranker",
      value: safeNumber(candidate.reranker_score),
    });
  }
  return entries.slice(0, 4);
}

function candidateInsightBuckets(candidate) {
  const notes = Array.isArray(candidate.verification_notes) ? candidate.verification_notes.map(humanizeNote).filter(Boolean) : [];
  const positives = notes.filter((note) => !/missing|cap|reject|no public|weak/i.test(note)).slice(0, 3);
  const gaps = notes.filter((note) => /missing|no public|weak|cap/i.test(note)).slice(0, 2);
  if (!positives.length) {
    const matchedTitles = Array.isArray(candidate.matched_titles) ? candidate.matched_titles : [];
    if (matchedTitles.length) {
      positives.push(`Matched title: ${matchedTitles[0]}`);
    }
  }
  return { positives, gaps };
}

function compactFeatureSummary(candidate) {
  const signals = candidateSignalEntries(candidate);
  if (!signals.length) {
    return `<p class="muted">Public evidence was limited, so the score leaned more on the overall reranker and parsed profile quality.</p>`;
  }
  return `
    <div class="chip-row">
      ${signals
        .map(
          (signal) => `<span class="info-chip"><strong>${escapeHtml(signal.label)}</strong> ${escapeHtml(formatScore(signal.value))}</span>`,
        )
        .join("")}
    </div>
  `;
}

function qualityIssueTone(severity) {
  const normalized = String(severity || "").toLowerCase();
  if (normalized === "high") return "quality-issue-high";
  if (normalized === "medium") return "quality-issue-medium";
  return "quality-issue-watch";
}

function qualityDiagnosticsMarkup(summary) {
  const diagnostics = summary?.quality_diagnostics;
  if (!diagnostics?.enabled) {
    return "";
  }
  const issues = Array.isArray(diagnostics.issues) ? diagnostics.issues : [];
  const issuesMarkup = issues.length
    ? issues.map((issue) => `
        <article class="quality-issue ${qualityIssueTone(issue.severity)}">
          <div class="quality-issue-head">
            <strong>${escapeHtml(issue.label || "Diagnostic")}</strong>
            <span>${escapeHtml(String(issue.count || 0))} candidates | ${escapeHtml(formatPercent(issue.share || 0))}</span>
          </div>
          <p>${escapeHtml(issue.message || "")}</p>
          <p class="muted small"><strong>How to improve:</strong> ${escapeHtml(issue.action || "")}</p>
        </article>
      `).join("")
    : `<p class="muted small">No major quality blockers were detected in the latest candidate set.</p>`;
  return `
    <section class="quality-diagnostics-card">
      <div class="quality-diagnostics-head">
        <div>
          <p class="eyebrow">Quality Diagnostics</p>
          <h4>${escapeHtml(diagnostics.headline || "Candidate quality diagnostics")}</h4>
        </div>
        <div class="quality-diagnostics-metrics">
          <span class="info-chip"><strong>Verified Yield</strong> ${escapeHtml(formatPercent(diagnostics.verified_rate || 0))}</span>
          <span class="info-chip"><strong>Verified</strong> ${escapeHtml(String(diagnostics.verified_count || 0))}</span>
          <span class="info-chip"><strong>Unique</strong> ${escapeHtml(String(diagnostics.unique_after_dedupe || 0))}</span>
          <span class="info-chip"><strong>Raw</strong> ${escapeHtml(String(diagnostics.raw_found || 0))}</span>
        </div>
      </div>
      <div class="quality-issue-grid">
        ${issuesMarkup}
      </div>
    </section>
  `;
}

function renderResultsSummary() {
  const root = document.getElementById("results-summary");
  const activeJob = activeSearchJobForSelectedProject();
  const failedJob = failedSearchJobForSelectedProject();
  if (!state.selectedProject) {
    root.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>Select or Create a Project</h4>
        <p>Choose a project first, then run or load a search to see the summary.</p>
      </div>
    `;
    return;
  }
  if (!state.currentReport) {
    if (failedJob) {
      root.innerHTML = searchFailureMarkup(failedJob, "results-retry-search");
      document.getElementById("results-retry-search")?.addEventListener("click", retrySearchForSelectedProject);
      return;
    }
    if (activeJob) {
      const live = runningJobProgress(activeJob);
      const queryCounter = live.queriesTotal > 0
        ? `${formatCounterValue(live.queriesCompleted)} / ${formatCounterValue(live.queriesTotal)}`
        : formatCounterValue(live.queriesCompleted);
      const etaText = live.etaKnown ? formatDuration(live.remainingSeconds) : "Calculating...";
      root.innerHTML = `
        <div class="empty-state compact-empty">
          <h4>Search In Progress</h4>
          <p>The ${escapeHtml(titleCaseWords(activeJob.status))} search is running for up to ${escapeHtml(String(jobRequestedLimit(activeJob)))} candidates.</p>
          <p class="muted">Stage: ${escapeHtml(live.stageLabel)} | Progress: ${escapeHtml(String(live.progressPercent))}% | Queries: ${escapeHtml(queryCounter)}</p>
          <p class="muted">Elapsed: ${escapeHtml(formatDuration(live.elapsedSeconds))} | ETA: ${escapeHtml(etaText)}</p>
        </div>
      `;
      return;
    }
    root.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Results Yet</h4>
        <p>Run a search for <strong>${escapeHtml(state.selectedProject.name)}</strong> to populate the current results.</p>
      </div>
    `;
    return;
  }
  const summary = state.currentReport.summary || {};
  const cards = [
    { label: "Candidates", value: safeNumber(summary.candidate_count, state.currentReport.candidates?.length || 0) },
    { label: "Verified", value: safeNumber(summary.verified_count) },
    { label: "Needs Review", value: safeNumber(summary.review_count) },
    { label: "Rejected", value: safeNumber(summary.reject_count) },
  ];
  const timing = resultsRunTiming();
  const finalRuntime = timing.runtimeSeconds > 0 ? formatDuration(timing.runtimeSeconds) : "Not available";
  const estimatedRuntime = timing.estimatedSeconds > 0 ? formatDuration(timing.estimatedSeconds) : "Not available";
  const topLocationBuckets = candidateLocationBuckets().slice(0, 8);
  const topLocationsMarkup = topLocationBuckets.length
    ? `
      <div class="summary-location-row">
        <p class="muted small">Top Locations</p>
        <div class="chip-row">
          ${topLocationBuckets
            .map((bucket) => `<span class="info-chip"><strong>${escapeHtml(bucket.location)}</strong> ${escapeHtml(String(bucket.count))}</span>`)
            .join("")}
        </div>
      </div>
    `
    : "";
  const timingNote =
    timing.source === "completed"
      ? "Timing from the latest completed run."
      : timing.source === "active"
        ? "Live timing from the active running search."
        : "Timing will appear after search telemetry is available.";
  const diagnosticsMarkup = qualityDiagnosticsMarkup(summary);
  root.innerHTML = `
        <div class="summary-cards">
      ${cards
        .map(
          (card) => `
          <div class="summary-card">
            <span>${escapeHtml(card.label)}</span>
            <strong>${escapeHtml(String(card.value))}</strong>
          </div>
        `,
        )
        .join("")}
        </div>
        <div class="summary-timing-grid">
          <div class="summary-timing-card">
            <span>Final Runtime</span>
            <strong>${escapeHtml(finalRuntime)}</strong>
          </div>
          <div class="summary-timing-card">
            <span>Estimated Time</span>
            <strong>${escapeHtml(estimatedRuntime)}</strong>
          </div>
        </div>
        ${topLocationsMarkup}
        ${diagnosticsMarkup}
        <p class="muted small">${escapeHtml(timingNote)}</p>
        <p class="muted">Latest run: ${escapeHtml(formatTimestamp(state.currentReport.generated_at))} for ${escapeHtml(state.selectedProject.name)}.</p>
        ${activeJob ? `<p class="muted">A newer search is currently ${escapeHtml(titleCaseWords(activeJob.status))}. Requested limit: ${escapeHtml(String(activeJob.payload?.limit || currentRequestedCandidateLimit()))} candidates.</p>` : ""}
        ${failedJob ? `<p class="muted">The latest search failed: ${escapeHtml(failedJob.error || "Retry when ready.")}</p><div class="inline-actions"><button type="button" class="button button-secondary" id="results-retry-search">Retry Search</button></div>` : ""}
      `;
  document.getElementById("results-retry-search")?.addEventListener("click", retrySearchForSelectedProject);
}

function renderCsvSummary() {
  const root = document.getElementById("csv-summary");
  const reportPaths = state.currentReport?.report_paths || {};
  if (!state.selectedProject || !reportPaths.csv) {
    root.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No CSV Yet</h4>
        <p>Run a search to generate a CSV for the selected project.</p>
      </div>
    `;
    return;
  }
  const csvFileName = String(reportPaths.csv).split(/[\\/]/).pop() || reportPaths.csv;
  root.innerHTML = `
      <p><a class="inline-link" href="${artifactHref(reportPaths.csv)}">Download CSV</a></p>
      <p class="muted">Requested CSV rows: ${escapeHtml(String(currentRequestedCandidateLimit()))}</p>
      <small class="muted">${escapeHtml(csvFileName)}</small>
    `;
}

function feedbackFormMarkup(candidate) {
  const actionOptions = (state.config?.feedback_actions || []).map(
    (action) => `<option value="${escapeHtml(action)}">${escapeHtml(titleCaseWords(action))}</option>`,
  );
  const candidateRef = candidateIdentityRef(candidate);
  return `
    <form class="feedback-form" data-feedback-ref="${escapeHtml(candidateRef)}">
      <select data-feedback-action>
        ${actionOptions.join("")}
      </select>
      <input type="text" data-feedback-reason placeholder="Reason code (optional)" />
      <input type="text" data-feedback-note placeholder="Recruiter note (optional)" />
      <button type="submit" class="button button-primary button-small">Save Feedback</button>
    </form>
  `;
}

function candidateCardMarkup(candidate) {
  const status = statusFromCandidate(candidate);
  const signals = compactFeatureSummary(candidate);
  const insights = candidateInsightBuckets(candidate);
  const links = [];
  if (candidate.linkedin_url) links.push(`<a class="inline-link" href="${escapeHtml(candidate.linkedin_url)}" target="_blank" rel="noreferrer">LinkedIn</a>`);
  if (candidate.source_url) links.push(`<a class="inline-link" href="${escapeHtml(candidate.source_url)}" target="_blank" rel="noreferrer">Source</a>`);
  const registrySeen = safeNumber(candidate.raw?.registry?.search_count);
  return `
    <article class="candidate-card">
      <div class="candidate-top">
        <div>
          <div class="candidate-name-row">
            <h4>${escapeHtml(candidate.full_name || "Unnamed Candidate")}</h4>
            <span class="status-pill status-${escapeHtml(status.key)}">${escapeHtml(status.label)}</span>
          </div>
          <p class="candidate-subtitle">${escapeHtml(candidate.current_title || "No title")} | ${escapeHtml(candidate.current_company || "No company")} | ${escapeHtml(candidate.location_name || "No location")}</p>
          <div class="candidate-meta">
            ${links.join("<span class=\"divider\">|</span>")}
            ${registrySeen > 1 ? `<span class="muted">Seen in ${registrySeen} runs</span>` : `<span class="muted">New to the registry</span>`}
          </div>
        </div>
        <div class="candidate-score">
          <strong>${escapeHtml(formatScore(candidate.score))}</strong>
        </div>
      </div>
      ${signals}
      ${insights.positives.length ? `<p class="candidate-notes"><strong>Why it matched:</strong> ${escapeHtml(insights.positives.join(" | "))}</p>` : ""}
      ${insights.gaps.length ? `<p class="candidate-notes muted"><strong>Evidence gaps:</strong> ${escapeHtml(insights.gaps.join(" | "))}</p>` : ""}
      ${feedbackFormMarkup(candidate)}
    </article>
  `;
}

function candidateTableRowMarkup(candidate, selectedRef) {
  const status = statusFromCandidate(candidate);
  const candidateRef = candidateIdentityRef(candidate);
  const qualification = titleCaseWords(candidate.qualification_tier || "unclassified");
  const links = [];
  if (candidate.linkedin_url) {
    links.push(`<a class="inline-link" href="${escapeHtml(candidate.linkedin_url)}" target="_blank" rel="noreferrer">LinkedIn</a>`);
  }
  if (candidate.source_url) {
    links.push(`<a class="inline-link" href="${escapeHtml(candidate.source_url)}" target="_blank" rel="noreferrer">Source</a>`);
  }
  return `
    <tr class="candidate-table-row ${candidateRef === selectedRef ? "selected" : ""}" data-candidate-row="${escapeHtml(candidateRef)}">
      <td>
        <div class="candidate-primary">
          <strong>${escapeHtml(candidate.full_name || "Unnamed Candidate")}</strong>
          <span class="candidate-secondary">${escapeHtml(candidate.location_name || "No location")}</span>
        </div>
      </td>
      <td>${escapeHtml(candidate.current_title || "No title")}</td>
      <td>${escapeHtml(candidate.current_company || "No company")}</td>
      <td class="candidate-score-cell">${escapeHtml(formatScore(candidate.score))}</td>
      <td><span class="status-pill status-${escapeHtml(status.key)}">${escapeHtml(status.label)}</span></td>
      <td>${escapeHtml(qualification)}</td>
      <td>
        <div class="candidate-links-cell">
          ${links.length ? links.join("") : `<span class="muted">No links</span>`}
        </div>
      </td>
    </tr>
  `;
}

function renderCandidatesSummary() {
  const root = document.getElementById("candidates-summary");
  const candidates = currentCandidates();
  const activeJob = activeSearchJobForSelectedProject();
  const failedJob = failedSearchJobForSelectedProject();
  if (!state.selectedProject) {
    root.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>Select or Create a Project</h4>
        <p>Choose a project first to browse its latest candidate set.</p>
      </div>
    `;
    return;
  }
  if (!candidates.length) {
    if (failedJob) {
      root.innerHTML = searchFailureMarkup(failedJob, "candidates-retry-search");
      document.getElementById("candidates-retry-search")?.addEventListener("click", retrySearchForSelectedProject);
      return;
    }
    root.innerHTML = `
        ${activeJob
          ? runningJobMarkup(activeJob, {
            heading: "Candidate Search In Progress",
            lead: `The ${titleCaseWords(activeJob.status)} search requested up to ${jobRequestedLimit(activeJob)} candidates.`,
          })
          : `<div class="empty-state compact-empty">
          <h4>No Candidates Yet</h4>
          <p>Run a search for ${escapeHtml(state.selectedProject.name)} to populate the candidate list.</p>
        </div>`}
      `;
      return;
    }
    const summary = state.currentReport?.summary || {};
    const requested = currentRequestedCandidateLimit();
  const cards = [
    { label: "Returned", value: candidates.length },
    { label: "Verified", value: bucketCountFor("verified") },
    { label: "Needs Review", value: bucketCountFor("review") },
    { label: "Rejected", value: bucketCountFor("reject") },
  ];
  root.innerHTML = `
    <div class="summary-cards">
      ${cards
        .map(
          (card) => `
            <div class="summary-card">
              <span>${escapeHtml(card.label)}</span>
              <strong>${escapeHtml(String(card.value))}</strong>
            </div>
          `,
        )
        .join("")}
    </div>
    <div class="candidate-table-meta">
      <p class="muted">Latest run: ${escapeHtml(formatTimestamp(state.currentReport.generated_at))}</p>
      <p class="muted">Requested up to ${escapeHtml(String(requested))} candidates | Returned ${escapeHtml(String(candidates.length))}</p>
    </div>
    ${activeJob ? `<p class="muted">A newer search is still ${escapeHtml(titleCaseWords(activeJob.status))}. The list below shows the latest completed run until the fresh one finishes.</p>` : ""}
    ${failedJob ? `<p class="muted">The latest search failed: ${escapeHtml(failedJob.error || "Retry when ready.")}</p><div class="inline-actions"><button type="button" class="button button-secondary" id="candidates-retry-search">Retry Search</button></div>` : ""}
    ${summary.slice_count ? `<p class="muted">Retrieval slices used: ${escapeHtml(String(summary.slice_count))}</p>` : ""}
  `;
  document.getElementById("candidates-retry-search")?.addEventListener("click", retrySearchForSelectedProject);
}

function renderCandidateControls() {
  const pillRoot = document.getElementById("candidate-filter-pills");
  const exportRoot = document.getElementById("candidate-export-actions");
  const exportNote = document.getElementById("candidate-export-note");
  const locationSelect = document.getElementById("candidate-location-filter");
  const locationNote = document.getElementById("candidate-location-note");
  const buckets = [
    { id: "all", label: "All", count: currentCandidates().length },
    { id: "verified", label: "Verified", count: bucketCountFor("verified") },
    { id: "review", label: "Needs Review", count: bucketCountFor("review") },
    { id: "reject", label: "Rejected", count: bucketCountFor("reject") },
  ];
  pillRoot.innerHTML = buckets
    .map(
      (bucket) => `
        <button type="button" class="filter-pill ${state.candidateStatusFilter === bucket.id ? "active" : ""}" data-candidate-bucket="${escapeHtml(bucket.id)}">
          ${escapeHtml(bucket.label)} (${escapeHtml(String(bucket.count))})
        </button>
      `,
    )
    .join("");
  const locationBuckets = candidateLocationBuckets();
  if (locationSelect) {
    const hasSelectedLocation =
      state.candidateLocationFilter === "all"
      || locationBuckets.some((bucket) => bucket.location === state.candidateLocationFilter);
    if (!hasSelectedLocation) {
      state.candidateLocationFilter = "all";
    }
    locationSelect.innerHTML = [
      `<option value="all">All Locations (${currentCandidates().length})</option>`,
      ...locationBuckets.map(
        (bucket) => `<option value="${escapeHtml(bucket.location)}">${escapeHtml(`${bucket.location} (${bucket.count})`)}</option>`,
      ),
    ].join("");
    locationSelect.value = state.candidateLocationFilter;
    locationSelect.onchange = () => {
      state.candidateLocationFilter = locationSelect.value || "all";
      renderCandidates();
    };
  }
  if (locationNote) {
    if (!currentCandidates().length) {
      locationNote.textContent = "Filter by location from the latest candidate set.";
    } else if (state.candidateLocationFilter === "all") {
      locationNote.textContent = `${locationBuckets.length} locations in this run.`;
    } else {
      const selected = locationBuckets.find((bucket) => bucket.location === state.candidateLocationFilter);
      locationNote.textContent = `${selected?.count || 0} candidates in ${state.candidateLocationFilter}.`;
    }
  }
  const csvPath = state.currentReport?.report_paths?.csv || "";
  if (csvPath) {
    exportRoot.innerHTML = `<a class="button button-secondary" href="${artifactHref(csvPath)}">Export CSV</a>`;
    exportNote.textContent = `The latest export is set to include up to ${currentRequestedCandidateLimit()} rows.`;
  } else {
    exportRoot.innerHTML = `<span class="muted">No CSV available yet.</span>`;
    exportNote.textContent = "Run a search to generate the latest project CSV.";
  }
  pillRoot.querySelectorAll("[data-candidate-bucket]").forEach((button) => {
    button.addEventListener("click", () => {
      state.candidateStatusFilter = button.dataset.candidateBucket || "all";
      renderCandidates();
    });
  });
}

function renderCandidates() {
  const searchInput = document.getElementById("candidate-search-input");
  if (searchInput && searchInput.value !== state.candidateSearchQuery) {
    searchInput.value = state.candidateSearchQuery;
  }
  renderCandidatesSummary();
  renderCandidateControls();
  const tableRoot = document.getElementById("candidate-table-container");
  const detailRoot = document.getElementById("candidate-detail");
  const candidates = filteredCandidates();
  if (!state.selectedProject) {
    tableRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>Select or Create a Project</h4>
        <p>Choose a project first to view its candidate list.</p>
      </div>
    `;
    detailRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Candidate Selected</h4>
        <p>Select a project first, then open a saved run.</p>
      </div>
    `;
    return;
  }
  if (!currentCandidates().length) {
    tableRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Candidates Yet</h4>
        <p>Run a search for <strong>${escapeHtml(state.selectedProject.name)}</strong> to populate the candidate directory.</p>
      </div>
    `;
    detailRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Candidate Selected</h4>
        <p>The latest candidate detail will appear here after a run completes.</p>
      </div>
    `;
    return;
  }
  if (!candidates.length) {
    tableRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Matches For This Filter</h4>
        <p>Try a different review bucket, location, or clear the candidate search field.</p>
      </div>
    `;
    detailRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Candidate Selected</h4>
        <p>The current filter returned no candidates.</p>
      </div>
    `;
    return;
  }
  const selectedRef = candidates.some((candidate) => candidateIdentityRef(candidate) === state.selectedCandidateRef)
    ? state.selectedCandidateRef
    : candidateIdentityRef(candidates[0]);
  state.selectedCandidateRef = selectedRef;
  const selectedCandidate = candidates.find((candidate) => candidateIdentityRef(candidate) === selectedRef) || candidates[0];
  const locationFilterLabel =
    state.candidateLocationFilter === "all" ? "All locations" : state.candidateLocationFilter;
  tableRoot.innerHTML = `
    <div class="candidate-table-meta">
      <p class="muted">Showing ${escapeHtml(String(candidates.length))} of ${escapeHtml(String(currentCandidates().length))} candidates.</p>
      <p class="muted">Bucket: ${escapeHtml(titleCaseWords(state.candidateStatusFilter === "all" ? "all" : state.candidateStatusFilter))} | Location: ${escapeHtml(locationFilterLabel)}</p>
    </div>
    <div class="candidate-table-wrap">
      <table class="candidate-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Title</th>
            <th>Company</th>
            <th>Score</th>
            <th>Review Bucket</th>
            <th>Qualification</th>
            <th>Links</th>
          </tr>
        </thead>
        <tbody>
          ${candidates.map((candidate) => candidateTableRowMarkup(candidate, selectedRef)).join("")}
        </tbody>
      </table>
    </div>
  `;
  detailRoot.innerHTML = `<div class="candidate-detail-shell">${candidateCardMarkup(selectedCandidate)}</div>`;
  tableRoot.querySelectorAll("[data-candidate-row]").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedCandidateRef = row.dataset.candidateRow || "";
      renderCandidates();
    });
  });
  attachFeedbackHandlers();
}

function attachFeedbackHandlers() {
  document.querySelectorAll("[data-feedback-ref]").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!state.selectedProject || !state.currentReport?.report_paths?.json) {
        setStatus("Select a project and load results before saving feedback.", "warning");
        return;
      }
      const candidateRef = form.dataset.feedbackRef;
      const action = form.querySelector("[data-feedback-action]").value;
      const reasonCode = form.querySelector("[data-feedback-reason]").value.trim();
      const note = form.querySelector("[data-feedback-note]").value.trim();
      const submitButton = form.querySelector("button[type='submit']");
      submitButton.disabled = true;
      try {
        await fetchJSON("/app/feedback", {
          method: "POST",
          body: {
            project_id: state.selectedProjectId,
            report_path: state.currentReport.report_paths.json,
            candidate_ref: candidateRef,
            action,
            reason_code: reasonCode,
            note,
            feedback_db: state.settings.feedback_db,
            brief: state.selectedProject?.latest_brief_json || {},
          },
        });
        form.reset();
        setStatus("Feedback saved.", "success", "The recruiter action is now stored against the selected project.");
        await loadProjectReviews(state.selectedProjectId);
      } catch (error) {
        setStatus("Could not save feedback.", "danger", error.message);
      } finally {
        submitButton.disabled = false;
      }
    });
  });
}

function renderResults() {
  renderResultsSummary();
  renderCsvSummary();
  const root = document.getElementById("results-snapshot");
  const card = document.getElementById("results-snapshot-card");
  const failedJob = failedSearchJobForSelectedProject();
  if (!state.selectedProject) {
    if (card) card.hidden = true;
    root.innerHTML = `
      <div class="empty-state">
        <h4>Select or Create a Project</h4>
        <p>Choose a project first to open its latest search snapshot.</p>
      </div>
    `;
    return;
  }
  if (!state.currentReport || !Array.isArray(state.currentReport.candidates) || !state.currentReport.candidates.length) {
    if (card) card.hidden = true;
    root.innerHTML = `
      <div class="empty-state">
        <h4>${failedJob ? "Latest Search Failed" : "No Results Yet"}</h4>
        <p>${failedJob
          ? escapeHtml(failedJob.error || "The latest background search did not finish successfully.")
          : `Run a search for <strong>${escapeHtml(state.selectedProject.name)}</strong> to generate candidates.`}</p>
      </div>
    `;
    return;
  }
  if (card) card.hidden = false;
  const summary = state.currentReport.summary || {};
  const requested = currentRequestedCandidateLimit();
  const returned = currentCandidates().length;
  const activeJob = activeSearchJobForSelectedProject();
  root.innerHTML = `
    <div class="list-row list-row-detail">
      <div>
        <strong>${escapeHtml(formatTimestamp(state.currentReport.generated_at))}</strong>
        <p>${escapeHtml(state.selectedProject.role_title || state.selectedProject.name)} | Requested ${escapeHtml(String(requested))} candidates | Returned ${escapeHtml(String(returned))}</p>
      </div>
      <div class="list-meta run-history-meta">
        <span>${escapeHtml(String(summary.verified_count || 0))} Verified</span>
        <small>${escapeHtml(String(summary.review_count || 0))} Needs Review | ${escapeHtml(String(summary.reject_count || 0))} Rejected</small>
      </div>
    </div>
    <div class="results-snapshot-actions">
      <button type="button" class="button button-secondary" id="results-open-candidates">Open Candidates</button>
      <button type="button" class="button button-secondary" id="results-open-history">Open History</button>
      ${state.currentReport?.report_paths?.csv ? `<a class="button button-secondary" href="${artifactHref(state.currentReport.report_paths.csv)}">Download CSV</a>` : ""}
    </div>
    ${activeJob ? `<p class="muted snapshot-note">A newer search is still ${escapeHtml(titleCaseWords(activeJob.status))}. This snapshot shows the latest completed run until that new one finishes.</p>` : ""}
  `;
  document.getElementById("results-open-candidates")?.addEventListener("click", () => switchTab("candidates"));
  document.getElementById("results-open-history")?.addEventListener("click", () => switchTab("history"));
}

function reviewRowMarkup(review) {
  return `
    <div class="list-row list-row-detail">
      <div>
        <strong>${escapeHtml(review.full_name || review.candidate_id)}</strong>
          <p>${escapeHtml(titleCaseWords(review.action))}${review.reason_code ? ` | ${escapeHtml(review.reason_code)}` : ""}</p>
      </div>
      <div class="list-meta">
        <span>${escapeHtml(review.reviewer_name || review.reviewer_id)}</span>
        <small>${escapeHtml(formatTimestamp(review.created_at))}</small>
      </div>
    </div>
  `;
}

function renderFeedback() {
  const root = document.getElementById("feedback-list");
  if (!state.selectedProject) {
    root.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>Select or Create a Project</h4>
        <p>Choose a project first to see its recruiter feedback history.</p>
      </div>
    `;
    } else if (!state.currentReviews.length) {
      root.innerHTML = `
        <div class="empty-state compact-empty">
          <h4>No Feedback Yet</h4>
          <p>Save recruiter feedback from the Results or Candidates tab to start building project history.</p>
        </div>
      `;
  } else {
    root.innerHTML = state.currentReviews.map(reviewRowMarkup).join("");
  }
  document.getElementById("training-card").hidden = !state.user?.is_admin;
}

function runHistoryRowMarkup(run) {
  const summary = run.summary || {};
  const actions = [
    `<button type="button" class="button button-secondary button-small" data-load-run="${escapeHtml(run.run_id)}">Load Results</button>`,
  ];
  if (state.user?.is_admin) {
    actions.push(`<button type="button" class="button button-danger button-small" data-delete-run="${escapeHtml(run.run_id)}">Delete Run</button>`);
  }
  return `
    <div class="list-row list-row-detail">
      <div>
        <strong>${escapeHtml(formatTimestamp(run.created_at))}</strong>
        <p>${escapeHtml(run.role_title || state.selectedProject?.role_title || "Search Run")} | ${escapeHtml(String(run.candidate_count || 0))} candidates</p>
      </div>
      <div class="list-meta run-history-meta">
        <span>${escapeHtml(String(summary.verified_count || 0))} Verified</span>
        <div class="run-history-actions">
          ${actions.join("")}
        </div>
      </div>
    </div>
  `;
}

function recentProjectRowMarkup(project) {
  return `
    <button type="button" class="list-row list-row-detail project-row-button" data-project-open="${escapeHtml(project.id)}">
      <div>
        <strong>${escapeHtml(project.name)}</strong>
        <p>${escapeHtml(project.client_name || "No client")} | ${escapeHtml(project.role_title || "No role")}</p>
      </div>
      <div class="list-meta">
        <span>${escapeHtml(titleCaseWords(project.status))}</span>
        <small>${escapeHtml(formatTimestamp(project.latest_run_at || project.updated_at))}</small>
      </div>
    </button>
  `;
}

function attachHistoryHandlers() {
  document.querySelectorAll("[data-load-run]").forEach((button) => {
    button.addEventListener("click", async () => {
      await loadProjectRun(state.selectedProjectId, button.dataset.loadRun);
      switchTab("results");
    });
  });
  document.querySelectorAll("[data-delete-run]").forEach((button) => {
    button.addEventListener("click", async () => {
      await deleteRun(button.dataset.deleteRun);
    });
  });
  document.querySelectorAll("[data-project-open]").forEach((button) => {
    button.addEventListener("click", async () => {
      await loadProject(button.dataset.projectOpen);
      switchTab("results");
    });
  });
}

function renderHistory() {
  const runRoot = document.getElementById("run-history-list");
  const projectRoot = document.getElementById("recent-projects-list");
  if (!state.selectedProject) {
    runRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>Select or Create a Project</h4>
        <p>Choose a project first to see its saved runs.</p>
      </div>
    `;
  } else if (!state.currentRuns.length) {
    runRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Saved Runs Yet</h4>
        <p>Run a search and the project history will appear here.</p>
      </div>
    `;
  } else {
    runRoot.innerHTML = state.currentRuns.map(runHistoryRowMarkup).join("");
  }
  if (!state.projects.length) {
    projectRoot.innerHTML = `
      <div class="empty-state compact-empty">
        <h4>No Projects Yet</h4>
        <p>Recent project activity will appear once projects are saved.</p>
      </div>
    `;
  } else {
    projectRoot.innerHTML = state.projects.slice(0, 8).map(recentProjectRowMarkup).join("");
  }
  attachHistoryHandlers();
}

function renderOwnerSnapshot() {
  if (!state.user?.is_admin) {
    document.getElementById("owner-fab").hidden = true;
    closeOwnerDrawer();
    return;
  }
  document.getElementById("owner-fab").hidden = false;
  document.getElementById("owner-user-name").textContent = state.user.full_name || state.user.email;
  document.getElementById("owner-user-role").textContent = state.user.is_admin ? "Admin" : "Recruiter";
  const metricsRoot = document.getElementById("owner-metrics");
  const ops = state.ops;
  if (!ops) {
    metricsRoot.innerHTML = `<p class="muted">System information will appear here once loaded.</p>`;
  } else {
    const remote = ops.remote_sourcing || {};
    metricsRoot.innerHTML = `
      <div class="chip-row">
        <span class="info-chip">Projects ${escapeHtml(String(ops.counts?.mandates || 0))}</span>
        <span class="info-chip">Runs ${escapeHtml(String(ops.counts?.search_runs || 0))}</span>
        <span class="info-chip">Saved Candidates ${escapeHtml(String(ops.counts?.candidate_registry || 0))}</span>
        <span class="info-chip">Feedback ${escapeHtml(String(ops.counts?.review_actions || 0))}</span>
      </div>
      <p class="muted">Private sourcing connection: ${remote.configured ? "Connected" : "Not connected"}${remote.required ? " | Required for live searches" : ""}</p>
    `;
  }
  const userRoot = document.getElementById("owner-user-list");
  if (!state.users.length) {
    userRoot.innerHTML = `<p class="muted">No recruiter accounts loaded yet.</p>`;
  } else {
    userRoot.innerHTML = state.users
      .map(
        (user) => `
        <section class="directory-card">
          <div class="directory-card-head">
            <div>
              <strong>${escapeHtml(user.full_name || user.email)}</strong>
              <p>${escapeHtml(user.email)}</p>
            </div>
            <div class="directory-chip-row">
              <span class="info-chip">${escapeHtml(user.is_admin ? "Admin" : "Recruiter")}</span>
              <span class="info-chip">${escapeHtml(user.team_id || "No Team")}</span>
              <span class="info-chip ${user.has_totp_secret ? "chip-good" : "chip-warn"}">${escapeHtml(user.has_totp_secret ? "Authenticator Ready" : "Authenticator Missing")}</span>
            </div>
          </div>
          <div class="directory-card-actions">
            <button type="button" class="button button-secondary button-small" data-user-show-totp="${escapeHtml(user.id)}">Show Setup Key</button>
            <button type="button" class="button button-secondary button-small" data-user-rotate-totp="${escapeHtml(user.id)}">Rotate Key</button>
          </div>
        </section>
      `,
      )
      .join("");
  }
  renderOwnerJobs();
}

function renderOwnerJobs() {
  const root = document.getElementById("owner-job-list");
  if (!state.activeJob) {
    root.innerHTML = `<p class="muted">Queued searches and training jobs will appear here.</p>`;
    return;
  }
  const isSearchJob = state.activeJob.job_type === "search";
  const isRunningJob = ["queued", "running"].includes(String(state.activeJob.status || "").toLowerCase());
  const isFailedJob = String(state.activeJob.status || "").toLowerCase() === "failed";
  root.innerHTML = `
    <div class="list-row list-row-detail">
      <div>
        <strong>${escapeHtml(titleCaseWords(state.activeJob.job_type || "job"))}</strong>
        <p>${escapeHtml(titleCaseWords(state.activeJob.status || "queued"))}</p>
        ${state.activeJob.error ? `<small>${escapeHtml(state.activeJob.error)}</small>` : ""}
      </div>
      <div class="list-meta">
        <span>${escapeHtml(formatTimestamp(state.activeJob.created_at))}</span>
        <small>${escapeHtml(state.activeJob.job_id || "")}</small>
      </div>
    </div>
    <div class="inline-actions">
      ${isRunningJob && state.user?.is_admin ? `<button type="button" class="button button-secondary button-small" id="owner-stop-job">Stop Job</button>` : ""}
      ${isFailedJob && isSearchJob ? `<button type="button" class="button button-secondary button-small" id="owner-retry-search">Retry Search</button>` : ""}
    </div>
  `;
  document.getElementById("owner-stop-job")?.addEventListener("click", stopActiveJob);
  document.getElementById("owner-retry-search")?.addEventListener("click", retrySearchForSelectedProject);
}

async function refreshProjects(query = state.projectSearchQuery) {
  if (!state.user) return;
  state.projectSearchQuery = query;
  const payload = await fetchJSON(`/app/projects?query=${encodeURIComponent(query)}&limit=60`);
  state.projects = Array.isArray(payload.projects) ? payload.projects : [];
  renderProjectList();
  renderHistory();
  const selected = selectedProjectFromList();
  if (selected && !state.selectedProject) {
    state.selectedProject = selected;
  } else if (!selected && state.selectedProjectId) {
    state.selectedProjectId = "";
    state.selectedProject = null;
  }
  updateTopbarActions();
}

async function refreshUsers() {
  if (!state.user) return;
  const payload = await fetchJSON("/app/users");
  state.users = Array.isArray(payload.users) ? payload.users : [];
  const selectedIds =
    state.selectedProject?.assigned_recruiters?.map((user) => user.id)
    || (state.user ? [state.user.id] : []);
  renderMemberPicker(selectedIds);
  renderOwnerSnapshot();
}

async function refreshOps() {
  if (!state.user?.is_admin) return;
  state.ops = await fetchJSON("/app/ops");
  renderOwnerSnapshot();
}

async function loadProject(projectId) {
  if (!projectId) return;
  const payload = await fetchJSON(`/app/projects/${encodeURIComponent(projectId)}`);
  state.selectedProjectId = payload.project.id;
  state.selectedProject = payload.project;
  state.currentReport = null;
  state.candidateSearchQuery = "";
  state.candidateStatusFilter = "all";
  state.candidateLocationFilter = "all";
  state.selectedCandidateRef = "";
  persistStoredState();
  populateProjectForm(payload.project);
  renderProjectSummary();
  renderProjectList();
  updateTopbarActions();
  await Promise.allSettled([
    loadProjectRuns(projectId),
    loadProjectReviews(projectId),
    loadLatestProjectJob(projectId),
  ]);
  const reportLoad = loadProjectRun(projectId, "", { timeoutMs: 5000, suppressErrors: true });
  const failedJob = failedSearchJobForSelectedProject();
  const activeJob = activeSearchJobForSelectedProject();
  if (!activeJob) {
    await reportLoad;
  }
  if (failedJob) {
    setStatus("Latest search failed.", "danger", `${failedJob.error || "The latest search did not complete successfully."} Retry when ready.`);
    return;
  }
  if (activeJob) {
    setStatus("Search still running.", "warning", `The latest search is ${titleCaseWords(activeJob.status)} for up to ${jobRequestedLimit(activeJob)} candidates.`);
    return;
  }
  setStatus(`${payload.project.name} loaded.`, "success", "The project brief, results, feedback, and history are ready.");
}

async function loadLatestProjectJob(projectId) {
  if (!projectId) {
    state.activeJob = null;
    clearJobPolling();
    renderOwnerJobs();
    renderResults();
    renderCandidates();
    renderStatusJobPanel(null);
    return;
  }
  const payload = await fetchJSON(`/app/projects/${encodeURIComponent(projectId)}/latest-job`);
  const incomingJob = payload?.job || null;
  const polledJobActive =
    Boolean(state.activeJob) &&
    state.activeJob.job_type === "search" &&
    state.activeJob.job_id === state.polledJobId &&
    jobProjectId(state.activeJob) === projectId &&
    isActiveJobStatus(state.activeJob.status);
  if (!polledJobActive) {
    state.activeJob = incomingJob;
  } else if (incomingJob && incomingJob.job_id === state.polledJobId) {
    state.activeJob = incomingJob;
  }
  if (incomingJob && isActiveJobStatus(incomingJob.status) && String(incomingJob.job_id || "").trim()) {
    if (state.polledJobId !== String(incomingJob.job_id)) {
      startJobPolling(incomingJob.job_id);
    } else {
      startLiveProgressTicker();
    }
  } else if (!incomingJob || !isActiveJobStatus(incomingJob.status)) {
    clearJobPolling();
  }
  renderOwnerJobs();
  renderResults();
  renderCandidates();
  syncLiveJobStatus();
}

async function loadProjectRuns(projectId) {
  if (!projectId) {
    state.currentRuns = [];
    renderHistory();
    return;
  }
  const payload = await fetchJSON(`/app/projects/${encodeURIComponent(projectId)}/runs?limit=25`);
  state.currentRuns = Array.isArray(payload.runs) ? payload.runs : [];
  renderHistory();
}

async function loadProjectReviews(projectId) {
  if (!projectId) {
    state.currentReviews = [];
    renderFeedback();
    return;
  }
  const payload = await fetchJSON(`/app/reviews?project_id=${encodeURIComponent(projectId)}&limit=50`);
  state.currentReviews = Array.isArray(payload.reviews) ? payload.reviews : [];
  renderFeedback();
}

async function loadProjectRun(projectId, runId = "", options = {}) {
  if (!projectId) {
    state.currentReport = null;
    state.candidateSearchQuery = "";
    state.candidateStatusFilter = "all";
    state.candidateLocationFilter = "all";
    state.selectedCandidateRef = "";
    renderResults();
    renderCandidates();
    return;
  }
  const query = runId ? `?run_id=${encodeURIComponent(runId)}` : "";
  try {
    const timeoutMs = safeNumber(options.timeoutMs, 0);
    const payload = await fetchJSON(
      `/app/projects/${encodeURIComponent(projectId)}/run${query}`,
      timeoutMs > 0 ? { timeoutMs } : {},
    );
    state.currentReport = payload && payload.run_id ? payload : null;
    state.candidateSearchQuery = "";
    state.candidateStatusFilter = "all";
    state.candidateLocationFilter = "all";
    state.selectedCandidateRef = "";
    renderResults();
    renderCandidates();
    return state.currentReport;
  } catch (error) {
    if (!options.suppressErrors) {
      throw error;
    }
    return null;
  }
}

async function saveProject() {
  if (!state.user) return;
  const payload = projectPayloadForSave();
  const methodUrl = state.selectedProjectId
    ? `/app/projects/${encodeURIComponent(state.selectedProjectId)}`
    : "/app/projects";
  const response = await fetchJSON(methodUrl, { method: "POST", body: payload });
  state.selectedProjectId = response.project.id;
  state.selectedProject = response.project;
  persistStoredState();
  populateProjectForm(response.project);
  await refreshProjects(state.projectSearchQuery);
  renderProjectSummary();
  updateTopbarActions();
  setStatus("Project saved.", "success", "The hunt brief and project metadata are now stored.");
  return response.project;
}

async function deleteProject() {
  if (!state.user || !state.selectedProjectId || !state.selectedProject) return;
  const confirmed = window.confirm(
    `Delete project "${state.selectedProject.name}"? This will remove its saved runs and review history from the workspace.`,
  );
  if (!confirmed) return;

  const deletedName = state.selectedProject.name;
  await fetchJSON(`/app/projects/${encodeURIComponent(state.selectedProjectId)}`, { method: "DELETE" });
  state.selectedProjectId = "";
  state.selectedProject = null;
  state.currentReport = null;
  state.currentRuns = [];
  state.currentReviews = [];
  state.candidateSearchQuery = "";
  state.candidateStatusFilter = "all";
  state.candidateLocationFilter = "all";
  state.selectedCandidateRef = "";
  persistStoredState();
  resetProjectForm();
  await refreshProjects(state.projectSearchQuery);
  renderProjectSummary();
  renderResults();
  renderCandidates();
  renderFeedback();
  renderHistory();
  updateTopbarActions();
  switchTab("projects");
  setStatus("Project deleted.", "success", `${deletedName} was removed from this workspace.`);
}

async function deleteRun(runId) {
  if (!state.user?.is_admin || !state.selectedProjectId || !runId) return;
  const run = state.currentRuns.find((entry) => entry.run_id === runId);
  const projectName = state.selectedProject?.name || "this project";
  const confirmed = window.confirm(
    `Delete this saved run from ${projectName}? This removes the run from project history and deletes its saved report files.`,
  );
  if (!confirmed) return;
  try {
    await fetchJSON(
      `/app/projects/${encodeURIComponent(state.selectedProjectId)}/runs/${encodeURIComponent(runId)}`,
      { method: "DELETE" },
    );
    await refreshProjects(state.projectSearchQuery);
    await loadProject(state.selectedProjectId);
    switchTab("history");
    setStatus(
      "Saved run deleted.",
      "success",
      `${run?.candidate_count ?? 0} candidates were removed from ${projectName} history.`,
    );
  } catch (error) {
    setStatus("Could not delete saved run.", "danger", error.message);
  }
}

async function runSearch() {
  if (!state.user) return;
  const briefPayload = buildBriefPayload();
  const roleTitle = String(briefPayload.role_title || "").trim();
  if (!roleTitle) {
    setStatus("Role title is required.", "warning", "Add a role title before running search.");
    switchTab("recruiter");
    return;
  }
  const readiness = assessHuntReadiness(briefPayload);
  if (!readiness.ok) {
    setStatus("Add more hunt details.", "warning", readiness.message);
    switchTab("recruiter");
    return;
  }
  try {
    const project = await saveProject();
    const payload = {
      ...briefPayload,
      project_id: project.id,
    };
    const job = await fetchJSON("/app/search-jobs", { method: "POST", body: payload });
    state.activeJob = job;
    renderOwnerJobs();
    renderResults();
    renderCandidates();
      switchTab("results");
      syncLiveJobStatus();
    startJobPolling(job.job_id);
  } catch (error) {
    setStatus("Search could not start.", "danger", error.message);
  }
}

function clearJobPolling() {
  if (state.jobPollHandle) {
    window.clearTimeout(state.jobPollHandle);
    state.jobPollHandle = null;
  }
  if (state.liveProgressHandle) {
    window.clearInterval(state.liveProgressHandle);
    state.liveProgressHandle = null;
  }
  state.polledJobId = "";
}

function startLiveProgressTicker() {
  if (state.liveProgressHandle) {
    return;
  }
  state.liveProgressHandle = window.setInterval(() => {
    const activeJob = activeSearchJobForSelectedProject();
    if (!activeJob || !isActiveJobStatus(activeJob.status)) {
      if (state.liveProgressHandle) {
        window.clearInterval(state.liveProgressHandle);
        state.liveProgressHandle = null;
      }
      return;
    }
    syncLiveJobStatus();
    renderActiveJobTabPanels();
  }, 1000);
}

function startJobPolling(jobId) {
  const resolvedJobId = String(jobId || "").trim();
  if (!resolvedJobId) {
    return;
  }
  clearJobPolling();
  state.polledJobId = resolvedJobId;
  startLiveProgressTicker();
  const poll = async () => {
    try {
      const job = await fetchJSON(`/app/jobs/${encodeURIComponent(resolvedJobId)}`, { timeoutMs: 8000 });
      state.activeJob = job;
      renderOwnerJobs();
      renderActiveJobTabPanels();
      syncLiveJobStatus();
      if (job.status === "completed") {
        clearJobPolling();
        await handleCompletedJob(job);
        return;
      }
      if (job.status === "failed") {
        clearJobPolling();
        renderOwnerJobs();
        renderResults();
        renderCandidates();
        setStatus("Search failed.", "danger", `${job.error || "The job did not complete successfully."} Retry the search when you're ready.`);
        return;
      }
      state.jobPollHandle = window.setTimeout(poll, 2000);
    } catch {
      state.jobPollHandle = window.setTimeout(poll, 2500);
    }
  };
  poll();
}

async function retrySearchForSelectedProject() {
  if (!state.selectedProjectId) {
    setStatus("Select a project first.", "warning", "Choose a project before retrying the search.");
    return;
  }
  await runSearch();
}

async function stopActiveJob() {
  if (!state.user?.is_admin || !state.activeJob?.job_id) return;
  try {
    const job = await fetchJSON(`/app/jobs/${encodeURIComponent(state.activeJob.job_id)}/stop`, {
      method: "POST",
      body: { reason: "Stopped by admin. Retry when ready." },
    });
    state.activeJob = job;
    renderOwnerJobs();
    renderResults();
    renderCandidates();
    setStatus("Background job stopped.", "warning", "The job was stopped and can be retried when ready.");
  } catch (error) {
    setStatus("Could not stop background job.", "danger", error.message);
  }
}

async function handleCompletedJob(job) {
  if (job.job_type === "search") {
    const projectId = job.result?.project?.id || state.selectedProjectId;
    if (projectId) {
      await refreshProjects(state.projectSearchQuery);
      await loadProject(projectId);
      if (job.result?.run_id) {
        await loadProjectRun(projectId, job.result.run_id);
      }
    }
    setStatus("Search completed.", "success", "The latest run has been attached to the current project.");
    switchTab("results");
    return;
  }
  if (job.job_type === "train_ranker") {
    setStatus("Feedback-trained model ready.", "success", "The latest trained model from recruiter feedback has been saved.");
    return;
  }
  setStatus("Background job completed.", "success");
}

async function runBreakdown() {
  const roleTitle = document.getElementById("role-title").value.trim();
  const jobDescription = document.getElementById("job-description").value.trim();
  const uploadedJobDescription = state.uploadedJobDescription?.text || "";
  if (!jobDescription && !uploadedJobDescription) {
    setStatus("Add a JD first.", "warning", "Upload a JD file or paste the job description before running JD Breakdown.");
    return;
  }
  try {
    const breakdown = await fetchJSON("/app/jd-breakdown", {
      method: "POST",
      body: {
        role_title: roleTitle,
        job_description: jobDescription,
        uploaded_job_description_text: uploadedJobDescription,
        uploaded_job_description_name: state.uploadedJobDescription?.name || "",
      },
    });
    applyBreakdownToForm(breakdown);
    setStatus(
      "JD breakdown ready.",
      "success",
      state.uploadedJobDescription?.name
        ? `Structured points were extracted from ${state.uploadedJobDescription.name} using your typed notes as optional context.`
        : "Key experience points and suggested anchors have been updated from the typed description.",
    );
  } catch (error) {
    setStatus("JD breakdown failed.", "danger", error.message);
  }
}

async function handleJdUpload(event) {
  const input = event.target;
  const file = input.files?.[0];
  if (!file) {
    return;
  }
  const roleTitle = document.getElementById("role-title").value.trim();
  const jobDescriptionNotes = document.getElementById("job-description").value.trim();
  const formData = new FormData();
  formData.append("file", file);
  formData.append("role_title", roleTitle);
  formData.append("job_description_notes", jobDescriptionNotes);
  input.disabled = true;
  setStatus("Uploading JD file...", "default", `Reading ${file.name} and preparing the JD breakdown.`);
  try {
    const payload = await fetchFormData("/app/jd-upload", formData);
    setUploadedJobDescription({
      name: payload.uploaded_file_name,
      text: payload.uploaded_job_description_text,
      extension: payload.uploaded_file_extension,
      parser: payload.uploaded_parser,
    });
    applyBreakdownToForm(payload.breakdown || {});
    setStatus(
      "JD file uploaded.",
      "success",
      `${payload.uploaded_file_name || file.name} is now the primary JD source and the breakdown has been updated automatically.`,
    );
  } catch (error) {
    setStatus("JD upload failed.", "danger", error.message);
  } finally {
    input.disabled = false;
    input.value = "";
  }
}

async function handleLogin(event) {
  event.preventDefault();
  const emailRequired = loginEmailRequired();
  const emailInput = document.getElementById("login-email");
  const email = emailRequired && emailInput ? emailInput.value.trim() : "";
  const otpCode = document.getElementById("login-otp-code").value.trim();
  const message = document.getElementById("login-message");
  if ((!emailRequired && !otpCode) || (emailRequired && (!email || !otpCode))) {
    message.textContent = emailRequired ? "Enter your email and authenticator code." : "Enter your authenticator code.";
    return;
  }
  message.textContent = "Signing in...";
  try {
    const payload = await fetchJSON("/app/auth/login", {
      method: "POST",
      body: { email, otp_code: otpCode },
    });
    message.textContent = "Signing in...";
    await completeLoginPayload(payload);
  } catch (error) {
    showAuthShell();
    message.textContent = error.message;
  }
}

window.__hrHunterHandleLogin = handleLogin;

function readSessionTokenFromHash() {
  const raw = window.location.hash.startsWith("#") ? window.location.hash.slice(1) : window.location.hash;
  const params = new URLSearchParams(raw);
  return String(params.get(SESSION_HASH_KEY) || "").trim();
}

function clearSessionTokenFromUrl() {
  const url = new URL(window.location.href);
  url.searchParams.delete("session");
  url.hash = "";
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
}

async function completeLoginPayload(payload) {
  state.sessionToken = String(payload?.session_token || "").trim();
  if (!state.sessionToken) {
    throw new Error("Sign in succeeded but no session token was returned.");
  }
  try {
    window.localStorage.setItem(SESSION_KEY, state.sessionToken);
    window.localStorage.setItem(SESSION_HANDOFF_KEY, "1");
  } catch {
    // Non-fatal: keep the session token in memory for this page load.
  }
  showRestoringShell();
  await completeSessionBootstrap(payload.user, payload.projects || []);
  try {
    window.localStorage.removeItem(SESSION_HANDOFF_KEY);
  } catch {
    // Non-fatal.
  }
  clearSessionTokenFromUrl();
  switchTab("projects");
  setStatus(`Welcome back, ${payload.user?.full_name || payload.user?.email || "Recruiter"}.`, "success", "Select a project or open the hunt brief.");
}

window.__hrHunterCompleteLogin = completeLoginPayload;

async function restoreSession() {
  const urlSessionParam = new URL(window.location.href).searchParams.get("session") || "";
  const hashToken = readSessionTokenFromHash();
  let storedToken = "";
  try {
    storedToken = window.localStorage.getItem(SESSION_KEY) || "";
  } catch {
    storedToken = "";
  }
  const resolvedToken = storedToken || hashToken;
  if (!resolvedToken) {
    // Attempt cookie-backed session (server may have issued an HttpOnly session cookie).
    try {
      const payload = await fetchJSON("/app/auth/session");
      showRestoringShell();
      await completeSessionBootstrap(payload.user, payload.projects || []);
      clearSessionTokenFromUrl();
      switchTab("projects");
      setStatus(`Welcome back, ${payload.user.full_name || payload.user.email}.`, "success", "Select a project or open the hunt brief.");
      return;
    } catch {
      showAuthShell();
      if (urlSessionParam === "auth-failed") {
        const message = document.getElementById("login-message");
        if (message) {
          message.textContent = "Invalid authenticator code. Please try again.";
        }
      }
      clearSessionTokenFromUrl();
      return;
    }
  }
  if (hashToken && !storedToken) {
    try {
      window.localStorage.setItem(SESSION_KEY, hashToken);
      storedToken = hashToken;
    } catch {
      // Non-fatal.
    }
  }
  state.sessionToken = resolvedToken;
  showRestoringShell();
  try {
    const payload = await fetchJSON("/app/auth/session");
    await completeSessionBootstrap(payload.user, payload.projects || []);
    try {
      window.localStorage.removeItem(SESSION_HANDOFF_KEY);
    } catch {
      // Non-fatal.
    }
    clearSessionTokenFromUrl();
    switchTab("projects");
    setStatus(`Welcome back, ${payload.user.full_name || payload.user.email}.`, "success", "Select a project or open the hunt brief.");
  } catch {
    try {
      window.localStorage.removeItem(SESSION_KEY);
      window.localStorage.removeItem(SESSION_HANDOFF_KEY);
    } catch {
      // Non-fatal.
    }
    state.sessionToken = "";
    // If we have a cookie-backed session but the stored token was stale, retry once without the header.
    try {
      const payload = await fetchJSON("/app/auth/session");
      showRestoringShell();
      await completeSessionBootstrap(payload.user, payload.projects || []);
      try {
        window.localStorage.removeItem(SESSION_HANDOFF_KEY);
      } catch {
        // Non-fatal.
      }
      clearSessionTokenFromUrl();
      switchTab("projects");
      setStatus(`Welcome back, ${payload.user.full_name || payload.user.email}.`, "success", "Select a project or open the hunt brief.");
      return;
    } catch {
      showAuthShell();
    }
  }
}

async function completeSessionBootstrap(user, initialProjects = []) {
  state.user = user;
  state.projects = Array.isArray(initialProjects) ? initialProjects : [];
  document.getElementById("nav-user-name").textContent = user.full_name || user.email;
  document.getElementById("nav-user-role").textContent = user.is_admin ? "Admin" : "Recruiter";
  document.getElementById("owner-user-name").textContent = user.full_name || user.email;
  document.getElementById("owner-user-role").textContent = user.is_admin ? "Admin" : "Recruiter";
  populateSettingsFields();
  renderProjectList();
  renderHistory();
  const bootstrapTasks = [
    refreshUsers(),
    refreshProjects(state.projectSearchQuery),
  ];
  if (state.user?.is_admin) {
    bootstrapTasks.push(refreshOps());
  }
  const bootstrapResults = await Promise.allSettled(bootstrapTasks);
  const bootstrapFailure = bootstrapResults.find((result) => result.status === "rejected");
  const storedProject = state.selectedProjectId;
  try {
    if (storedProject && state.projects.some((project) => project.id === storedProject)) {
      await loadProject(storedProject);
    } else {
      startNewProject();
      switchTab("projects");
    }
  } catch {
    startNewProject();
    switchTab("projects");
  }
  if (bootstrapFailure && bootstrapFailure.reason) {
    setStatus(
      "Signed in with partial data.",
      "warning",
      bootstrapFailure.reason.message || "Some workspace data could not be loaded yet.",
    );
  }
}

async function logout() {
  try {
    await fetchJSON("/app/auth/logout", { method: "POST" });
  } catch {
    // no-op
  }
  clearJobPolling();
  window.localStorage.removeItem(SESSION_KEY);
  window.localStorage.removeItem(SESSION_HANDOFF_KEY);
  state.sessionToken = "";
  state.user = null;
  state.users = [];
  state.projects = [];
  state.selectedProjectId = "";
  state.selectedProject = null;
  state.currentReport = null;
  state.currentRuns = [];
  state.currentReviews = [];
  state.activeJob = null;
  document.getElementById("nav-user-name").textContent = "Guest";
  document.getElementById("nav-user-role").textContent = "Signed Out";
  document.getElementById("owner-user-name").textContent = "HR Hunter Admin";
  document.getElementById("owner-user-role").textContent = "Admin";
  clearStoredState();
  hideProvisioningCard();
  showAuthShell();
}

async function handleCreateRecruiter(event) {
  event.preventDefault();
  try {
    const payload = await fetchJSON("/app/admin/users", {
      method: "POST",
      body: {
        full_name: document.getElementById("new-user-name").value.trim(),
        email: document.getElementById("new-user-email").value.trim(),
        team_id: document.getElementById("new-user-team").value.trim(),
      },
    });
    event.target.reset();
    await refreshUsers();
    renderProvisioningCard(payload.user, `Share this key with ${payload.user?.full_name || payload.user?.email}. They will sign in with their email and a 6-digit authenticator code.`);
    setStatus("Recruiter account created.", "success", "The authenticator setup key is ready to share with the new recruiter.");
  } catch (error) {
    setStatus("Could not create recruiter.", "danger", error.message);
  }
}

async function handleRevealUserTotp(userId, rotate = false) {
  try {
    const payload = await fetchJSON(`/app/admin/users/${encodeURIComponent(userId)}/totp`, {
      method: "POST",
      body: { rotate },
    });
    renderProvisioningCard(
      payload,
      rotate
        ? `A new authenticator key has been issued for ${payload.user?.full_name || payload.user?.email}. Share the updated key with them.`
        : `Share this authenticator key only with ${payload.user?.full_name || payload.user?.email}.`,
    );
    setStatus(
      rotate ? "Authenticator key rotated." : "Authenticator key loaded.",
      "success",
      rotate
        ? "The previous key is no longer valid for that recruiter."
        : "The current authenticator setup key is ready to share.",
    );
    await refreshUsers();
  } catch (error) {
    setStatus("Could not load authenticator key.", "danger", error.message);
  }
}

function loadDemoBrief() {
  const preset =
    state.config?.presets?.ceo_marina_home_emea ||
    state.config?.presets?.supply_chain_manager_uae;
  if (!preset) return;
  const existingProject = state.selectedProject ? formValuesFromProject(state.selectedProject) : null;
  if (existingProject) {
    document.getElementById("project-name").value = existingProject.projectName;
    document.getElementById("client-name").value = existingProject.clientName;
    document.getElementById("project-status").value = existingProject.status;
    document.getElementById("target-geography").value = existingProject.targetGeography;
    document.getElementById("project-notes").value = existingProject.notes;
    renderMemberPicker(existingProject.assignedRecruiterIds.length ? existingProject.assignedRecruiterIds : (state.user ? [state.user.id] : []));
  } else {
    document.getElementById("project-name").value = preset.project_name || `${preset.role_title || "Demo Role"} - UAE`;
    document.getElementById("client-name").value = preset.client_name || "Demo Client";
    document.getElementById("project-status").value = "active";
  }
  document.getElementById("role-title").value = preset.role_title || "";
  if (!existingProject) {
    document.getElementById("target-geography").value = (preset.countries || [])[0] || "";
  }
  document.getElementById("years-mode").value = preset.years_mode || "range";
  document.getElementById("years-value").value = preset.years_value ?? "";
  document.getElementById("years-tolerance").value = preset.years_tolerance ?? "";
  document.getElementById("radius-miles").value = state.config?.defaults?.radius_miles || 25;
  document.getElementById("candidate-limit").value = preset.max_profiles || state.settings.limit || state.config?.defaults?.limit || 20;
  document.getElementById("company-match-mode").value = preset.company_match_mode || "both";
  document.getElementById("employment-status-mode").value = preset.employment_status_mode || "any";
  document.getElementById("job-description").value = preset.job_description || "";
  setUploadedJobDescription();
  state.tokenFields.titles.setTokens(preset.titles || []);
  state.tokenFields.countries.setTokens(preset.countries || []);
  state.tokenFields.continents.setTokens(preset.continents || []);
  state.tokenFields.cities.setTokens(preset.cities || []);
  state.tokenFields.companies.setTokens(preset.company_targets || []);
  state.tokenFields.mustHave.setTokens(preset.must_have_keywords || []);
  state.tokenFields.niceToHave.setTokens(preset.nice_to_have_keywords || []);
  state.tokenFields.industry.setTokens(preset.industry_keywords || []);
  state.currentBreakdown = preset.jd_breakdown || null;
  renderBreakdown();
  renderAnchorGrid(preset.anchors || {});
  switchTab("recruiter");
  setStatus(
    "Demo brief loaded.",
    "success",
    existingProject
      ? `Demo hunt criteria loaded into ${existingProject.projectName}. Save or run when you're ready.`
      : "You can edit it before saving or running the project.",
  );
}

async function trainRanker() {
  if (!state.user?.is_admin) return;
  collectSettingsFromInputs();
  try {
    const job = await fetchJSON("/app/train-ranker-jobs", {
      method: "POST",
      body: {
        feedback_db: state.settings.feedback_db,
        model_dir: state.settings.model_dir,
        n_estimators: safeNumber(document.getElementById("ranker-estimators").value, 80),
        num_leaves: safeNumber(document.getElementById("ranker-num-leaves").value, 31),
      },
    });
    state.activeJob = job;
    renderOwnerJobs();
    setStatus("Ranker training queued.", "default", "The learned ranker is training in the background.");
    startJobPolling(job.job_id);
  } catch (error) {
    setStatus("Could not queue ranker training.", "danger", error.message);
  }
}

async function submitSupport(event) {
  event.preventDefault();
  try {
    await fetchJSON("/app/support-request", {
      method: "POST",
      body: {
        name: document.getElementById("support-name").value.trim(),
        contact: document.getElementById("support-contact").value.trim(),
        topic: document.getElementById("support-topic").value.trim(),
        message: document.getElementById("support-message").value.trim(),
      },
    });
    event.target.reset();
    setStatus("Support request sent.", "success", "The request has been written to the app inbox.");
  } catch (error) {
    setStatus("Support request failed.", "danger", error.message);
  }
}

async function submitFeature(event) {
  event.preventDefault();
  try {
    await fetchJSON("/app/feature-request", {
      method: "POST",
      body: {
        title: document.getElementById("feature-title").value.trim(),
        message: document.getElementById("feature-message").value.trim(),
      },
    });
    event.target.reset();
    setStatus("Feature suggestion sent.", "success", "The request has been written to the app inbox.");
  } catch (error) {
    setStatus("Feature suggestion failed.", "danger", error.message);
  }
}

function saveSettings() {
  collectSettingsFromInputs();
  applyTheme(state.settings.theme);
  populateSettingsFields();
  persistStoredState();
  setStatus("Settings saved.", "success", "Theme, ranking preferences, and workspace paths have been updated for this browser.");
}

function resetSettings() {
  state.settings = defaultSettingsFromConfig();
  applyTheme(state.settings.theme);
  populateSettingsFields();
  renderProviderOptions();
  persistStoredState();
  setStatus("Settings reset.", "success", "Default theme, ranking preferences, and paths have been restored.");
}

function bindEvents() {
  window.__HR_HUNTER_LOGIN_BOUND = true;
  window.addEventListener("beforeunload", handleBeforeUnload);
  const loginForm = document.getElementById("login-form");
  if (loginForm && loginForm.dataset.inlineManaged !== "true") {
    loginForm.addEventListener("submit", handleLogin);
  }
  document.getElementById("nav-logout-button").addEventListener("click", logout);
  document.getElementById("nav-settings-button").addEventListener("click", () => switchTab("settings"));
  document.getElementById("menu-button").addEventListener("click", openNav);
  document.getElementById("nav-close-button").addEventListener("click", closeNav);
  document.getElementById("nav-backdrop").addEventListener("click", closeNav);
  document.querySelectorAll("[data-tab-target]").forEach((button) => {
    button.addEventListener("click", () => switchTab(button.dataset.tabTarget));
  });
  document.getElementById("new-project-button").addEventListener("click", startNewProject);
  document.getElementById("project-reset-button").addEventListener("click", startNewProject);
  document.getElementById("refresh-projects-button").addEventListener("click", () => refreshProjects(document.getElementById("project-search-input").value.trim()));
  document.getElementById("project-search-input").addEventListener("input", (event) => {
    window.clearTimeout(document.getElementById("project-search-input")._timer);
    document.getElementById("project-search-input")._timer = window.setTimeout(() => {
      refreshProjects(event.target.value.trim());
    }, 250);
  });
  document.getElementById("candidate-search-input").addEventListener("input", (event) => {
    state.candidateSearchQuery = event.target.value.trim();
    renderCandidates();
  });
  document.getElementById("jd-upload-input").addEventListener("change", handleJdUpload);
  document.getElementById("breakdown-button").addEventListener("click", runBreakdown);
  document.getElementById("top-save-button").addEventListener("click", async () => {
    try {
      await saveProject();
    } catch (error) {
      setStatus("Project could not be saved.", "danger", error.message);
    }
  });
  document.getElementById("top-delete-button").addEventListener("click", async () => {
    try {
      await deleteProject();
    } catch (error) {
      setStatus("Project could not be deleted.", "danger", error.message);
    }
  });
  document.getElementById("top-run-button").addEventListener("click", runSearch);
  document.getElementById("refresh-results-button").addEventListener("click", async () => {
    if (!state.selectedProjectId) return;
    await loadProjectRun(state.selectedProjectId);
    setStatus("Results refreshed.", "success", "The latest saved run has been loaded.");
  });
  document.getElementById("refresh-feedback-button").addEventListener("click", async () => {
    if (!state.selectedProjectId) return;
    await loadProjectReviews(state.selectedProjectId);
    setStatus("Feedback refreshed.", "success", "Recent recruiter actions are up to date.");
  });
  document.getElementById("refresh-history-button").addEventListener("click", async () => {
    if (!state.selectedProjectId) return;
    await loadProjectRuns(state.selectedProjectId);
    setStatus("History refreshed.", "success", "Saved search runs are up to date.");
  });
  document.getElementById("settings-save-button").addEventListener("click", saveSettings);
  document.getElementById("settings-reset-button").addEventListener("click", resetSettings);
  document.getElementById("support-form").addEventListener("submit", submitSupport);
  document.getElementById("feature-form").addEventListener("submit", submitFeature);
  document.getElementById("owner-fab").addEventListener("click", openOwnerDrawer);
  document.getElementById("owner-close-button").addEventListener("click", closeOwnerDrawer);
  document.getElementById("owner-open-settings-button").addEventListener("click", () => switchTab("settings"));
  document.getElementById("owner-logout-button").addEventListener("click", logout);
  document.getElementById("owner-copy-secret-button").addEventListener("click", async () => {
    const copied = await copyToClipboard(document.getElementById("owner-provision-secret").value);
    setStatus(
      copied ? "Manual key copied." : "Could not copy manual key.",
      copied ? "success" : "warning",
      copied ? "You can now paste it into an authenticator app." : "Copy the key manually from the Authenticator Setup card.",
    );
  });
  document.getElementById("owner-copy-uri-button").addEventListener("click", async () => {
    const copied = await copyToClipboard(document.getElementById("owner-provision-uri").value);
    setStatus(
      copied ? "Provisioning link copied." : "Could not copy provisioning link.",
      copied ? "success" : "warning",
      copied ? "You can share or open the provisioning link directly." : "Copy the provisioning link manually from the Authenticator Setup card.",
    );
  });
  document.getElementById("create-recruiter-form").addEventListener("submit", handleCreateRecruiter);
  document.getElementById("owner-user-list").addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const revealUserId = target.dataset.userShowTotp;
    const rotateUserId = target.dataset.userRotateTotp;
    if (revealUserId) {
      handleRevealUserTotp(revealUserId, false);
    }
    if (rotateUserId) {
      handleRevealUserTotp(rotateUserId, true);
    }
  });
  document.getElementById("load-demo-button").addEventListener("click", loadDemoBrief);
  document.getElementById("owner-train-ranker-button").addEventListener("click", trainRanker);
  document.getElementById("train-ranker-button").addEventListener("click", trainRanker);
  bindOwnerToggle("owner-semantic-enabled", "settings-semantic-enabled", "reranker_enabled", "HR Hunter AI Match Scoring is now updated.");
  bindOwnerToggle("owner-feedback-model-enabled", "settings-feedback-model-enabled", "learned_ranker_enabled", "The feedback-trained model setting is now updated.");
  bindOwnerToggle("owner-history-context-enabled", "settings-history-context-enabled", "include_history_slices", "Previous search context is now updated.");
  bindOwnerToggle("owner-discovery-enabled", "", "include_discovery_slices", "Broader discovery search is now updated.");
  bindOwnerToggle("owner-memory-enabled", "", "registry_memory_enabled", "Candidate memory reuse is now updated.");
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeNav();
      closeOwnerDrawer();
    }
  });
}

async function initialiseApp() {
  bindEvents();
  state.config = await fetchJSON("/app-config");
  applyAuthConfig();
  hydrateSettings();
  initialiseTokenFields();
  renderProjectStatuses();
  renderThemeToggle();
  renderProviderOptions();
  renderAnchorGrid();
  renderUploadedJdSummary();
  populateSettingsFields();
  await restoreSession();
}

window.addEventListener("DOMContentLoaded", () => {
  initialiseApp().catch((error) => {
    showAuthShell();
    const message = document.getElementById("login-message");
    if (message) {
      message.textContent = error.message || "The app could not be loaded.";
    }
  });
});
