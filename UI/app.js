const STORAGE_KEY = "hr-hunter-ui-settings";
const TAB_META = {
  search: {
    title: "Recruiter",
    description: "Build the search brief, break down the JD, and prepare the candidate run.",
  },
  results: {
    title: "Results",
    description: "Review ranked candidates, CSV exports, and score explanations.",
  },
  feedback: {
    title: "Feedback",
    description: "Save recruiter actions and train the learned ranker from real feedback.",
  },
  settings: {
    title: "Settings",
    description: "Change themes, manage file paths, and send support or feature requests.",
  },
};

const state = {
  config: null,
  currentBreakdown: null,
  currentSearch: null,
  tokenFields: {},
  activeTab: "search",
  navOpen: false,
  developerOpen: false,
};

class TokenField {
  constructor(root, suggestions = []) {
    this.root = root;
    this.tokens = [];
    this.suggestions = suggestions;
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
      this.input.setAttribute("list", listId);
      root.appendChild(datalist);
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
    if (!this.input.value.trim()) return;
    this.add(this.input.value);
    this.input.value = "";
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

function formatJson(value) {
  return JSON.stringify(value, null, 2);
}

function safeNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}

function featureValue(candidate, key) {
  if (candidate?.feature_scores && candidate.feature_scores[key] !== undefined) {
    return safeNumber(candidate.feature_scores[key]);
  }
  return 0;
}

function statusKey(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "verified") return "verified";
  if (normalized === "review" || normalized === "needs_review") return "review";
  return "reject";
}

function getStoredSettings() {
  try {
    return JSON.parse(window.localStorage.getItem(STORAGE_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveStoredSettings(value) {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
}

function clearStoredSettings() {
  window.localStorage.removeItem(STORAGE_KEY);
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const detail = typeof payload === "string" ? payload : payload.detail || "Request failed";
    throw new Error(detail);
  }
  return payload;
}

function setStatus(message, tone = "default", detail = "") {
  const line = document.getElementById("status-line");
  const extra = document.getElementById("status-detail");
  const statusCard = document.querySelector(".status-card");
  line.textContent = message;
  extra.textContent = detail || TAB_META[state.activeTab]?.description || "";
  statusCard.dataset.tone = tone;
}

function closeNavDrawer() {
  state.navOpen = false;
  document.getElementById("nav-drawer").hidden = true;
  document.getElementById("nav-backdrop").hidden = true;
  document.body.classList.remove("nav-open");
}

function openNavDrawer() {
  state.navOpen = true;
  document.getElementById("nav-drawer").hidden = false;
  document.getElementById("nav-backdrop").hidden = false;
  document.body.classList.add("nav-open");
}

function toggleNavDrawer() {
  if (state.navOpen) {
    closeNavDrawer();
    return;
  }
  openNavDrawer();
}

function closeDeveloperPanel() {
  state.developerOpen = false;
  document.getElementById("developer-panel").hidden = true;
  document.getElementById("developer-backdrop").hidden = true;
  document.body.classList.remove("developer-open");
}

function openDeveloperPanel() {
  state.developerOpen = true;
  document.getElementById("developer-panel").hidden = false;
  document.getElementById("developer-backdrop").hidden = false;
  document.body.classList.add("developer-open");
}

function toggleDeveloperPanel() {
  if (state.developerOpen) {
    closeDeveloperPanel();
    return;
  }
  openDeveloperPanel();
}

function switchTab(tabId) {
  state.activeTab = tabId;
  document.querySelectorAll(".nav-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.tab === tabId);
  });
  document.querySelectorAll("[data-tab-panel]").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.tabPanel === tabId);
  });
  const meta = TAB_META[tabId] || TAB_META.search;
  document.getElementById("tab-title").textContent = meta.title;
  document.getElementById("tab-description").textContent = meta.description;
  setStatus(document.getElementById("status-line").textContent || "Workspace ready.", "default", meta.description);
  closeNavDrawer();
}

function applyTheme(themeId) {
  const allowedThemes = new Set((state.config?.themes || []).map((theme) => theme.id));
  const fallbackTheme = state.config?.defaults?.theme || "bright";
  const theme = allowedThemes.has(themeId) ? themeId : fallbackTheme;
  document.body.dataset.theme = theme;
  document.querySelectorAll(".theme-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.theme === theme);
  });
}

function createTokenFields(config) {
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

function renderThemeOptions(config) {
  const box = document.getElementById("theme-options");
  box.innerHTML = "";
  (config.themes || []).forEach((theme) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "theme-button";
    button.dataset.theme = theme.id;
    button.innerHTML = `<strong>${escapeHtml(theme.label)}</strong><span>${escapeHtml(theme.description)}</span>`;
    button.addEventListener("click", () => applyTheme(theme.id));
    box.appendChild(button);
  });
}

function renderAnchors(config) {
  const grid = document.getElementById("anchor-grid");
  grid.innerHTML = "";
  const levels = [
    { value: "critical", label: "Critical" },
    { value: "important", label: "Important" },
    { value: "preferred", label: "Preferred" },
    { value: "ignore", label: "Ignore" },
  ];
  (config.anchors || []).forEach((anchor) => {
    const card = document.createElement("div");
    card.className = "anchor-card";
    const select = document.createElement("select");
    select.id = `anchor-${anchor.id}`;
    levels.forEach((level) => {
      const option = document.createElement("option");
      option.value = level.value;
      option.textContent = level.label;
      if ((anchor.default || "preferred") === level.value) option.selected = true;
      select.appendChild(option);
    });
    card.innerHTML = `<label for="anchor-${anchor.id}">${escapeHtml(anchor.label)}</label><p>${escapeHtml(anchor.description)}</p>`;
    card.appendChild(select);
    grid.appendChild(card);
  });
}

function renderProviders(config) {
  const box = document.getElementById("provider-box");
  box.innerHTML = "";
  (config.providers || []).forEach((provider) => {
    const displayName = provider === "mock" ? "mock (developer only)" : provider;
    const label = document.createElement("label");
    label.className = "provider-option";
    label.innerHTML = `<input type="checkbox" name="providers" value="${escapeHtml(provider)}"><span>${escapeHtml(displayName)}</span>`;
    box.appendChild(label);
  });
}

function collectAnchors() {
  const anchors = {};
  (state.config.anchors || []).forEach((anchor) => {
    const element = document.getElementById(`anchor-${anchor.id}`);
    if (!element) return;
    const value = element.value;
    if (value && value !== "ignore") {
      anchors[anchor.id] = value;
    }
  });
  return anchors;
}

function currentProviderSelection() {
  const selected = Array.from(document.querySelectorAll('input[name="providers"]:checked')).map((input) => input.value);
  return selected.length ? selected : ["scrapingbee_google"];
}

function collectPayload() {
  const rowLimit = document.getElementById("row-limit").value;
  return {
    role_title: document.getElementById("role-title").value.trim(),
    titles: state.tokenFields.titles.getTokens(),
    years_mode: document.getElementById("years-mode").value,
    years_value: document.getElementById("years-value").value,
    years_tolerance: document.getElementById("years-tolerance").value,
    minimum_years_experience: document.getElementById("min-years").value,
    maximum_years_experience: document.getElementById("max-years").value,
    radius_miles: document.getElementById("radius-miles").value,
    countries: state.tokenFields.countries.getTokens(),
    continents: state.tokenFields.continents.getTokens(),
    cities: state.tokenFields.cities.getTokens(),
    company_targets: state.tokenFields.companies.getTokens(),
    company_match_mode: document.getElementById("company-match-mode").value,
    job_description: document.getElementById("job-description").value,
    must_have_keywords: state.tokenFields.mustHave.getTokens(),
    nice_to_have_keywords: state.tokenFields.niceToHave.getTokens(),
    industry_keywords: state.tokenFields.industry.getTokens(),
    exclude_title_keywords: state.tokenFields.excludeTitles.getTokens(),
    exclude_company_keywords: state.tokenFields.excludeCompanies.getTokens(),
    anchors: collectAnchors(),
    providers: currentProviderSelection(),
    reranker_enabled: document.getElementById("reranker-enabled").checked,
    learned_ranker_enabled: document.getElementById("learned-ranker-enabled").checked,
    limit: rowLimit,
    csv_export_limit: rowLimit,
    feedback_db: document.getElementById("feedback-db").value,
    model_dir: document.getElementById("model-dir").value,
    output_dir: document.getElementById("output-dir").value,
    jd_breakdown: state.currentBreakdown,
  };
}

function renderBreakdown(breakdown) {
  const container = document.getElementById("jd-breakdown");
  if (!breakdown) {
    container.className = "breakdown-empty";
    container.textContent = "No JD breakdown yet. Paste a job description and use Break down JD.";
    return;
  }
  container.className = "breakdown-grid";
  const keyPoints = (breakdown.key_experience_points || []).map((item) => `<li>${escapeHtml(item)}</li>`).join("");
  const required = (breakdown.required_keywords || []).map((item) => `<span class="tag">${escapeHtml(item)}</span>`).join("");
  const preferred = (breakdown.preferred_keywords || []).map((item) => `<span class="tag">${escapeHtml(item)}</span>`).join("");
  const industries = (breakdown.industry_keywords || []).map((item) => `<span class="tag">${escapeHtml(item)}</span>`).join("");
  const years = breakdown.years || {};
  const yearText = years.min || years.max || years.value
    ? `${years.mode} | min: ${years.min ?? "-"} | max: ${years.max ?? "-"}`
    : "No clear years signal found";
  container.innerHTML = `
    <div class="breakdown-block">
      <p class="field-label">Summary</p>
      <p>${escapeHtml(breakdown.summary || "")}</p>
      <p class="field-label">Key experience points</p>
      <ul>${keyPoints || "<li>No strong signals extracted yet.</li>"}</ul>
    </div>
    <div class="breakdown-block">
      <p class="field-label">Required keywords</p>
      <div class="candidate-tags">${required || "<span class='tag'>None detected</span>"}</div>
      <p class="field-label">Preferred keywords</p>
      <div class="candidate-tags">${preferred || "<span class='tag'>None detected</span>"}</div>
      <p class="field-label">Industry hints</p>
      <div class="candidate-tags">${industries || "<span class='tag'>None detected</span>"}</div>
      <p class="field-label">Years signal</p>
      <p>${escapeHtml(yearText)}</p>
    </div>
  `;
}

function renderSummary(summary) {
  const grid = document.getElementById("summary-grid");
  if (!summary) {
    grid.innerHTML = "<article class='summary-card empty'><p class='eyebrow'>No run yet</p><strong>0</strong><h3>Search results will appear here after a run.</h3></article>";
    return;
  }
  const counts = summary.verification_status_counts || {};
  const cards = [
    { label: "Candidates", value: summary.candidate_count || 0, detail: "Profiles returned" },
    { label: "Verified", value: counts.verified || summary.verified_count || 0, detail: "70.00 - 100.00" },
    { label: "Needs review", value: counts.review || summary.review_count || 0, detail: "50.00 - 69.99" },
    { label: "Rejected", value: counts.reject || summary.reject_count || 0, detail: "0.00 - 49.99" },
  ];
  grid.innerHTML = cards
    .map((card) => `<article class="summary-card"><p class="eyebrow">${escapeHtml(card.label)}</p><strong>${escapeHtml(card.value)}</strong><h3>${escapeHtml(card.detail)}</h3></article>`)
    .join("");
}

function renderArtifacts(response) {
  const panel = document.getElementById("artifacts-panel");
  if (!response?.report_paths?.csv) {
    panel.innerHTML = "<p class='panel-note'>Run a search to generate the CSV export.</p>";
    return;
  }
  const csvLink = `/app/artifact?path=${encodeURIComponent(response.report_paths.csv)}`;
  panel.innerHTML = `
    <div class="artifact-links">
      <a class="artifact-link" href="${csvLink}" target="_blank" rel="noreferrer">Download CSV</a>
    </div>
    <p class="panel-note">Rows exported: ${escapeHtml(response.csv_export_limit || 0)}</p>
    <p class="panel-note">${escapeHtml(response.report_paths.csv)}</p>
  `;
}

function statusLabel(status) {
  if (statusKey(status) === "verified") return "Verified";
  if (statusKey(status) === "review") return "Needs review";
  return "Rejected";
}

function statusClass(status) {
  if (statusKey(status) === "verified") return "status-verified";
  if (statusKey(status) === "review") return "status-review";
  return "status-reject";
}

function renderCandidateLinks(candidate) {
  const links = [];
  if (candidate.linkedin_url) links.push(`<a href="${escapeHtml(candidate.linkedin_url)}" target="_blank" rel="noreferrer">LinkedIn</a>`);
  if (candidate.source_url) links.push(`<a href="${escapeHtml(candidate.source_url)}" target="_blank" rel="noreferrer">Source</a>`);
  return links.join("");
}

function renderMetrics(candidate) {
  const metrics = [
    ["Title", featureValue(candidate, "title_similarity")],
    ["Company", featureValue(candidate, "company_match")],
    ["Location", featureValue(candidate, "location_match")],
    ["Skills", featureValue(candidate, "skill_overlap")],
    ["Industry", featureValue(candidate, "industry_fit")],
    ["Years", featureValue(candidate, "years_fit")],
    ["Semantic", featureValue(candidate, "semantic_similarity")],
  ];
  return metrics
    .map(([label, value]) => `
      <div class="metric-card">
        <span>${escapeHtml(label)}</span>
        <strong>${safeNumber(value).toFixed(2)}</strong>
        <div class="meter"><i style="width:${Math.max(0, Math.min(100, safeNumber(value) * 100))}%"></i></div>
      </div>
    `)
    .join("");
}

function renderContributionChips(candidate) {
  const contributions = Object.entries(candidate.anchor_scores || {})
    .filter(([, value]) => safeNumber(value) > 0)
    .sort((left, right) => safeNumber(right[1]) - safeNumber(left[1]))
    .slice(0, 8);
  if (!contributions.length) {
    return "<span class='tag'>No anchor contribution recorded</span>";
  }
  return contributions
    .map(([key, value]) => `<span class="contribution-chip"><span>${escapeHtml(key.replaceAll("_", " "))}</span><strong>${safeNumber(value).toFixed(2)}</strong></span>`)
    .join("");
}

function renderCandidateTags(candidate) {
  const tags = [];
  (candidate.matched_titles || []).slice(0, 3).forEach((value) => tags.push(`Title: ${value}`));
  (candidate.matched_companies || []).slice(0, 3).forEach((value) => tags.push(`Company: ${value}`));
  (candidate.cap_reasons || []).slice(0, 3).forEach((value) => tags.push(`Cap: ${value}`));
  if (safeNumber(candidate.reranker_score) > 0) {
    tags.push(`Reranker: ${safeNumber(candidate.reranker_score).toFixed(2)}`);
  }
  return tags.map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("");
}

function feedbackOptionsMarkup(actions) {
  return actions.map((action) => `<option value="${escapeHtml(action)}">${escapeHtml(action)}</option>`).join("");
}

async function handleFeedbackSave(candidate, card) {
  if (!state.currentSearch?.report_paths?.json) {
    throw new Error("Run a search first so the app has a report to attach feedback to.");
  }
  const recruiterId = document.getElementById("recruiter-id").value.trim();
  if (!recruiterId) {
    throw new Error("Add a recruiter ID in the Feedback tab before saving feedback.");
  }
  const payload = {
    report_path: state.currentSearch.report_paths.json,
    candidate_ref: candidate.linkedin_url || candidate.source_url || candidate.full_name,
    recruiter_id: recruiterId,
    recruiter_name: document.getElementById("recruiter-name").value.trim(),
    team_id: document.getElementById("team-id").value.trim(),
    action: card.querySelector(".feedback-action").value,
    reason_code: card.querySelector(".feedback-reason").value.trim(),
    note: card.querySelector(".feedback-note").value.trim(),
    feedback_db: document.getElementById("feedback-db").value.trim(),
    brief: state.currentSearch.brief,
  };
  const result = await fetchJSON("/app/feedback", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  card.querySelector(".feedback-status").textContent = `Saved: ${payload.action}`;
  if (result.feedback_db) {
    document.getElementById("feedback-db").value = result.feedback_db;
  }
  renderFeedbackSummary(state.currentSearch);
}

function renderCandidates(response) {
  const list = document.getElementById("candidate-results");
  const candidates = response?.candidates || [];
  if (!candidates.length) {
    list.innerHTML = "<article class='empty-card'><h4>No candidates yet</h4><p>Run a search from the Recruiter tab to populate this view.</p></article>";
    return;
  }
  const template = document.getElementById("candidate-template");
  list.innerHTML = "";
  candidates.forEach((candidate) => {
    const fragment = template.content.cloneNode(true);
    const card = fragment.querySelector(".candidate-card");
    fragment.querySelector(".candidate-name").textContent = candidate.full_name || "Unnamed candidate";
    fragment.querySelector(".candidate-meta").textContent = [candidate.current_title, candidate.current_company, candidate.location_name].filter(Boolean).join(" | ");
    fragment.querySelector(".candidate-links").innerHTML = renderCandidateLinks(candidate);
    fragment.querySelector(".status-badge").textContent = statusLabel(candidate.verification_status);
    fragment.querySelector(".status-badge").classList.add(statusClass(candidate.verification_status));
    fragment.querySelector(".candidate-score").textContent = safeNumber(candidate.score).toFixed(2);
    fragment.querySelector(".candidate-model").textContent = `Model: ${candidate.ranking_model_version || "heuristic"}${safeNumber(candidate.reranker_score) ? ` | reranker ${safeNumber(candidate.reranker_score).toFixed(2)}` : ""}`;
    fragment.querySelector(".metric-grid").innerHTML = renderMetrics(candidate);
    fragment.querySelector(".contribution-list").innerHTML = renderContributionChips(candidate);
    fragment.querySelector(".candidate-notes").textContent = (candidate.verification_notes || []).slice(0, 8).join(" | ") || "No scoring notes recorded.";
    fragment.querySelector(".candidate-tags").innerHTML = renderCandidateTags(candidate);
    fragment.querySelector(".feedback-action").innerHTML = feedbackOptionsMarkup(state.config.feedback_actions || []);
    fragment.querySelector(".feedback-save").addEventListener("click", async () => {
      try {
        await handleFeedbackSave(candidate, card);
        setStatus(`Feedback saved for ${candidate.full_name}.`, "success", TAB_META.results.description);
      } catch (error) {
        setStatus(error.message, "error", TAB_META.results.description);
      }
    });
    list.appendChild(fragment);
  });
}

function renderFeedbackSummary(response) {
  const box = document.getElementById("feedback-summary");
  if (!response?.report_paths) {
    box.innerHTML = "<p class='panel-note'>Once a search finishes, this tab will show the active report reference and feedback database.</p>";
    return;
  }
  box.innerHTML = `
    <p><strong>Run reference:</strong> ${escapeHtml(response.report_paths.json)}</p>
    <p><strong>CSV export:</strong> ${escapeHtml(response.report_paths.csv)}</p>
    <p><strong>Feedback DB:</strong> ${escapeHtml(document.getElementById("feedback-db").value.trim())}</p>
    <p><strong>Model directory:</strong> ${escapeHtml(document.getElementById("model-dir").value.trim())}</p>
    <p><strong>Candidate count:</strong> ${escapeHtml(response.summary?.candidate_count || 0)}</p>
  `;
}

function renderDeveloperState(payload = null, response = null) {
  document.getElementById("debug-brief").textContent = payload ? formatJson(payload) : "No brief collected yet.";
  document.getElementById("debug-summary").textContent = response
    ? formatJson({ summary: response.summary, report_paths: response.report_paths, brief: response.brief })
    : "No search response yet.";
  document.getElementById("debug-providers").textContent = response?.provider_results
    ? formatJson(response.provider_results)
    : "No provider diagnostics yet.";
}

function fillFromPreset(preset) {
  document.getElementById("role-title").value = preset.role_title || "";
  document.getElementById("years-mode").value = preset.years_mode || "plus_minus";
  document.getElementById("years-value").value = preset.years_value || "";
  document.getElementById("years-tolerance").value = preset.years_tolerance ?? 2;
  document.getElementById("min-years").value = preset.minimum_years_experience || "";
  document.getElementById("max-years").value = preset.maximum_years_experience || "";
  document.getElementById("company-match-mode").value = preset.company_match_mode || "both";
  document.getElementById("job-description").value = preset.job_description || "";
  state.tokenFields.titles.setTokens(preset.titles || []);
  state.tokenFields.countries.setTokens(preset.countries || []);
  state.tokenFields.continents.setTokens(preset.continents || []);
  state.tokenFields.cities.setTokens(preset.cities || []);
  state.tokenFields.companies.setTokens(preset.company_targets || []);
  state.tokenFields.mustHave.setTokens(preset.must_have_keywords || []);
  state.tokenFields.niceToHave.setTokens(preset.nice_to_have_keywords || []);
  state.tokenFields.industry.setTokens(preset.industry_keywords || []);
  state.tokenFields.excludeTitles.setTokens(preset.exclude_title_keywords || []);
  state.tokenFields.excludeCompanies.setTokens(preset.exclude_company_keywords || []);
  (state.config.anchors || []).forEach((anchor) => {
    const select = document.getElementById(`anchor-${anchor.id}`);
    if (!select) return;
    select.value = preset.anchors?.[anchor.id] || anchor.default || "preferred";
  });
  state.currentBreakdown = null;
  renderBreakdown(null);
  renderDeveloperState(collectPayload(), state.currentSearch);
  switchTab("search");
}

function resetBriefForm() {
  document.getElementById("role-title").value = "";
  document.getElementById("years-mode").value = "plus_minus";
  document.getElementById("years-value").value = "";
  document.getElementById("years-tolerance").value = 2;
  document.getElementById("min-years").value = "";
  document.getElementById("max-years").value = "";
  document.getElementById("radius-miles").value = state.config?.defaults?.radius_miles || 25;
  document.getElementById("company-match-mode").value = state.config?.defaults?.company_match_mode || "both";
  document.getElementById("job-description").value = "";
  Object.values(state.tokenFields).forEach((field) => field.setTokens([]));
  (state.config.anchors || []).forEach((anchor) => {
    const select = document.getElementById(`anchor-${anchor.id}`);
    if (select) {
      select.value = anchor.default || "preferred";
    }
  });
  state.currentBreakdown = null;
  renderBreakdown(null);
  renderDeveloperState(collectPayload(), state.currentSearch);
}

function buildSettingsSnapshot() {
  return {
    theme: document.body.dataset.theme || state.config?.defaults?.theme || "bright",
    feedback_db: document.getElementById("feedback-db").value,
    model_dir: document.getElementById("model-dir").value,
    output_dir: document.getElementById("output-dir").value,
    recruiter_id: document.getElementById("recruiter-id").value,
    recruiter_name: document.getElementById("recruiter-name").value,
    team_id: document.getElementById("team-id").value,
    providers: currentProviderSelection(),
    reranker_enabled: document.getElementById("reranker-enabled").checked,
    learned_ranker_enabled: document.getElementById("learned-ranker-enabled").checked,
    row_limit: document.getElementById("row-limit").value,
    company_match_mode: document.getElementById("company-match-mode").value,
  };
}

function applySettingsSnapshot(snapshot) {
  if (!snapshot) return;
  const allowedThemes = new Set((state.config?.themes || []).map((theme) => theme.id));
  if (snapshot.theme && allowedThemes.has(snapshot.theme)) {
    applyTheme(snapshot.theme);
  }
  if (snapshot.feedback_db !== undefined) document.getElementById("feedback-db").value = snapshot.feedback_db;
  if (snapshot.model_dir !== undefined) document.getElementById("model-dir").value = snapshot.model_dir;
  if (snapshot.output_dir !== undefined) document.getElementById("output-dir").value = snapshot.output_dir;
  if (snapshot.recruiter_id !== undefined) document.getElementById("recruiter-id").value = snapshot.recruiter_id;
  if (snapshot.recruiter_name !== undefined) document.getElementById("recruiter-name").value = snapshot.recruiter_name;
  if (snapshot.team_id !== undefined) document.getElementById("team-id").value = snapshot.team_id;
  if (snapshot.row_limit !== undefined) document.getElementById("row-limit").value = snapshot.row_limit;
  if (snapshot.company_match_mode) document.getElementById("company-match-mode").value = snapshot.company_match_mode;
  if (snapshot.reranker_enabled !== undefined) document.getElementById("reranker-enabled").checked = Boolean(snapshot.reranker_enabled);
  if (snapshot.learned_ranker_enabled !== undefined) document.getElementById("learned-ranker-enabled").checked = Boolean(snapshot.learned_ranker_enabled);
  if (Array.isArray(snapshot.providers)) {
    document.querySelectorAll('input[name="providers"]').forEach((input) => {
      input.checked = snapshot.providers.includes(input.value);
    });
  }
}

function applyDefaults(config) {
  const defaults = config.defaults || {};
  document.getElementById("row-limit").value = defaults.limit || 20;
  document.getElementById("radius-miles").value = defaults.radius_miles || 25;
  document.getElementById("company-match-mode").value = defaults.company_match_mode || "both";
  document.getElementById("feedback-db").value = defaults.feedback_db || "";
  document.getElementById("model-dir").value = defaults.model_dir || "";
  document.getElementById("output-dir").value = defaults.output_dir || "";
  document.getElementById("reranker-enabled").checked = Boolean(defaults.reranker_enabled);
  document.getElementById("learned-ranker-enabled").checked = Boolean(defaults.learned_ranker_enabled);
  applyTheme(defaults.theme || "bright");
  document.querySelectorAll('input[name="providers"]').forEach((input) => {
    input.checked = (defaults.providers || []).includes(input.value);
  });
  applySettingsSnapshot(getStoredSettings());
  applyTheme(document.body.dataset.theme || defaults.theme || "bright");
}

async function runBreakdown() {
  try {
    setStatus("Breaking down the job description...", "default", TAB_META.search.description);
    const payload = {
      role_title: document.getElementById("role-title").value.trim(),
      job_description: document.getElementById("job-description").value,
    };
    const response = await fetchJSON("/app/jd-breakdown", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.currentBreakdown = response;
    renderBreakdown(response);
    renderDeveloperState(collectPayload(), state.currentSearch);
    setStatus("JD breakdown ready.", "success", TAB_META.search.description);
  } catch (error) {
    setStatus(error.message, "error", TAB_META.search.description);
  }
}

async function runSearch() {
  try {
    setStatus("Running search and grading candidates...", "default", TAB_META.search.description);
    const payload = collectPayload();
    renderDeveloperState(payload, state.currentSearch);
    const response = await fetchJSON("/app/search", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.currentSearch = response;
    state.currentBreakdown = response.jd_breakdown || state.currentBreakdown;
    if (response.feedback_db) document.getElementById("feedback-db").value = response.feedback_db;
    if (response.model_dir) document.getElementById("model-dir").value = response.model_dir;
    renderSummary(response.summary);
    renderArtifacts(response);
    renderBreakdown(state.currentBreakdown);
    renderCandidates(response);
    renderFeedbackSummary(response);
    renderDeveloperState(payload, response);
    switchTab("results");
    setStatus(`Search complete. ${response.summary?.candidate_count || 0} candidates returned.`, "success", TAB_META.results.description);
  } catch (error) {
    setStatus(error.message, "error", TAB_META.search.description);
  }
}

async function trainRanker() {
  try {
    setStatus("Training learned ranker from recruiter feedback...", "default", TAB_META.feedback.description);
    const response = await fetchJSON("/app/train-ranker", {
      method: "POST",
      body: JSON.stringify({
        feedback_db: document.getElementById("feedback-db").value.trim(),
        model_dir: document.getElementById("model-dir").value.trim(),
        n_estimators: document.getElementById("train-estimators").value,
        num_leaves: document.getElementById("train-num-leaves").value,
      }),
    });
    document.getElementById("learned-ranker-enabled").checked = true;
    document.getElementById("model-dir").value = response.model_dir || document.getElementById("model-dir").value;
    setStatus(`Learned ranker trained with ${response.training_row_count} rows.`, "success", TAB_META.feedback.description);
  } catch (error) {
    setStatus(error.message, "error", TAB_META.feedback.description);
  }
}

async function submitSupportRequest() {
  try {
    const payload = {
      name: document.getElementById("support-name").value.trim(),
      contact: document.getElementById("support-contact").value.trim(),
      topic: document.getElementById("support-topic").value.trim(),
      message: document.getElementById("support-message").value.trim(),
      report_path: state.currentSearch?.report_paths?.json || "",
      workspace_root: state.config?.paths?.workspace_root || "",
    };
    if (!payload.topic || !payload.message) {
      throw new Error("Add both a support topic and message.");
    }
    const response = await fetchJSON("/app/support-request", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    document.getElementById("support-status").textContent = `Support request saved to ${response.saved_to}`;
    setStatus("Support request saved.", "success", TAB_META.settings.description);
  } catch (error) {
    document.getElementById("support-status").textContent = error.message;
    setStatus(error.message, "error", TAB_META.settings.description);
  }
}

async function submitFeatureRequest() {
  try {
    const payload = {
      title: document.getElementById("feature-title").value.trim(),
      why: document.getElementById("feature-why").value.trim(),
      details: document.getElementById("feature-message").value.trim(),
      report_path: state.currentSearch?.report_paths?.json || "",
      workspace_root: state.config?.paths?.workspace_root || "",
    };
    if (!payload.title || !payload.why || !payload.details) {
      throw new Error("Add a feature title, why it matters, and the details.");
    }
    const response = await fetchJSON("/app/feature-request", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    document.getElementById("feature-status").textContent = `Feature request saved to ${response.saved_to}`;
    setStatus("Feature request saved.", "success", TAB_META.settings.description);
  } catch (error) {
    document.getElementById("feature-status").textContent = error.message;
    setStatus(error.message, "error", TAB_META.settings.description);
  }
}

function saveSettings() {
  const snapshot = buildSettingsSnapshot();
  saveStoredSettings(snapshot);
  applyTheme(snapshot.theme || "bright");
  setStatus("Settings saved locally for this app.", "success", TAB_META.settings.description);
}

function resetSettings() {
  clearStoredSettings();
  applyDefaults(state.config);
  setStatus("Settings reset to app defaults.", "success", TAB_META.settings.description);
}

function isOwnerSession() {
  return ["127.0.0.1", "localhost"].includes(window.location.hostname);
}

function bindEvents() {
  document.getElementById("menu-toggle").addEventListener("click", toggleNavDrawer);
  document.getElementById("drawer-close").addEventListener("click", closeNavDrawer);
  document.getElementById("nav-backdrop").addEventListener("click", closeNavDrawer);
  document.getElementById("developer-close").addEventListener("click", closeDeveloperPanel);
  document.getElementById("developer-backdrop").addEventListener("click", closeDeveloperPanel);
  if (isOwnerSession()) {
    const ownerFab = document.getElementById("owner-fab");
    ownerFab.hidden = false;
    ownerFab.addEventListener("click", toggleDeveloperPanel);
  }
  document.querySelectorAll(".nav-button").forEach((button) => {
    button.addEventListener("click", () => switchTab(button.dataset.tab));
  });
  document.getElementById("header-breakdown-button").addEventListener("click", runBreakdown);
  document.getElementById("header-search-button").addEventListener("click", runSearch);
  document.getElementById("load-example-button").addEventListener("click", () => {
    fillFromPreset(state.config.presets?.senior_data_analyst_uae || {});
    setStatus("UAE demo brief loaded. You can edit it before running.", "success", TAB_META.search.description);
  });
  document.getElementById("reset-brief-button").addEventListener("click", () => {
    resetBriefForm();
    setStatus("Recruiter brief cleared.", "success", TAB_META.search.description);
  });
  document.getElementById("train-ranker-button").addEventListener("click", trainRanker);
  document.getElementById("save-settings-button").addEventListener("click", saveSettings);
  document.getElementById("reset-settings-button").addEventListener("click", resetSettings);
  document.getElementById("support-submit-button").addEventListener("click", submitSupportRequest);
  document.getElementById("feature-submit-button").addEventListener("click", submitFeatureRequest);
  document.getElementById("job-description").addEventListener("input", () => {
    state.currentBreakdown = null;
    renderBreakdown(null);
    renderDeveloperState(collectPayload(), state.currentSearch);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeNavDrawer();
      closeDeveloperPanel();
      return;
    }
    if (event.ctrlKey && event.shiftKey && event.key.toLowerCase() === "d") {
      event.preventDefault();
      toggleDeveloperPanel();
    }
  });
}

async function init() {
  try {
    setStatus("Loading recruiter workspace...", "default", TAB_META.search.description);
    state.config = await fetchJSON("/app-config");
    createTokenFields(state.config);
    renderThemeOptions(state.config);
    renderAnchors(state.config);
    renderProviders(state.config);
    applyDefaults(state.config);
    renderSummary(null);
    renderArtifacts(null);
    renderBreakdown(null);
    renderCandidates(null);
    renderFeedbackSummary(null);
    renderDeveloperState(null, null);
    bindEvents();
    switchTab("search");
    setStatus("Workspace ready.", "success", TAB_META.search.description);
  } catch (error) {
    setStatus(`Failed to load app: ${error.message}`, "error", TAB_META.search.description);
  }
}

window.addEventListener("DOMContentLoaded", init);
