/* Radiology Impression Rater v7
 * - Professional UI with improved accessibility
 * - TWO MODES: Data Quality Assessment + Model Output Evaluation
 * - Data Quality: CoT + Hardness evaluation (1-5 scale), fixed cases
 * - Model Evaluation: Fixed cases, user-seeded order/models
 * - Supabase sync + localStorage backup
 *
 * Important deployment note:
 * - Must be served over http(s) (GitHub Pages)
 * - Opening index.html via file:// will cause fetch() to fail
 */

// ===== SUPABASE CONFIG =====
const SUPABASE_URL = "https://oqfbijskgpfqhbonbjww.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9xZmJpanNrZ3BmcWhib25iand3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk5MzIxMTksImV4cCI6MjA4NTUwODExOX0.9lbI8zWpPRytiO5j__DeniQ73d0ouuvsj17RDFsS_4s";

const DATASETS = [
  { key: "rexgradient", label: "RexGradient", file: "data/rexgradient_all_predictions_final.csv" },
  { key: "chexpert", label: "CheXpert", file: "data/chexpert_all_predictions_final.csv" },
  { key: "mimic", label: "MIMIC-CXR", file: "data/mimic_all_predictions_final.csv" },
];

// For data quality assessment - uses deepseek_cot_out.csv with hardness labels and CoT
const DATA_QUALITY_DATASET = { key: "data_quality", label: "Data Quality Assessment", file: "data/deepseek_cot_out.csv" };

// For CoT quality evaluation - uses merged CSV files with raw CoT outputs
const COT_DATASETS = [
  { key: "rexgradient", label: "RexGradient", file: "data/rexgradient_all_predictions_merged.csv", cotCol: "qwen3_8b_rexgradient_rl_raw" },
  { key: "chexpert", label: "CheXpert", file: "data/chexpert_all_predictions_merged.csv", cotCol: "qwen3_8b_chexpert_rl_raw" },
  { key: "mimic", label: "MIMIC-CXR", file: "data/mimic_all_predictions_merged.csv", cotCol: "qwen3_8b_mimic_rl_raw" },
];

const MODEL_COLUMNS = [
  { key: "huatuo", col: "HuatuoGPT_o1_8B_impression", display: "Huatuo" },
  { key: "m1", col: "m1_7B_23K_impression", display: "m1" },
  { key: "medreason", col: "MedReason_8B_impression", display: "MedReason" },
  { key: "qwen8b_zs", col: "qwen3_8b_base_impression", display: "Qwen3 8B Zero Shot" },
  { key: "qwen8b_nocot", col: "qwen3_8b_nocot_impression", display: "Qwen3 8B No-Cot" },
  { key: "qwen8b_sft", col: "qwen3_8b_lora_impression", display: "Qwen3 8B SFT only" },
  { key: "qwen8b_rl", col: "qwen3_8b_lora_rl_impression", display: "Qwen3 8B SFT+RL" },
];

const STORAGE_PREFIX = "rad_rater_v4";
const AUTO_SAVE_INTERVAL = 30000; // 30 seconds

function $(id) { return document.getElementById(id); }

function stableHash(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0);
}
function mulberry32(seed) {
  return function () {
    let t = seed += 0x6D2B79F5;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
function seededSample(array, n, seedStr) {
  const seed = stableHash(seedStr);
  const rnd = mulberry32(seed);
  const idx = array.map((_, i) => i);
  for (let i = idx.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1));
    [idx[i], idx[j]] = [idx[j], idx[i]];
  }
  return idx.slice(0, Math.min(n, idx.length)).map(i => array[i]);
}

function setHidden(el, hidden) { el.classList.toggle("hidden", hidden); }
function nowISO() { return new Date().toISOString(); }
function storageKey(userId) { return `${STORAGE_PREFIX}:${userId}`; }

function loadState(userId) {
  try {
    const raw = localStorage.getItem(storageKey(userId));
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (e) { console.warn("loadState failed", e); return null; }
}
function saveState(userId, state) {
  // localStorage is now only a backup, primary source is Supabase
  localStorage.setItem(storageKey(userId), JSON.stringify(state));
}

// Clear all localStorage cache on page load (force clean slate)
function clearAllLocalStorageCache() {
  const keys = [];
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && key.startsWith(STORAGE_PREFIX)) {
      keys.push(key);
    }
  }
  keys.forEach(key => {
    console.log("Clearing old localStorage cache:", key);
    localStorage.removeItem(key);
  });
}

// ===== SUPABASE FUNCTIONS =====
async function supabaseGet(userId) {
  try {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/ratings?user_id=eq.${encodeURIComponent(userId)}&select=*`, {
      headers: {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
      }
    });
    if (!res.ok) throw new Error(`Supabase GET failed: ${res.status}`);
    const rows = await res.json();
    if (rows.length > 0) {
      return rows[0].data;
    }
    return null;
  } catch (e) {
    console.warn('Supabase GET error:', e);
    return null;
  }
}

async function supabaseSave(userId, state) {
  try {
    // First check if user exists
    const existing = await fetch(`${SUPABASE_URL}/rest/v1/ratings?user_id=eq.${encodeURIComponent(userId)}&select=id`, {
      headers: {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
      }
    });
    const rows = await existing.json();

    const payload = {
      user_id: userId,
      data: state,
      updated_at: new Date().toISOString()
    };

    let res;
    if (rows.length > 0) {
      // Update existing
      res = await fetch(`${SUPABASE_URL}/rest/v1/ratings?user_id=eq.${encodeURIComponent(userId)}`, {
        method: 'PATCH',
        headers: {
          'apikey': SUPABASE_ANON_KEY,
          'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
          'Content-Type': 'application/json',
          'Prefer': 'return=minimal'
        },
        body: JSON.stringify(payload)
      });
    } else {
      // Insert new
      res = await fetch(`${SUPABASE_URL}/rest/v1/ratings`, {
        method: 'POST',
        headers: {
          'apikey': SUPABASE_ANON_KEY,
          'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
          'Content-Type': 'application/json',
          'Prefer': 'return=minimal'
        },
        body: JSON.stringify(payload)
      });
    }

    if (!res.ok) throw new Error(`Supabase save failed: ${res.status}`);
    return true;
  } catch (e) {
    console.error('Supabase save error:', e);
    return false;
  }
}

function downloadText(filename, text) {
  const blob = new Blob([text], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(url);
}

async function fetchCsv(file) {
  // Try fetch first, fallback to XMLHttpRequest for file:// protocol
  let text;

  if (location.protocol === "file:") {
    // XMLHttpRequest works with file:// in some browsers
    text = await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("GET", file, true);
      xhr.onload = () => {
        if (xhr.status === 0 || xhr.status === 200) {
          resolve(xhr.responseText);
        } else {
          reject(new Error(`XHR failed (${xhr.status}): ${file}`));
        }
      };
      xhr.onerror = () => reject(new Error(`XHR error loading: ${file}. Chrome blocks file:// requests. Try Firefox or use python -m http.server.`));
      xhr.send();
    });
  } else {
    const res = await fetch(file, { cache: "no-store" });
    if (!res.ok) throw new Error(`CSV fetch failed (${res.status}): ${file}`);
    text = await res.text();
  }

  return new Promise((resolve, reject) => {
    Papa.parse(text, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      complete: (results) => resolve(results.data),
      error: (err) => reject(err),
    });
  });
}

function normalizeRow(row) {
  const safe = {
    // deepseek_cot_out.csv uses standard column names
    id: row.StudyInstanceUid ?? row.id ?? row.ID ?? "",
    findings: row.Findings ?? row.findings ?? "",
    indication: row.Indication ?? row.indication ?? "",
    ground_truth: row.Impression ?? row.impression ?? row.ground_truth ?? "",
    split: row.split ?? row.Split ?? "",
    hardness: row.Hardness ?? row.hardness ?? "",
    cot: row.raw_output ?? row.raw_output_cot ?? row.CoT ?? row.cot ?? row.COT ?? "",
  };
  // Debug log for first few rows
  if (Math.random() < 0.001) {
    console.log("Sample normalized row:", {
      id: safe.id?.substring(0, 30),
      gt_source: row.Impression ? 'Impression' : 'other',
      gt_value: safe.ground_truth?.substring(0, 50),
      cot_source: row.raw_output ? 'raw_output' : (row.raw_output_cot ? 'raw_output_cot' : 'other'),
      cot_value: safe.cot?.substring(0, 50)
    });
  }
  for (const m of MODEL_COLUMNS) {
    safe[m.key] = row[m.col] ?? "";
  }
  return safe;
}

function buildBlindedOrder(userId, datasetKey, caseId) {
  const seedStr = `${userId}::${datasetKey}::${caseId}::modelorder`;
  const seed = stableHash(seedStr);
  const rnd = mulberry32(seed);
  const keys = MODEL_COLUMNS.map(m => m.key);
  for (let i = keys.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1));
    [keys[i], keys[j]] = [keys[j], keys[i]];
  }
  return keys;
}

// ---------- UI State ----------
let STATE = null;
let ACTIVE_DATASET = null;
let ACTIVE_INDEX = 0;
let autoSaveTimer = null;

// Show toast notification
function showToast(message, type = 'info') {
  const existing = document.querySelector('.toast');
  if (existing) existing.remove();

  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      ${type === 'success' ? '<polyline points="20 6 9 17 4 12"/>' :
      type === 'error' ? '<circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/>' :
        '<circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>'}
    </svg>
    <span>${message}</span>
  `;
  document.body.appendChild(toast);

  // Add toast styles if not exists
  if (!document.querySelector('#toast-styles')) {
    const style = document.createElement('style');
    style.id = 'toast-styles';
    style.textContent = `
      .toast {
        position: fixed;
        bottom: 24px;
        left: 50%;
        transform: translateX(-50%) translateY(100px);
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 14px 20px;
        border-radius: 12px;
        background: rgba(20, 25, 40, 0.95);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255,255,255,0.1);
        color: #eaf0ff;
        font-size: 14px;
        font-weight: 500;
        box-shadow: 0 12px 40px rgba(0,0,0,0.4);
        z-index: 9999;
        animation: toastIn 0.3s ease forwards;
      }
      .toast-success { border-color: rgba(43, 228, 167, 0.4); }
      .toast-success svg { color: #2be4a7; }
      .toast-error { border-color: rgba(255, 77, 109, 0.4); }
      .toast-error svg { color: #ff4d6d; }
      .toast-warning { border-color: rgba(232, 169, 78, 0.4); }
      .toast-warning svg { color: #e8a94e; }
      .toast-info svg { color: #5a7bb5; }
      @keyframes toastIn {
        to { transform: translateX(-50%) translateY(0); }
      }
    `;
    document.head.appendChild(style);
  }

  setTimeout(() => {
    toast.style.animation = 'toastIn 0.3s ease reverse forwards';
    setTimeout(() => toast.remove(), 300);
  }, 3000);
}

// Set up click handlers for progress bars to navigate between forms
function setupProgressBarNavigation() {
  // Data Quality screen progress bars
  const hardnessDataQualityBar = document.querySelector('#screenHardness .progress:nth-child(1)');
  const hardnessModelEvalBar = document.querySelector('#screenHardness .progress:nth-child(2)');
  const hardnessCotBar = document.querySelector('#screenHardness .progress:nth-child(3)');
  
  if (hardnessDataQualityBar) {
    hardnessDataQualityBar.style.cursor = 'pointer';
    hardnessDataQualityBar.title = 'Click to go to Data Quality Assessment';
    hardnessDataQualityBar.addEventListener('click', async () => await navigateToForm('data_quality'));
  }
  if (hardnessModelEvalBar) {
    hardnessModelEvalBar.style.cursor = 'pointer';
    hardnessModelEvalBar.title = 'Click to go to Model Evaluation';
    hardnessModelEvalBar.addEventListener('click', async () => await navigateToForm('model_eval'));
  }
  if (hardnessCotBar) {
    hardnessCotBar.style.cursor = 'pointer';
    hardnessCotBar.title = 'Click to go to CoT Quality Evaluation';
    hardnessCotBar.addEventListener('click', async () => await navigateToForm('cot_eval'));
  }

  // Model Evaluation screen progress bars
  const modelEvalDataQualityBar = document.querySelector('#screenApp .progress:nth-child(1)');
  const modelEvalModelEvalBar = document.querySelector('#screenApp .progress:nth-child(2)');
  const modelEvalCotBar = document.querySelector('#screenApp .progress:nth-child(3)');
  
  if (modelEvalDataQualityBar) {
    modelEvalDataQualityBar.style.cursor = 'pointer';
    modelEvalDataQualityBar.title = 'Click to go to Data Quality Assessment';
    modelEvalDataQualityBar.addEventListener('click', async () => await navigateToForm('data_quality'));
  }
  if (modelEvalModelEvalBar) {
    modelEvalModelEvalBar.style.cursor = 'pointer';
    modelEvalModelEvalBar.title = 'Click to go to Model Evaluation';
    modelEvalModelEvalBar.addEventListener('click', async () => await navigateToForm('model_eval'));
  }
  if (modelEvalCotBar) {
    modelEvalCotBar.style.cursor = 'pointer';
    modelEvalCotBar.title = 'Click to go to CoT Quality Evaluation';
    modelEvalCotBar.addEventListener('click', async () => await navigateToForm('cot_eval'));
  }

  // CoT Evaluation screen progress bars
  const cotDataQualityBar = document.querySelector('#screenCot .progress:nth-child(1)');
  const cotModelEvalBar = document.querySelector('#screenCot .progress:nth-child(2)');
  const cotCotBar = document.querySelector('#screenCot .progress:nth-child(3)');
  
  if (cotDataQualityBar) {
    cotDataQualityBar.style.cursor = 'pointer';
    cotDataQualityBar.title = 'Click to go to Data Quality Assessment';
    cotDataQualityBar.addEventListener('click', async () => await navigateToForm('data_quality'));
  }
  if (cotModelEvalBar) {
    cotModelEvalBar.style.cursor = 'pointer';
    cotModelEvalBar.title = 'Click to go to Model Evaluation';
    cotModelEvalBar.addEventListener('click', async () => await navigateToForm('model_eval'));
  }
  if (cotCotBar) {
    cotCotBar.style.cursor = 'pointer';
    cotCotBar.title = 'Click to go to CoT Quality Evaluation';
    cotCotBar.addEventListener('click', async () => await navigateToForm('cot_eval'));
  }
}

function init() {
  // Clear all old localStorage cache on page load (Supabase is the only source of truth)
  clearAllLocalStorageCache();

  // Mode toggle logic
  const modeRadios = document.getElementsByName('evalMode');
  const modelGuide = $("modelGuide");
  const hardnessGuide = $("hardnessGuide");

  modeRadios.forEach(radio => {
    radio.addEventListener('change', (e) => {
      if (e.target.value === 'model') {
        modelGuide.style.display = 'block';
        hardnessGuide.style.display = 'none';
        modelGuide.open = true;
        hardnessGuide.open = false;
      } else {
        modelGuide.style.display = 'none';
        hardnessGuide.style.display = 'block';
        modelGuide.open = false;
        hardnessGuide.open = true;
      }
    });
  });

  $("btnStart").addEventListener("click", onStart);
  $("btnPrev").addEventListener("click", onPrev);
  $("btnSaveNext").addEventListener("click", onSaveNext);

  // Hardness mode buttons (check if they exist first)
  const btnHardnessPrev = $("btnHardnessPrev");
  const btnHardnessSaveNext = $("btnHardnessSaveNext");
  if (btnHardnessPrev) btnHardnessPrev.addEventListener("click", onHardnessPrev);
  if (btnHardnessSaveNext) btnHardnessSaveNext.addEventListener("click", onHardnessSaveNext);

  // CoT mode buttons (check if they exist first)
  const btnCotPrev = $("btnCotPrev");
  const btnCotSaveNext = $("btnCotSaveNext");
  if (btnCotPrev) btnCotPrev.addEventListener("click", onCotPrev);
  if (btnCotSaveNext) btnCotSaveNext.addEventListener("click", onCotSaveNext);

  // Make progress bars clickable for navigation
  setupProgressBarNavigation();

  // Keyboard shortcuts
  document.addEventListener('keydown', (e) => {
    if (!STATE) return;
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

    // Check which screen is visible
    const isModelScreen = !$("screenApp").classList.contains("hidden");
    const isHardnessScreen = !$("screenHardness").classList.contains("hidden");
    const isCotScreen = !$("screenCot")?.classList.contains("hidden");

    if (isModelScreen) {
      if (e.key === 'ArrowLeft' || e.key === 'p') onPrev();
      if (e.key === 'ArrowRight' || e.key === 'n') onSaveNext();
    } else if (isHardnessScreen) {
      if (e.key === 'ArrowLeft' || e.key === 'p') onHardnessPrev();
      if (e.key === 'ArrowRight' || e.key === 'n') onHardnessSaveNext();
    } else if (isCotScreen) {
      if (e.key === 'ArrowLeft' || e.key === 'p') onCotPrev();
      if (e.key === 'ArrowRight' || e.key === 'n') onCotSaveNext();
    }
  });

  // Warn before leaving with unsaved changes
  window.addEventListener('beforeunload', (e) => {
    if (STATE) {
      saveState(STATE.user.id, STATE);
    }
  });
}
document.addEventListener("DOMContentLoaded", init);

async function onStart() {
  try {
    console.log("onStart called");
    const userId = $("userId").value.trim();
    const sampleSize = 10; // Fixed: 10 cases per dataset

    if (!userId) {
      $("loginStatus").textContent = "⚠️ User ID is required.";
      $("userId").focus();
      return;
    }

    $("loginStatus").innerHTML = `
      <div style="display:flex;align-items:center;gap:10px;">
        <div class="spinner"></div>
        <span>Loading...</span>
      </div>
    `;

    // Add spinner styles
    if (!document.querySelector('#spinner-styles')) {
      const style = document.createElement('style');
      style.id = 'spinner-styles';
      style.textContent = `
        .spinner {
          width: 18px; height: 18px;
          border: 2px solid rgba(90,123,181,0.2);
          border-top-color: #5a7bb5;
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
      `;
      document.head.appendChild(style);
    }

    console.log("Fetching state from Supabase...");
    // Load ONLY from Supabase (no localStorage fallback)
    let state = await supabaseGet(userId);
    console.log("Supabase state:", state);

    if (!state) {
      console.log("Creating new state");
      state = {
        version: 8,
        user: { id: userId, created_at: nowISO() },
        config: { sample_size_per_dataset: sampleSize, cot_cases_per_dataset: 10 },
        data_quality: { cases: [], cursor: 0, answers: {}, completed: false },
        model_evaluation: {
          datasets: {
            rexgradient: { cases: [], cursor: 0, answers: {}, hardness_distribution: null },
            chexpert: { cases: [], cursor: 0, answers: {}, hardness_distribution: null },
            mimic: { cases: [], cursor: 0, answers: {}, hardness_distribution: null }
          },
          completed: false
        },
        cot_evaluation: { cases: [], cursor: 0, answers: {}, completed: false },
        audit: { last_saved_at: null, save_count: 0 },
      };
    } else {
      // Initialize missing fields for backwards compatibility
      if (!state.data_quality) {
        state.data_quality = { cases: [], cursor: 0, answers: {}, completed: false };
      }
      if (!state.model_evaluation) {
        state.model_evaluation = {
          datasets: {
            rexgradient: { cases: [], cursor: 0, answers: {}, hardness_distribution: null },
            chexpert: { cases: [], cursor: 0, answers: {}, hardness_distribution: null },
            mimic: { cases: [], cursor: 0, answers: {}, hardness_distribution: null }
          },
          completed: false
        };
      } else if (!state.model_evaluation.datasets) {
        state.model_evaluation.datasets = {
          rexgradient: { cases: [], cursor: 0, answers: {}, hardness_distribution: null },
          chexpert: { cases: [], cursor: 0, answers: {}, hardness_distribution: null },
          mimic: { cases: [], cursor: 0, answers: {}, hardness_distribution: null }
        };
      }
      if (!state.cot_evaluation) {
        state.cot_evaluation = { cases: [], cursor: 0, answers: {}, completed: false };
      }
      // Ensure config exists
      if (!state.config) {
        state.config = { sample_size_per_dataset: sampleSize, cot_cases_per_dataset: 10 };
      }
      if (!state.config.sample_size_per_dataset) {
        state.config.sample_size_per_dataset = sampleSize;
      }
      if (!state.config.cot_cases_per_dataset) {
        state.config.cot_cases_per_dataset = 10;
      }
    }

    // Determine which phase to start
    // Phase 1: Data Quality (must complete first)
    const dataQualityDone = Object.keys(state.data_quality.answers).length;
    const dataQualityTotal = state.data_quality.cases.length || 100; // 25 per level * 4

    if (dataQualityDone < dataQualityTotal) {
      console.log(`Starting Data Quality phase (${dataQualityDone}/${dataQualityTotal} completed)`);
      await startDataQualityMode(userId, state);
    } else {
      // Check Model Evaluation progress
      const modelEvalTotal = DATASETS.length * state.config.sample_size_per_dataset;
      let modelEvalDone = 0;
      for (const ds of DATASETS) {
        modelEvalDone += Object.keys(state.model_evaluation.datasets[ds.key]?.answers || {}).length;
      }
      
      if (modelEvalDone < modelEvalTotal) {
        // Phase 2: Model Evaluation
        console.log(`Data Quality complete, continuing Model Evaluation (${modelEvalDone}/${modelEvalTotal})`);
        await startModelEvalMode(userId, state);
      } else {
        // Phase 3: CoT Evaluation
        console.log("Model Evaluation complete, starting CoT Evaluation");
        await startCotEvalMode(userId, state);
      }
    }
  } catch (error) {
    console.error("Error in onStart:", error);
    $("loginStatus").innerHTML = `
      <div style="color:#ff4d6d;">❌ Error: ${escapeHtml(error.message)}</div>
      <div style="margin-top:8px;font-size:12px;color:var(--muted);">${escapeHtml(error.stack || '')}</div>
    `;
  }
}

async function startDataQualityMode(userId, state) {
  try {
    console.log("Starting data quality mode, current cases:", state.data_quality.cases.length);
    // Load data quality cases if not already loaded
    if (state.data_quality.cases.length === 0) {
      console.log("Loading CSV from:", DATA_QUALITY_DATASET.file);
      const rows = await fetchCsv(DATA_QUALITY_DATASET.file);
      console.log("Total CSV rows loaded:", rows.length);
      if (rows.length > 0) {
        console.log("First raw CSV row columns:", Object.keys(rows[0]));
        console.log("First raw row sample:", {
          id: rows[0].StudyInstanceUid?.substring(0, 20),
          impression: rows[0].Impression?.substring(0, 50),
          cot: rows[0].raw_output?.substring(0, 50)
        });
      }

      const normalized = rows.map(normalizeRow);
      console.log("Normalized rows:", normalized.length);
      if (normalized.length > 0) {
        console.log("First normalized row:", {
          id: normalized[0].id?.substring(0, 20),
          gt: normalized[0].ground_truth?.substring(0, 50),
          cot: normalized[0].cot?.substring(0, 50)
        });
      }

      // Filter for train split with hardness data AND non-empty CoT
      const trainRows = normalized.filter(r => r.split === 'train' && r.hardness && r.cot && r.cot.trim() !== '');
      console.log("Train rows with hardness and CoT:", trainRows.length);

      if (trainRows.length === 0) {
        // Try without split filter - maybe split column doesn't exist or has different values
        const rowsWithHardness = normalized.filter(r => r.hardness && r.cot && r.cot.trim() !== '');
        console.log("Rows with hardness and CoT (no split filter):", rowsWithHardness.length);

        if (rowsWithHardness.length > 0) {
          console.log("Sample row:", rowsWithHardness[0]);
          // Use all rows with hardness
          trainRows.length = 0;
          trainRows.push(...rowsWithHardness);
        } else {
          throw new Error("No rows with hardness column and CoT data found in dataset");
        }
      }

      // Group by hardness level
      const byHardness = { '1': [], '2': [], '3': [], '4': [] };
      trainRows.forEach(r => {
        const h = String(r.hardness).trim();
        if (byHardness[h]) {
          byHardness[h].push(r);
        }
      });

      console.log("Grouped by hardness:", {
        '1': byHardness['1'].length,
        '2': byHardness['2'].length,
        '3': byHardness['3'].length,
        '4': byHardness['4'].length
      });

      // Deterministically select first 25 from each hardness level (sorted by ID for consistency)
      const selectedCases = [];
      for (const level of ['1', '2', '3', '4']) {
        const pool = byHardness[level].sort((a, b) => String(a.id).localeCompare(String(b.id)));
        const selected = pool.slice(0, 25); // Take first 25 (same for everyone)
        console.log(`Selected ${selected.length} cases for hardness level ${level}`);
        selectedCases.push(...selected);
      }

      console.log("Total selected cases before shuffle:", selectedCases.length);

      // Shuffle the cases using user-specific seed (so same user sees same order)
      const shuffleSeed = stableHash(`${userId}::data_quality_order`);
      const shuffleRnd = mulberry32(shuffleSeed);
      for (let i = selectedCases.length - 1; i > 0; i--) {
        const j = Math.floor(shuffleRnd() * (i + 1));
        [selectedCases[i], selectedCases[j]] = [selectedCases[j], selectedCases[i]];
      }

      console.log("Cases shuffled for user:", userId);

      // Map to data quality case structure
      state.data_quality.cases = selectedCases.map(r => {
        console.log("Mapping case:", r.id, "GT:", r.ground_truth?.substring(0, 50), "CoT:", r.cot?.substring(0, 50));
        return {
          id: String(r.id),
          findings: r.findings,
          indication: r.indication,
          ground_truth: r.ground_truth,
          hardness: r.hardness, // System's assigned hardness
          cot: r.cot, // Chain-of-thought
        };
      });

      state.data_quality.cursor = 0;
      state.data_quality.answers = {};
    }

    saveState(userId, state);
    STATE = state;

    setHidden($("screenLogin"), true);
    setHidden($("screenApp"), true);
    setHidden($("screenHardness"), false);
    setHidden($("userBadge"), false);

    // Update user badge
    const badgeText = $("userBadgeText");
    if (badgeText) {
      badgeText.textContent = STATE.user.id;
    } else {
      $("userBadge").textContent = `user: ${STATE.user.id}`;
    }

    renderHardnessCase();
    $("loginStatus").textContent = "";

    // Start auto-save timer
    if (autoSaveTimer) clearInterval(autoSaveTimer);
    autoSaveTimer = setInterval(() => {
      if (STATE) {
        saveState(STATE.user.id, STATE);
        console.log('Auto-saved at', nowISO());
      }
    }, AUTO_SAVE_INTERVAL);

    showToast(`Welcome to Hardness Evaluation, ${STATE.user.id}!`, 'success');
  } catch (e) {
    console.error("Hardness mode error:", e);
    $("loginStatus").innerHTML =
      `<div style="color:#ff4d6d;">❌ Hardness data loading error</div>
      <div style="margin-top:8px;color:var(--muted);">Error: ${escapeHtml(e.message)}</div>`;
  }
}

async function startModelEvalMode(userId, state) {
  try {
    // Debug: Log initial state of all datasets before processing
    console.log('\n=== BEFORE Processing: Initial Dataset State ===');
    for (const ds of DATASETS) {
      const dsData = state.model_evaluation.datasets[ds.key];
      console.log(`  ${ds.key}: cases=${dsData?.cases?.length || 0}, answers=${Object.keys(dsData?.answers || {}).length}, exists=${!!dsData}`);
    }
    
    for (const ds of DATASETS) {
      console.log(`\n=== Processing dataset: ${ds.key} ===`);
      
      // Check if we need to reload: either no cases exist, or cases are missing new models, or cases have empty data
      let needsReload = false;
      
      const casesLength = state.model_evaluation.datasets[ds.key]?.cases?.length || 0;
      console.log(`  Dataset ${ds.key} has ${casesLength} existing cases`);
      
      if (casesLength === 0) {
        console.log(`  No cases exist for ${ds.key}, will load from CSV`);
        needsReload = true;
      } else {
        console.log(`  Found ${casesLength} existing cases`);
        
        // Check if existing cases have all current models
        const firstCase = state.model_evaluation.datasets[ds.key].cases[0];
        const existingModelKeys = Object.keys(firstCase.models || {});
        const currentModelKeys = MODEL_COLUMNS.map(m => m.key);
        const missingModels = currentModelKeys.filter(k => !existingModelKeys.includes(k));
        
        console.log(`  Existing model keys: ${existingModelKeys.join(', ')}`);
        console.log(`  Current model keys:  ${currentModelKeys.join(', ')}`);
        
        if (missingModels.length > 0) {
          console.log(`  Dataset ${ds.key} is missing models: ${missingModels.join(', ')}. Reloading from CSV...`);
          needsReload = true;
        }
        
        // Check if any existing cases have empty model data and replace them
        if (!needsReload) {
          const casesWithEmpty = [];
          const validCases = [];
          
          state.model_evaluation.datasets[ds.key].cases.forEach((c, idx) => {
            let hasEmpty = false;
            for (const m of MODEL_COLUMNS) {
              const value = c.models?.[m.key];
              if (!value || value.trim() === '') {
                hasEmpty = true;
                break;
              }
            }
            if (hasEmpty) {
              casesWithEmpty.push({ case: c, index: idx });
            } else {
              validCases.push(c);
            }
          });
          
          if (casesWithEmpty.length > 0) {
            console.log(`Dataset ${ds.key} has ${casesWithEmpty.length} cases with empty data. Replacing them...`);
            
            // Load CSV to get replacement cases
            const rows = await fetchCsv(ds.file);
            const normalized = rows.map(normalizeRow);
            
            // Filter for complete data
            const completeRows = normalized.filter(r => {
              for (const m of MODEL_COLUMNS) {
                const value = r[m.key];
                if (!value || value.trim() === '') return false;
              }
              return true;
            });
            
            // Get IDs of cases we're keeping
            const keepIds = new Set(validCases.map(c => c.id));
            
            // Find replacement cases (not already in our list)
            const availableReplacements = completeRows.filter(r => !keepIds.has(String(r.id)));
            
            // Use user-specific deterministic selection for replacements
            const replacementSeed = stableHash(`${userId}::${ds.key}::replacement`);
            const replacementRnd = mulberry32(replacementSeed);
            
            // Replace each problematic case
            casesWithEmpty.forEach(({ case: oldCase, index }) => {
              if (availableReplacements.length > 0) {
                const replaceIdx = Math.floor(replacementRnd() * availableReplacements.length);
                const replacement = availableReplacements[replaceIdx];
                availableReplacements.splice(replaceIdx, 1); // Remove so we don't reuse
                
                // Create new case object
                const newCase = {
                  id: String(replacement.id),
                  findings: replacement.findings,
                  indication: replacement.indication,
                  ground_truth: replacement.ground_truth,
                  hardness: replacement.hardness || null,
                  models: Object.fromEntries(MODEL_COLUMNS.map(m => [m.key, replacement[m.key]])),
                };
                
                // Replace in the cases array at the same position
                state.model_evaluation.datasets[ds.key].cases[index] = newCase;
                
                // Remove answer for the old case if it exists
                const answers = state.model_evaluation.datasets[ds.key].answers || {};
                if (answers[oldCase.id]) {
                  delete answers[oldCase.id];
                  console.log(`Removed answer for replaced case ${oldCase.id}`);
                }
                
                console.log(`Replaced case ${oldCase.id} with ${newCase.id} at position ${index}`);
              }
            });
            
            // Save the updated state
            saveState(userId, state);
            console.log(`Dataset ${ds.key} migration complete: replaced ${casesWithEmpty.length} cases`);
          }
        }
      }
      
      if (!needsReload) {
        console.log(`  Dataset ${ds.key}: No reload needed, skipping CSV load`);
        continue;
      }
      
      console.log(`  Dataset ${ds.key}: Loading from CSV...`);

      const rows = await fetchCsv(ds.file);
      const normalized = rows.map(normalizeRow);

      console.log(`Dataset ${ds.key}: Loaded ${rows.length} rows from CSV`);
      console.log(`  Looking for columns:`, MODEL_COLUMNS.map(m => `${m.key}(${m.col})`).join(', '));

      // Check first row to see which model columns are present
      if (normalized.length > 0) {
        const firstRow = normalized[0];
        const presentModels = MODEL_COLUMNS.filter(m => firstRow[m.key] !== undefined && firstRow[m.key] !== null);
        const missingModels = MODEL_COLUMNS.filter(m => firstRow[m.key] === undefined || firstRow[m.key] === null);
        console.log(`  Present model columns (${presentModels.length}/${MODEL_COLUMNS.length}):`, presentModels.map(m => m.key).join(', '));
        if (missingModels.length > 0) {
          console.warn(`  Missing model columns:`, missingModels.map(m => `${m.key}(${m.col})`).join(', '));
        }
      }

      // Filter out cases where any model has empty/missing data
      const completeRows = normalized.filter(r => {
        // Check that all models have non-empty predictions
        for (const m of MODEL_COLUMNS) {
          const value = r[m.key];
          if (!value || value.trim() === '') {
            return false; // Skip this case - has empty data
          }
        }
        return true; // All models have data
      });

      console.log(`Dataset ${ds.key}: ${normalized.length} total rows, ${completeRows.length} with complete model data`);

      if (completeRows.length === 0) {
        console.error(`ERROR: Dataset ${ds.key} has NO rows with complete model data! All cases were filtered out.`);
        console.error(`  This means at least one model has empty data in ALL rows.`);
        console.error(`  Check that all model columns exist and have data in the CSV file.`);
      }

      let fixedSample;
      
      // Special hardness-based sampling for RexGradient
      if (ds.key === 'rexgradient') {
        console.log('RexGradient: Loading hardness data from deepseek_cot_out.csv...');
        
        // Load hardness labels from deepseek_cot_out.csv (training data)
        const hardnessRows = await fetchCsv(DATA_QUALITY_DATASET.file);
        const hardnessNormalized = hardnessRows.map(normalizeRow);
        
        // Filter for train split with hardness data
        const trainRows = hardnessNormalized.filter(r => 
          (r.split === 'train' || !r.split) && r.hardness && r.hardness.trim() !== ''
        );
        console.log(`Found ${trainRows.length} training cases with hardness labels`);
        
        // Group by hardness level
        const byHardness = { '1': [], '2': [], '3': [], '4': [] };
        trainRows.forEach(r => {
          const h = String(r.hardness).trim();
          if (byHardness[h]) {
            byHardness[h].push(r);
          }
        });
        
        console.log(`Hardness distribution: ${Object.entries(byHardness).map(([k, v]) => `L${k}:${v.length}`).join(', ')}`);
        
        // Sample case IDs with hardness distribution: 2-2-3-3
        const hardnessDistribution = { '1': 2, '2': 2, '3': 3, '4': 3 };
        const selectedIds = [];
        
        for (const [level, count] of Object.entries(hardnessDistribution)) {
          const pool = byHardness[level].sort((a, b) => String(a.id).localeCompare(String(b.id)));
          const selected = pool.slice(0, count);
          console.log(`Selected ${selected.length} case IDs from hardness level ${level}`);
          selected.forEach(r => selectedIds.push(r.id));
        }
        
        console.log(`Total selected case IDs: ${selectedIds.length}`);
        
        // Now get model outputs for these case IDs from rexgradient_all_predictions_final.csv
        // Filter completeRows (already loaded from final CSV) to match the selected IDs
        const selectedIdSet = new Set(selectedIds);
        fixedSample = completeRows.filter(r => selectedIdSet.has(r.id));
        
        console.log(`Matched ${fixedSample.length} cases with complete model data from final CSV`);
        
        // If we didn't get all 10, take alphabetical as fallback
        if (fixedSample.length < state.config.sample_size_per_dataset) {
          console.warn(`⚠️ Only found ${fixedSample.length}/${state.config.sample_size_per_dataset} cases with complete data. Adding alphabetical cases...`);
          const existingIds = new Set(fixedSample.map(r => r.id));
          const additionalCases = completeRows
            .filter(r => !existingIds.has(r.id))
            .sort((a, b) => String(a.id).localeCompare(String(b.id)))
            .slice(0, state.config.sample_size_per_dataset - fixedSample.length);
          fixedSample.push(...additionalCases);
          console.log(`Added ${additionalCases.length} additional cases. Total: ${fixedSample.length}`);
        }
      } else {
        // For other datasets: Take first 10 cases with complete data (sorted by ID)
        const sortedRows = completeRows.sort((a, b) => String(a.id).localeCompare(String(b.id)));
        fixedSample = sortedRows.slice(0, state.config.sample_size_per_dataset);
        console.log(`  Selected ${fixedSample.length} cases from ${ds.key}`);
      }

      // Shuffle case order using user-specific seed (same user sees same order)
      const orderSeed = stableHash(`${userId}::${ds.key}::order`);
      const orderRnd = mulberry32(orderSeed);
      const shuffledCases = [...fixedSample];
      for (let i = shuffledCases.length - 1; i > 0; i--) {
        const j = Math.floor(orderRnd() * (i + 1));
        [shuffledCases[i], shuffledCases[j]] = [shuffledCases[j], shuffledCases[i]];
      }

      // When models change, reset cursor and answers since they don't match new model keys
      const existingCursor = 0; // Reset to start
      const existingAnswers = {}; // Clear old answers since model keys changed

      // Calculate hardness distribution for RexGradient
      let hardnessStats = null;
      if (ds.key === 'rexgradient') {
        // Re-fetch hardness values for the selected cases from deepseek_cot_out.csv
        const hardnessRows = await fetchCsv(DATA_QUALITY_DATASET.file);
        const hardnessMap = new Map();
        hardnessRows.forEach(r => {
          const normalized = normalizeRow(r);
          if (normalized.id && normalized.hardness) {
            hardnessMap.set(normalized.id, normalized.hardness.trim());
          }
        });
        
        const distribution = { '1': 0, '2': 0, '3': 0, '4': 0 };
        shuffledCases.forEach(c => {
          const h = hardnessMap.get(c.id);
          if (h && distribution[h] !== undefined) {
            distribution[h]++;
          }
        });
        
        hardnessStats = {
          target: { '1': 2, '2': 2, '3': 3, '4': 3 },
          actual: distribution,
          total: shuffledCases.length
        };
        console.log(`RexGradient hardness stats:`, hardnessStats);
        
        // Store hardness map for later use
        state.model_evaluation.datasets[ds.key] = {
          cases: shuffledCases.map(r => ({
            id: String(r.id),
            findings: r.findings,
            indication: r.indication,
            ground_truth: r.ground_truth,
            hardness: hardnessMap.get(r.id) || null, // Get hardness from deepseek_cot_out.csv
            models: Object.fromEntries(MODEL_COLUMNS.map(m => [m.key, r[m.key]])),
          })),
          cursor: existingCursor,
          answers: existingAnswers,
          hardness_distribution: hardnessStats,
        };
      } else {
        // For other datasets: no hardness
        state.model_evaluation.datasets[ds.key] = {
          cases: shuffledCases.map(r => ({
            id: String(r.id),
            findings: r.findings,
            indication: r.indication,
            ground_truth: r.ground_truth,
            hardness: null,
            models: Object.fromEntries(MODEL_COLUMNS.map(m => [m.key, r[m.key]])),
          })),
          cursor: existingCursor,
          answers: existingAnswers,
          hardness_distribution: null,
        };
      }
      
      console.log(`Dataset ${ds.key} loaded/reloaded with ${MODEL_COLUMNS.length} models, ${shuffledCases.length} cases, ${Object.keys(existingAnswers).length} existing answers`);
    }

    // Debug: Log final state of all datasets
    console.log('=== Model Evaluation Setup Complete ===');
    let totalCasesLoaded = 0;
    for (const ds of DATASETS) {
      const d = state.model_evaluation.datasets[ds.key];
      const casesCount = d?.cases?.length || 0;
      totalCasesLoaded += casesCount;
      console.log(`  ${ds.key}: ${casesCount} cases, ${Object.keys(d?.answers || {}).length} answers`);
    }
    
    if (totalCasesLoaded === 0) {
      const errorMsg = 'ERROR: No cases were loaded for any dataset! This means all CSVs failed to load or all data was filtered out.';
      console.error(errorMsg);
      console.error('Current MODEL_COLUMNS:', MODEL_COLUMNS);
      throw new Error(errorMsg);
    }
    
    saveState(userId, state);
    STATE = state;
    ACTIVE_DATASET = DATASETS[0].key;
    ACTIVE_INDEX = state.model_evaluation.datasets[ACTIVE_DATASET].cursor || 0;
    
    console.log(`Starting with ACTIVE_DATASET=${ACTIVE_DATASET}, ACTIVE_INDEX=${ACTIVE_INDEX}`);

    setHidden($("screenLogin"), true);
    setHidden($("screenApp"), false);
    setHidden($("userBadge"), false);

    // Update user badge with new structure
    const badgeText = $("userBadgeText");
    if (badgeText) {
      badgeText.textContent = STATE.user.id;
    } else {
      $("userBadge").textContent = `user: ${STATE.user.id}`;
    }

    renderTabs();
    renderCase();
    $("loginStatus").textContent = "";

    // Start auto-save timer
    if (autoSaveTimer) clearInterval(autoSaveTimer);
    autoSaveTimer = setInterval(() => {
      if (STATE) {
        saveState(STATE.user.id, STATE);
        console.log('Auto-saved at', nowISO());
      }
    }, AUTO_SAVE_INTERVAL);

    showToast(`Welcome, ${STATE.user.id}!`, 'success');
  } catch (e) {
    console.error(e);
    const isFileProtocol = location.protocol === "file:";
    $("loginStatus").innerHTML =
      `<div style="color:#ff4d6d;">❌ Dataset loading error</div>
      <div style="margin-top:8px;color:var(--muted);">Error: ${escapeHtml(e.message)}</div>
      <div style="margin-top:12px;padding:12px;background:rgba(0,0,0,0.2);border-radius:8px;font-size:12px;">
        <strong>Checklist:</strong><br>
        ${isFileProtocol ? `
        • <strong>file:// protocol</strong>: Chrome blocks CORS<br>
        • Try <strong>Firefox</strong> with file://<br>
        • Or run: <code>python -m http.server 8000</code> then open <code>localhost:8000</code><br>
        ` : `
        • Did you open <code>https://emrecobann.github.io</code>?<br>
        • Is index.html in repo root?<br>
        • Is data/ folder in repo root?<br>
        • First load may take 1-2 min for GitHub Pages deployment<br>
        `}
      </div>`;
  }
}

async function startCotEvalMode(userId, state) {
  try {
    // Check if we need to load or reload CoT cases
    let needsReload = false;
    
    if (state.cot_evaluation.cases.length === 0) {
      needsReload = true;
    } else {
      // Check if any existing cases have empty CoT data
      const casesWithEmptyCot = state.cot_evaluation.cases.filter(c => !c.cot || c.cot.trim() === '');
      if (casesWithEmptyCot.length > 0) {
        console.log(`Found ${casesWithEmptyCot.length} cases with empty CoT. Reloading...`);
        needsReload = true;
        // Reset cursor and answers if reloading
        state.cot_evaluation.cursor = 0;
        state.cot_evaluation.answers = {};
      }
    }
    
    // Load CoT cases if needed - use SAME case IDs as Model Evaluation
    if (needsReload) {
      console.log("Loading CoT evaluation cases - using same IDs as Model Evaluation...");
      
      // Collect case IDs from model evaluation (in the order they appear for this user)
      const modelEvalCaseIds = new Map(); // Map: caseId -> dataset
      for (const ds of DATASETS) {
        const dsData = state.model_evaluation.datasets[ds.key];
        if (dsData && dsData.cases) {
          dsData.cases.forEach(c => {
            modelEvalCaseIds.set(c.id, ds.key);
          });
        }
      }
      
      console.log(`Found ${modelEvalCaseIds.size} case IDs from Model Evaluation`);
      
      if (modelEvalCaseIds.size === 0) {
        console.warn("No model evaluation cases found! User must complete Model Evaluation first.");
        throw new Error("Please complete Model Evaluation first before starting CoT Evaluation");
      }
      
      const allCases = [];
      
      // Load CoT data for the SAME cases from merged CSVs
      for (const ds of COT_DATASETS) {
        const rows = await fetchCsv(ds.file);
        console.log(`Loaded ${rows.length} rows from ${ds.key} merged CSV`);
        
        // Normalize rows
        const normalized = rows.map(row => ({
          id: String(row.id || row.StudyInstanceUid || ""),
          findings: row.findings || row.Findings || "",
          indication: row.indication || row.Indication || "",
          ground_truth: row.ground_truth || row.Impression || row.impression || "",
          cot: row[ds.cotCol] || "",
          dataset: ds.key
        }));
        
        // Filter to ONLY the case IDs that were used in Model Evaluation for this dataset
        const modelEvalCasesForThisDataset = Array.from(modelEvalCaseIds.entries())
          .filter(([id, dataset]) => dataset === ds.key)
          .map(([id]) => id);
        
        console.log(`Model Eval has ${modelEvalCasesForThisDataset.length} cases from ${ds.key}`);
        
        const matchedCases = normalized.filter(r => {
          const hasId = modelEvalCasesForThisDataset.includes(r.id);
          const hasCot = r.cot && r.cot.trim() !== '';
          return hasId && hasCot;
        });
        
        console.log(`Matched ${matchedCases.length} cases from ${ds.key} with CoT data`);
        allCases.push(...matchedCases);
      }
      
      // Maintain the same shuffled order as model evaluation for this user
      // Sort by the order they appear in model_evaluation
      const orderMap = new Map();
      let order = 0;
      for (const ds of DATASETS) {
        const dsData = state.model_evaluation.datasets[ds.key];
        if (dsData && dsData.cases) {
          dsData.cases.forEach(c => {
            orderMap.set(c.id, order++);
          });
        }
      }
      
      allCases.sort((a, b) => {
        const orderA = orderMap.get(a.id) ?? 999999;
        const orderB = orderMap.get(b.id) ?? 999999;
        return orderA - orderB;
      });
      
      state.cot_evaluation.cases = allCases;
      state.cot_evaluation.cursor = 0;
      console.log(`Total CoT evaluation cases: ${allCases.length} (same as Model Evaluation)`);
    }
    
    saveState(userId, state);
    STATE = state;
    
    setHidden($("screenLogin"), true);
    setHidden($("screenApp"), true);
    setHidden($("screenHardness"), true);
    setHidden($("screenCot"), false);
    setHidden($("userBadge"), false);
    
    // Update user badge
    const badgeText = $("userBadgeText");
    if (badgeText) {
      badgeText.textContent = STATE.user.id;
    } else {
      $("userBadge").textContent = `user: ${STATE.user.id}`;
    }
    
    renderCotCase();
    $("loginStatus").textContent = "";
    
    // Start auto-save timer
    if (autoSaveTimer) clearInterval(autoSaveTimer);
    autoSaveTimer = setInterval(() => {
      if (STATE) {
        saveState(STATE.user.id, STATE);
        console.log('Auto-saved at', nowISO());
      }
    }, AUTO_SAVE_INTERVAL);
    
    showToast(`Starting CoT Evaluation - ${STATE.cot_evaluation.cases.length} cases`, 'success');
  } catch (e) {
    console.error("CoT evaluation error:", e);
    $("loginStatus").innerHTML =
      `<div style="color:#ff4d6d;">❌ CoT evaluation loading error</div>
      <div style="margin-top:8px;color:var(--muted);">Error: ${escapeHtml(e.message)}</div>`;
  }
}

function renderTabs() {
  // Dataset tabs removed - no longer needed
}

// Get all cases from all datasets in order
function getAllCases() {
  const allCases = [];
  for (const ds of DATASETS) {
    const d = STATE.model_evaluation.datasets[ds.key];
    if (d && d.cases) {
      d.cases.forEach(c => {
        allCases.push({
          ...c,
          _dataset: ds.key, // Store original dataset
          _globalId: `${ds.key}::${c.id}` // Unique global ID
        });
      });
    }
  }
  return allCases;
}

// Get global cursor position across all datasets
function getGlobalCursor() {
  let cursor = 0;
  for (const ds of DATASETS) {
    if (ds.key === ACTIVE_DATASET) {
      cursor += ACTIVE_INDEX;
      break;
    }
    cursor += STATE.model_evaluation.datasets[ds.key].cases.length;
  }
  return cursor;
}

// Set cursor from global position
function setGlobalCursor(globalIdx) {
  let remaining = globalIdx;
  for (const ds of DATASETS) {
    const len = STATE.model_evaluation.datasets[ds.key].cases.length;
    if (remaining < len) {
      ACTIVE_DATASET = ds.key;
      ACTIVE_INDEX = remaining;
      return;
    }
    remaining -= len;
  }
  // If we reach here, set to last case
  const lastDs = DATASETS[DATASETS.length - 1];
  ACTIVE_DATASET = lastDs.key;
  ACTIVE_INDEX = STATE.model_evaluation.datasets[ACTIVE_DATASET].cases.length - 1;
}

function computeOverallProgress() {
  const totals = DATASETS.map(ds => {
    const d = STATE.model_evaluation.datasets[ds.key];
    const total = d.cases.length;
    const done = Object.keys(d.answers || {}).length;
    return { total, done };
  });
  const totalAll = totals.reduce((a, x) => a + x.total, 0);
  const doneAll = totals.reduce((a, x) => a + x.done, 0);
  return { totalAll, doneAll };
}

// Centralized function to update all progress bars across all screens
function updateAllProgressBars() {
  // Calculate Data Quality progress
  const dataQualityDone = Object.keys(STATE.data_quality.answers || {}).length;
  const dataQualityTotal = STATE.data_quality.cases.length || 100;
  const dataQualityPct = dataQualityTotal ? Math.round((dataQualityDone / dataQualityTotal) * 100) : 0;

  // Calculate Model Evaluation progress
  const { totalAll: modelEvalTotal, doneAll: modelEvalDone } = computeOverallProgress();
  const expectedModelTotal = DATASETS.length * (STATE.config?.sample_size_per_dataset || 10);
  const displayModelTotal = modelEvalTotal > 0 ? modelEvalTotal : expectedModelTotal;
  const modelEvalPct = displayModelTotal ? Math.round((modelEvalDone / displayModelTotal) * 100) : 0;

  // Calculate CoT Evaluation progress
  const cotDone = Object.keys(STATE.cot_evaluation?.answers || {}).length;
  const cotTotal = STATE.cot_evaluation?.cases?.length || 30;
  const cotPct = cotTotal ? Math.round((cotDone / cotTotal) * 100) : 0;

  // Update Data Quality screen progress bars
  const updateIfExists = (id, value) => {
    const el = $(id);
    if (el) el.textContent = value;
  };
  const updateWidthIfExists = (id, width) => {
    const el = $(id);
    if (el) el.style.width = width;
  };

  // Data Quality screen
  updateIfExists("hardnessProgressText", `${dataQualityDone}/${dataQualityTotal}`);
  updateWidthIfExists("hardnessProgressFill", `${dataQualityPct}%`);
  updateIfExists("hardnessModelEvalProgressText", `${modelEvalDone}/${displayModelTotal}`);
  updateWidthIfExists("hardnessModelEvalProgressFill", `${modelEvalPct}%`);
  updateIfExists("hardnessCotProgressText", `${cotDone}/${cotTotal}`);
  updateWidthIfExists("hardnessCotProgressFill", `${cotPct}%`);

  // Model Evaluation screen
  updateIfExists("dataQualityProgressText", `${dataQualityDone}/${dataQualityTotal}`);
  updateWidthIfExists("dataQualityProgressFill", `${dataQualityPct}%`);
  updateIfExists("progressText", `${modelEvalDone}/${displayModelTotal}`);
  updateWidthIfExists("progressFill", `${modelEvalPct}%`);
  updateIfExists("modelEvalCotProgressText", `${cotDone}/${cotTotal}`);
  updateWidthIfExists("modelEvalCotProgressFill", `${cotPct}%`);

  // CoT Evaluation screen
  updateIfExists("cotDataQualityProgressText", `${dataQualityDone}/${dataQualityTotal}`);
  updateWidthIfExists("cotDataQualityProgressFill", `${dataQualityPct}%`);
  updateIfExists("cotModelEvalProgressText", `${modelEvalDone}/${displayModelTotal}`);
  updateWidthIfExists("cotModelEvalProgressFill", `${modelEvalPct}%`);
  updateIfExists("cotProgressText", `${cotDone}/${cotTotal}`);
  updateWidthIfExists("cotProgressFill", `${cotPct}%`);
}

// Navigate to a specific evaluation form
async function navigateToForm(formName) {
  if (!STATE || !STATE.user) {
    showToast('Please log in first', 'error');
    return;
  }

  // Save current state before switching
  await supabaseSave(STATE.user.id, STATE);

  // Hide all screens
  setHidden($("screenHardness"), true);
  setHidden($("screenApp"), true);
  setHidden($("screenCot"), true);

  if (formName === 'data_quality') {
    // Go to Data Quality form
    await startDataQualityMode(STATE.user.id, STATE);
    showToast('Switched to Data Quality Assessment', 'success');
  } else if (formName === 'model_eval') {
    // Go to Model Evaluation form
    await startModelEvalMode(STATE.user.id, STATE);
    showToast('Switched to Model Evaluation', 'success');
  } else if (formName === 'cot_eval') {
    // Check if Model Evaluation has been started (has cases loaded)
    let hasModelEvalCases = false;
    for (const ds of DATASETS) {
      if (STATE.model_evaluation.datasets[ds.key]?.cases?.length > 0) {
        hasModelEvalCases = true;
        break;
      }
    }
    
    if (!hasModelEvalCases) {
      showToast('Please complete or start Model Evaluation first', 'error');
      // Go to Model Evaluation instead
      await startModelEvalMode(STATE.user.id, STATE);
      return;
    }
    
    // Go to CoT Evaluation form
    await startCotEvalMode(STATE.user.id, STATE);
    showToast('Switched to CoT Quality Evaluation', 'success');
  }
}

function renderProgress() {
  updateAllProgressBars();
}

function renderCase() {
  renderProgress();
  const d = STATE.model_evaluation.datasets[ACTIVE_DATASET];
  const cases = d.cases;
  if (!cases.length) {
    $("caseCard").innerHTML = `<div class="panel">No cases.</div>`;
    return;
  }
  if (ACTIVE_INDEX < 0) ACTIVE_INDEX = 0;
  if (ACTIVE_INDEX >= cases.length) ACTIVE_INDEX = cases.length - 1;

  d.cursor = ACTIVE_INDEX;
  saveState(STATE.user.id, STATE);

  const c = cases[ACTIVE_INDEX];
  const caseId = c.id;

  const order = buildBlindedOrder(STATE.user.id, ACTIVE_DATASET, caseId);
  const labelMap = {};
  order.forEach((k, idx) => labelMap[k] = String.fromCharCode("A".charCodeAt(0) + idx));

  const existing = d.answers[caseId] || null;
  console.log("Rendering case:", caseId, "Existing answer:", existing);
  console.log("All answers for dataset:", ACTIVE_DATASET, d.answers);

  // Show hardness indicator for RexGradient
  const hardnessIndicator = (ACTIVE_DATASET === 'rexgradient' && c.hardness) 
    ? `<div class="kv" style="background:rgba(90,123,181,0.1);padding:8px;border-radius:6px;margin-top:8px;">
         <div class="k" style="color:var(--accent);">Hardness Level</div>
         <div class="v" style="color:var(--accent);font-weight:600;">Level ${escapeHtml(c.hardness)} / 4</div>
       </div>` 
    : '';

  $("caseCard").innerHTML = `
    <div class="case-grid">
      <div class="panel case-info">
        <h2>Case Information</h2>
        <div class="kv"><div class="k">Indication</div><div class="v">${escapeHtml(c.indication || "-")}</div></div>
        <div class="kv"><div class="k">Findings</div><div class="v">${escapeHtml(c.findings || "-")}</div></div>
        ${hardnessIndicator}

        <div class="kv">
          <div class="k">Comment (optional)</div>
          <textarea id="comment" class="input" rows="3" placeholder="Short note or explanation..." style="resize:vertical;">${escapeHtml(existing?.comment || "")}</textarea>
        </div>
      </div>

      <div class="panel">
        <h2>Model Outputs (Blinded)</h2>
        <div class="outputs" id="outputs"></div>
      </div>
    </div>
  `;

  const outEl = $("outputs");
  outEl.innerHTML = "";
  for (const modelKey of order) {
    const label = labelMap[modelKey];
    const outputText = (c.models[modelKey] || "").trim();
    const existingScore = existing?.model_scores?.[modelKey] || "";
    
    // DEBUG: Get model display name
    const modelInfo = MODEL_COLUMNS.find(m => m.key === modelKey);
    const modelDisplayName = modelInfo ? modelInfo.display : modelKey;
    
    // DEBUG: Log if data is missing
    if (!outputText) {
      console.log(`DEBUG: Empty data for ${modelKey} (${modelDisplayName})`, {
        caseId: c.id,
        modelKey,
        columnName: modelInfo?.col,
        hasModelsObject: !!c.models,
        modelKeys: Object.keys(c.models || {})
      });
    }

    const headId = `head_${modelKey}`;
    const bodyId = `body_${modelKey}`;
    const scoreId = `score_${modelKey}`;

    const card = document.createElement("div");
    card.className = "output";
    card.innerHTML = `
      <div class="output-head" id="${headId}">
        <div class="output-title" style="font-weight:600;font-size:1.05em;">
          Model ${label}
          <span id="rating_badge_${modelKey}" style="margin-left:8px;color:var(--accent);font-size:0.95em;">${existingScore ? `⭐ ${existingScore}` : ''}</span>
        </div>
      </div>
      <div class="output-body" id="${bodyId}">
        <div class="output-text" style="line-height:1.6;margin-bottom:16px;">${escapeHtml(outputText || "[EMPTY]")}</div>
        <div class="rating-row" style="display:flex;align-items:center;gap:12px;">
          <span style="font-weight:600;font-size:13px;">Rating:</span>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;">
            <input type="radio" name="score_${modelKey}" value="1" ${existingScore === "1" ? "checked" : ""}> 1
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;">
            <input type="radio" name="score_${modelKey}" value="2" ${existingScore === "2" ? "checked" : ""}> 2
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;">
            <input type="radio" name="score_${modelKey}" value="3" ${existingScore === "3" ? "checked" : ""}> 3
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;">
            <input type="radio" name="score_${modelKey}" value="4" ${existingScore === "4" ? "checked" : ""}> 4
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;">
            <input type="radio" name="score_${modelKey}" value="5" ${existingScore === "5" ? "checked" : ""}> 5
          </label>
        </div>
      </div>
    `;
    outEl.appendChild(card);

    const head = $(headId);
    const body = $(bodyId);
    const badge = $(`rating_badge_${modelKey}`);

    // Toggle model output on click - close others (accordion behavior)
    head.addEventListener("click", () => {
      const wasOpen = body.classList.contains("open");

      // Close all other models
      document.querySelectorAll('.output-body').forEach(b => {
        if (b !== body) b.classList.remove("open");
      });

      // Toggle this one
      if (!wasOpen) {
        body.classList.add("open");
      }
    });

    // Update badge when radio button changes
    document.querySelectorAll(`input[name="score_${modelKey}"]`).forEach(radio => {
      radio.addEventListener('change', (e) => {
        if (badge) {
          badge.textContent = `⭐ ${e.target.value}`;
        }
      });
    });
  }

  $("appStatus").textContent = existing
    ? `✅ Previously saved (${existing.saved_at.slice(0, 16).replace('T', ' ')}). You can edit and save again.`
    : `Not yet saved. Select scores and click "Save & Next".`;
}

function collectAnswer() {
  const d = STATE.model_evaluation.datasets[ACTIVE_DATASET];
  const c = d.cases[ACTIVE_INDEX];
  const caseId = c.id;

  // Collect per-model scores from radio buttons
  const modelScores = {};
  MODEL_COLUMNS.forEach(m => {
    const radio = document.querySelector(`input[name="score_${m.key}"]:checked`);
    if (radio) {
      modelScores[m.key] = radio.value;
    }
  });

  const comment = document.getElementById("comment")?.value || "";

  return {
    dataset: ACTIVE_DATASET,
    case_id: caseId,
    saved_at: nowISO(),
    model_scores: modelScores,
    comment,
  };
}

function validateAnswer(ans) {
  const scoredCount = Object.keys(ans.model_scores || {}).length;
  const totalModels = MODEL_COLUMNS.length;
  if (scoredCount < totalModels) {
    const missing = totalModels - scoredCount;
    return `${missing} model(s) not scored. Please score all models.`;
  }
  return null;
}

async function onSaveNext() {
  const ans = collectAnswer();
  const err = validateAnswer(ans);
  if (err) {
    $("appStatus").textContent = `⚠️ ${err}`;
    showToast(err, 'error');
    return;
  }

  const d = STATE.model_evaluation.datasets[ACTIVE_DATASET];
  d.answers[ans.case_id] = ans;
  STATE.audit.last_saved_at = nowISO();
  STATE.audit.save_count = (STATE.audit.save_count || 0) + 1;

  console.log("Saved answer for case:", ans.case_id, "Answer:", ans);
  console.log("All answers now:", d.answers);

  // Save to localStorage first (backup)
  try {
    saveState(STATE.user.id, STATE);
  } catch (e) {
    console.error('LocalStorage save failed:', e);
  }

  // Save to Supabase (primary)
  const synced = await supabaseSave(STATE.user.id, STATE);
  if (synced) {
    showToast('Saved to cloud!', 'success');
  } else {
    showToast('Saved locally (cloud sync failed)', 'warning');
  }

  // Update status
  const { totalAll, doneAll } = computeOverallProgress();
  $("appStatus").textContent = `✅ Saved (${doneAll}/${totalAll} completed)`;

  // Move to next case (automatically switch datasets if needed)
  if (ACTIVE_INDEX < d.cases.length - 1) {
    ACTIVE_INDEX += 1;
    renderCase();
  } else {
    // End of current dataset, move to next dataset
    const currentIdx = DATASETS.findIndex(ds => ds.key === ACTIVE_DATASET);
    if (currentIdx < DATASETS.length - 1) {
      ACTIVE_DATASET = DATASETS[currentIdx + 1].key;
      ACTIVE_INDEX = 0;
      renderCase();
    } else {
      // All model evaluation completed - transition to CoT evaluation
      STATE.model_evaluation.completed = true;
      await supabaseSave(STATE.user.id, STATE);
      
      $("appStatus").textContent = "🎉 Model Evaluation completed! Moving to CoT Evaluation...";
      showToast('Model Evaluation complete! Starting CoT Evaluation...', 'success');
      
      // Transition to CoT evaluation after a brief delay
      setTimeout(async () => {
        await startCotEvalMode(STATE.user.id, STATE);
      }, 1500);
    }
  }
}

function onPrev() {
  // Move to previous case (automatically switch datasets if needed)
  const d = STATE.model_evaluation.datasets[ACTIVE_DATASET];
  if (ACTIVE_INDEX > 0) {
    ACTIVE_INDEX -= 1;
    renderCase();
  } else {
    // At beginning of current dataset, move to previous dataset
    const currentIdx = DATASETS.findIndex(ds => ds.key === ACTIVE_DATASET);
    if (currentIdx > 0) {
      ACTIVE_DATASET = DATASETS[currentIdx - 1].key;
      ACTIVE_INDEX = STATE.model_evaluation.datasets[ACTIVE_DATASET].cases.length - 1;
      renderCase();
    } else {
      // At first case of first dataset - go back to data quality mode
      if (STATE.data_quality && STATE.data_quality.cases.length > 0) {
        setHidden($("screenApp"), true);
        setHidden($("screenHardness"), false);
        renderHardnessCase();
        showToast('Returning to Data Quality Assessment', 'info');
      } else {
        $("appStatus").textContent = "Already at the first case.";
      }
    }
  }
}

function onExport() {
  if (!STATE) return;

  const { totalAll, doneAll } = computeOverallProgress();

  const exportObj = {
    exported_at: nowISO(),
    version: STATE.version,
    user: STATE.user,
    config: STATE.config,
    audit: {
      ...STATE.audit,
      export_count: (STATE.audit.export_count || 0) + 1,
      total_cases: totalAll,
      completed_cases: doneAll,
      completion_percentage: totalAll ? Math.round((doneAll / totalAll) * 100) : 0
    },
    model_map: Object.fromEntries(MODEL_COLUMNS.map(m => [m.key, { column: m.col, display: m.display }])),
    datasets: {},
  };

  for (const ds of DATASETS) {
    const d = STATE.model_evaluation.datasets[ds.key];
    exportObj.datasets[ds.key] = {
      cases: d.cases.map(c => ({ 
        id: c.id,
        hardness: c.hardness || null // Include hardness for analysis
      })),
      answers: d.answers,
      hardness_distribution: d.hardness_distribution || null, // Include hardness stats (RexGradient only)
      stats: {
        total: d.cases.length,
        completed: Object.keys(d.answers || {}).length
      }
    };
  }

  // Update audit in state
  STATE.audit.export_count = exportObj.audit.export_count;
  saveState(STATE.user.id, STATE);

  downloadText(`rater_results_${STATE.user.id}_${new Date().toISOString().slice(0, 10)}.json`, JSON.stringify(exportObj, null, 2));
  showToast('Sonuclar indirildi!', 'success');
}

async function onReset() {
  if (!STATE) { $("loginStatus").textContent = "No active user."; return; }
  const ok = confirm(`⚠️ WARNING: This will permanently delete ALL ratings and progress for "${STATE.user.id}" from both local storage and cloud.\n\nThis action cannot be undone!\n\nAre you sure you want to continue?`);
  if (!ok) return;

  const userId = STATE.user.id;

  // Clear auto-save timer
  if (autoSaveTimer) {
    clearInterval(autoSaveTimer);
    autoSaveTimer = null;
  }

  // Delete from Supabase
  try {
    const url = `${SUPABASE_URL}/rest/v1/ratings?user_id=eq.${encodeURIComponent(userId)}`;
    const resp = await fetch(url, {
      method: "DELETE",
      headers: {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": `Bearer ${SUPABASE_ANON_KEY}`,
        "Content-Type": "application/json",
      }
    });
    if (!resp.ok) {
      console.warn("Supabase delete failed:", resp.status);
    }
  } catch (err) {
    console.warn("Supabase delete error:", err);
  }

  // Delete from localStorage
  localStorage.removeItem(storageKey(userId));

  showToast('All progress deleted from cloud and local storage', 'info');
  setTimeout(() => location.reload(), 500);
}


// ===== HARDNESS EVALUATION MODE =====

function renderHardnessCase() {
  const h = STATE.data_quality;
  const total = h.cases.length;
  if (!total) {
    $("hardnessCard").innerHTML = `<div class="panel">No data quality cases loaded.</div>`;
    return;
  }

  const idx = h.cursor || 0;
  if (h.cursor < 0) h.cursor = 0;
  if (h.cursor >= total) h.cursor = total - 1;

  const c = h.cases[idx];
  const caseId = c.id;
  const existing = h.answers[caseId];

  // Update all progress bars
  updateAllProgressBars();

  // Render case card
  $("hardnessCard").innerHTML = `
    <div class="panel case-info">
      <div style="margin-bottom:16px;">
        <div style="font-weight:600;font-size:14px;">Case ${idx + 1}/${total}</div>
      </div>

      <div style="margin-bottom:12px;">
        <div style="font-size:11px;text-transform:uppercase;color:var(--muted);margin-bottom:4px;">Indication</div>
        <div style="line-height:1.6;">${escapeHtml(c.indication || "[EMPTY]")}</div>
      </div>

      <div style="margin-bottom:12px;">
        <div style="font-size:11px;text-transform:uppercase;color:var(--muted);margin-bottom:4px;">Findings</div>
        <div style="line-height:1.6;">${escapeHtml(c.findings || "[EMPTY]")}</div>
      </div>

      <div style="margin-bottom:16px;">
        <div style="font-size:11px;text-transform:uppercase;color:var(--muted);margin-bottom:4px;">Ground Truth Impression</div>
        <div style="line-height:1.6;color:var(--accent);">${escapeHtml(c.ground_truth || "[EMPTY]")}</div>
      </div>

      <div style="margin-bottom:16px;padding:12px;background:var(--bg-secondary);border:1px solid var(--border);border-radius:8px;">
        <div style="font-size:11px;text-transform:uppercase;color:var(--muted);margin-bottom:6px;">Chain-of-Thought (CoT)</div>
        <div style="line-height:1.6;font-size:13px;">${escapeHtml(c.cot || "[NO COT]")}</div>
      </div>

      <div style="margin-bottom:12px;">
        <label style="font-size:13px;font-weight:600;display:block;margin-bottom:8px;">Hardness Rating (1-4):</label>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="hardnessRating" value="1" ${existing?.hardness === "1" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">1</span>
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="hardnessRating" value="2" ${existing?.hardness === "2" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">2</span>
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="hardnessRating" value="3" ${existing?.hardness === "3" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">3</span>
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="hardnessRating" value="4" ${existing?.hardness === "4" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">4</span>
          </label>
        </div>
        <div style="font-size:11px;color:var(--muted);margin-top:6px;">1=trivial, 2=simple, 3=moderate, 4=hard</div>
      </div>

      <div style="margin-bottom:12px;">
        <label style="font-size:13px;font-weight:600;display:block;margin-bottom:8px;">CoT Quality Rating (1-5):</label>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="cotRating" value="1" ${existing?.cot_quality === "1" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">1</span>
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="cotRating" value="2" ${existing?.cot_quality === "2" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">2</span>
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="cotRating" value="3" ${existing?.cot_quality === "3" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">3</span>
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="cotRating" value="4" ${existing?.cot_quality === "4" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">4</span>
          </label>
          <label style="display:flex;align-items:center;gap:4px;cursor:pointer;padding:6px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary);">
            <input type="radio" name="cotRating" value="5" ${existing?.cot_quality === "5" ? "checked" : ""} style="width:16px;height:16px;accent-color:#5a7bb5;" />
            <span style="font-weight:600;">5</span>
          </label>
        </div>
        <div style="font-size:11px;color:var(--muted);margin-top:6px;">1=poor, 2=fair, 3=good, 4=very good, 5=excellent</div>
      </div>

      <div style="margin-bottom:12px;">
        <label style="font-size:13px;font-weight:600;display:block;margin-bottom:8px;">Optional Comment:</label>
        <textarea id="hardnessComment" rows="3" placeholder="Any notes about this case..." style="width:100%;padding:10px;font-size:13px;background:var(--bg-secondary);border:1px solid var(--border);color:var(--text);border-radius:8px;resize:vertical;">${escapeHtml(existing?.comment || "")}</textarea>
      </div>
    </div>
  `;

  $("hardnessStatus").textContent = existing
    ? `✅ Previously saved (${existing.saved_at.slice(0, 16).replace('T', ' ')}). You can edit and save again.`
    : `Not yet saved. Select hardness and click "Save & Next".`;
}

function collectHardnessAnswer() {
  const h = STATE.data_quality;
  const c = h.cases[h.cursor];
  const caseId = c.id;

  const hardnessRadio = document.querySelector('input[name="hardnessRating"]:checked');
  const hardnessRating = hardnessRadio ? hardnessRadio.value : "";

  const cotRadio = document.querySelector('input[name="cotRating"]:checked');
  const cotQuality = cotRadio ? cotRadio.value : "";

  const comment = document.getElementById("hardnessComment")?.value || "";

  return {
    case_id: caseId,
    system_hardness: c.hardness, // Original system assignment
    hardness: hardnessRating, // User's hardness rating (1-5)
    cot_quality: cotQuality, // User's CoT quality rating (1-5)
    comment,
    saved_at: nowISO(),
  };
}

function validateHardnessAnswer(ans) {
  if (!ans.hardness || ans.hardness === "") {
    return "Please select a hardness level (1-5).";
  }
  if (!ans.cot_quality || ans.cot_quality === "") {
    return "Please select a CoT quality rating (1-5).";
  }
  return null;
}

async function onHardnessSaveNext() {
  const ans = collectHardnessAnswer();
  const err = validateHardnessAnswer(ans);
  if (err) {
    $("hardnessStatus").textContent = `❌ ${err}`;
    showToast(err, 'error');
    return;
  }

  const h = STATE.data_quality;
  h.answers[ans.case_id] = ans;

  // Save to localStorage
  try {
    saveState(STATE.user.id, STATE);
  } catch (e) {
    console.error('LocalStorage save failed:', e);
  }

  // Save to Supabase
  const synced = await supabaseSave(STATE.user.id, STATE);
  if (synced) {
    showToast('Saved to cloud!', 'success');
  } else {
    showToast('Saved locally (cloud sync failed)', 'warning');
  }

  const done = Object.keys(h.answers).length;
  const total = h.cases.length;
  $("hardnessStatus").textContent = `✅ Saved (${done}/${total} completed)`;

  if (h.cursor < total - 1) {
    h.cursor += 1;
    renderHardnessCase();
  } else {
    // All data quality cases completed - transition to model evaluation
    STATE.data_quality.completed = true;

    // Save state before transitioning
    await supabaseSave(STATE.user.id, STATE);

    // Hide hardness screen and show model evaluation
    setHidden($("screenHardness"), true);
    await startModelEvalMode(STATE.user.id, STATE);

    showToast('Data Quality completed! Starting Model Evaluation...', 'success');
  }
}

function onHardnessPrev() {
  const h = STATE.data_quality;
  if (h.cursor > 0) {
    h.cursor -= 1;
    renderHardnessCase();
  } else {
    $("hardnessStatus").textContent = "Already at the first case.";
  }
}

// ===== COT EVALUATION MODE =====

function renderCotCase() {
  const cot = STATE.cot_evaluation;
  const total = cot.cases.length;
  if (!total) {
    $("cotCard").innerHTML = `<div class="panel">No CoT evaluation cases loaded.</div>`;
    return;
  }

  const idx = cot.cursor || 0;
  if (cot.cursor < 0) cot.cursor = 0;
  if (cot.cursor >= total) cot.cursor = total - 1;

  const c = cot.cases[idx];
  const caseId = c.id;
  const existing = cot.answers[caseId];

  // Update all progress bars
  updateAllProgressBars();

  // Render case card
  $("cotCard").innerHTML = `
    <div class="case-grid">
      <div class="panel case-info">
        <h2>Case ${idx + 1}/${total} <span style="color:var(--muted);font-weight:400;font-size:0.9em;">(${c.dataset})</span></h2>
        <div class="kv"><div class="k">Indication</div><div class="v">${escapeHtml(c.indication || "-")}</div></div>
        <div class="kv"><div class="k">Findings</div><div class="v">${escapeHtml(c.findings || "-")}</div></div>
      </div>

      <div class="panel">
        <h2>Chain-of-Thought Output</h2>
        <div style="padding:16px;background:var(--bg-secondary);border:1px solid var(--border);border-radius:8px;margin-bottom:16px;">
          <div style="line-height:1.7;white-space:pre-wrap;font-family:system-ui;">${escapeHtml(c.cot || "[NO COT]")}</div>
        </div>

        <div style="margin-bottom:16px;">
          <label style="font-size:14px;font-weight:600;display:block;margin-bottom:12px;">Rate the CoT Quality (1-5):</label>
          <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:8px;">
            <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:10px 16px;border:2px solid var(--border);border-radius:8px;background:var(--bg-secondary);transition:all 0.2s;">
              <input type="radio" name="cotQuality" value="1" ${existing?.quality === "1" ? "checked" : ""} style="width:18px;height:18px;accent-color:#5a7bb5;" />
              <span style="font-weight:600;font-size:15px;">1</span>
            </label>
            <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:10px 16px;border:2px solid var(--border);border-radius:8px;background:var(--bg-secondary);transition:all 0.2s;">
              <input type="radio" name="cotQuality" value="2" ${existing?.quality === "2" ? "checked" : ""} style="width:18px;height:18px;accent-color:#5a7bb5;" />
              <span style="font-weight:600;font-size:15px;">2</span>
            </label>
            <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:10px 16px;border:2px solid var(--border);border-radius:8px;background:var(--bg-secondary);transition:all 0.2s;">
              <input type="radio" name="cotQuality" value="3" ${existing?.quality === "3" ? "checked" : ""} style="width:18px;height:18px;accent-color:#5a7bb5;" />
              <span style="font-weight:600;font-size:15px;">3</span>
            </label>
            <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:10px 16px;border:2px solid var(--border);border-radius:8px;background:var(--bg-secondary);transition:all 0.2s;">
              <input type="radio" name="cotQuality" value="4" ${existing?.quality === "4" ? "checked" : ""} style="width:18px;height:18px;accent-color:#5a7bb5;" />
              <span style="font-weight:600;font-size:15px;">4</span>
            </label>
            <label style="display:flex;align-items:center;gap:6px;cursor:pointer;padding:10px 16px;border:2px solid var(--border);border-radius:8px;background:var(--bg-secondary);transition:all 0.2s;">
              <input type="radio" name="cotQuality" value="5" ${existing?.quality === "5" ? "checked" : ""} style="width:18px;height:18px;accent-color:#5a7bb5;" />
              <span style="font-weight:600;font-size:15px;">5</span>
            </label>
          </div>
          <div style="font-size:12px;color:var(--muted);margin-top:8px;">
            1=poor (illogical, incorrect), 2=fair (some errors), 3=good (mostly correct), 4=very good (clear, accurate), 5=excellent (perfect reasoning)
          </div>
        </div>

        <div style="margin-bottom:12px;">
          <label style="font-size:13px;font-weight:600;display:block;margin-bottom:8px;">Optional Comment:</label>
          <textarea id="cotComment" rows="3" placeholder="Any notes about this CoT..." class="input" style="width:100%;resize:vertical;">${escapeHtml(existing?.comment || "")}</textarea>
        </div>
      </div>
    </div>
  `;

  $("cotStatus").textContent = existing
    ? `✅ Previously saved (${existing.saved_at.slice(0, 16).replace('T', ' ')}). You can edit and save again.`
    : `Not yet saved. Select quality rating and click "Save & Next".`;
}

function collectCotAnswer() {
  const cot = STATE.cot_evaluation;
  const c = cot.cases[cot.cursor];
  const caseId = c.id;

  const qualityRadio = document.querySelector('input[name="cotQuality"]:checked');
  const quality = qualityRadio ? qualityRadio.value : "";

  const comment = document.getElementById("cotComment")?.value || "";

  return {
    case_id: caseId,
    dataset: c.dataset,
    quality,
    comment,
    saved_at: nowISO(),
  };
}

function validateCotAnswer(ans) {
  if (!ans.quality || ans.quality === "") {
    return "Please select a CoT quality rating (1-5).";
  }
  return null;
}

async function onCotSaveNext() {
  const ans = collectCotAnswer();
  const err = validateCotAnswer(ans);
  if (err) {
    $("cotStatus").textContent = `❌ ${err}`;
    showToast(err, 'error');
    return;
  }

  const cot = STATE.cot_evaluation;
  cot.answers[ans.case_id] = ans;

  // Save to localStorage
  try {
    saveState(STATE.user.id, STATE);
  } catch (e) {
    console.error('LocalStorage save failed:', e);
  }

  // Save to Supabase
  const synced = await supabaseSave(STATE.user.id, STATE);
  if (synced) {
    showToast('Saved to cloud!', 'success');
  } else {
    showToast('Saved locally (cloud sync failed)', 'warning');
  }

  const done = Object.keys(cot.answers).length;
  const total = cot.cases.length;
  $("cotStatus").textContent = `✅ Saved (${done}/${total} completed)`;

  if (cot.cursor < total - 1) {
    cot.cursor += 1;
    renderCotCase();
  } else {
    STATE.cot_evaluation.completed = true;
    await supabaseSave(STATE.user.id, STATE);
    // Update progress bars to show 30/30
    updateAllProgressBars();
    $("cotStatus").textContent = "🎉 All CoT evaluation completed!";
    showToast('Congratulations! All evaluations completed!', 'success');
  }
}

function onCotPrev() {
  const cot = STATE.cot_evaluation;
  if (cot.cursor > 0) {
    cot.cursor -= 1;
    renderCotCase();
  } else {
    // At first CoT case - go back to Model Evaluation
    if (STATE.model_evaluation && Object.keys(STATE.model_evaluation.datasets).length > 0) {
      setHidden($("screenCot"), true);
      setHidden($("screenApp"), false);
      renderCase();
      showToast('Returning to Model Evaluation', 'info');
    } else {
      $("cotStatus").textContent = "Already at the first case.";
    }
  }
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#039;");
}
