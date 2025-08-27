// Set to wherever the FastAPI service is running:
const API_BASE = (location.port === '8080') ? 'http://localhost:8000' : '';

async function jfetch(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

function renderRows(tbody, rows, cols, formatters = {}) {
  tbody.innerHTML = '';
  if (!rows || rows.length === 0) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.className = 'p-4 text-center text-muted-foreground';
    td.setAttribute('colspan', cols.length.toString());
    td.textContent = 'No data found';
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  
  for (const r of rows) {
    const tr = document.createElement('tr');
    tr.className = 'border-b border-border hover:bg-muted/50';
    for (const c of cols) {
      const td = document.createElement('td');
      td.className = 'p-4 align-middle';
      let value = r[c] ?? '';
      
      // Apply formatters
      if (formatters[c]) {
        const formatted = formatters[c](value);
        if (typeof formatted === 'string' && formatted.includes('<')) {
          td.innerHTML = formatted;
        } else {
          td.textContent = formatted;
        }
      } else if (typeof value === 'number' && value > 1000) {
        td.textContent = value.toLocaleString();
      } else {
        td.textContent = value;
      }
      
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

function renderError(tbody, message, colCount) {
  tbody.innerHTML = '';
  const tr = document.createElement('tr');
  const td = document.createElement('td');
  td.className = 'p-4 text-center text-red-600';
  td.setAttribute('colspan', colCount.toString());
  td.textContent = message;
  tr.appendChild(td);
  tbody.appendChild(tr);
}

function showLoading(tbody, colCount) {
  tbody.innerHTML = '';
  const tr = document.createElement('tr');
  const td = document.createElement('td');
  td.className = 'p-4 text-center text-muted-foreground loading';
  td.setAttribute('colspan', colCount.toString());
  td.textContent = 'Loading...';
  tr.appendChild(td);
  tbody.appendChild(tr);
}

async function loadWordcount() {
  try {
    const tbody = document.querySelector('#tbl_wc_body');
    if (!tbody) {
      console.error('Table body #tbl_wc_body not found');
      return;
    }
    
    showLoading(tbody, 2);
    const d = document.getElementById('date_wc').value;
    const rows = await jfetch(`${API_BASE}/api/agency/wordcount?date=${encodeURIComponent(d)}`);
    renderRows(tbody, rows, ['agency_name', 'total_words'], {
      total_words: (val) => val ? val.toLocaleString() : 0
    });
  } catch (err) {
    console.error('Error loading wordcount:', err);
    const tbody = document.querySelector('#tbl_wc_body');
    if (tbody) renderError(tbody, 'Error loading data: ' + err.message, 2);
  }
}

async function loadChecksums() {
  try {
    const tbody = document.querySelector('#tbl_ck_body');
    if (!tbody) {
      console.error('Table body #tbl_ck_body not found');
      return;
    }
    
    showLoading(tbody, 2);
    const d = document.getElementById('date_ck').value;
    const rows = await jfetch(`${API_BASE}/api/agency/checksum?date=${encodeURIComponent(d)}`);
    renderRows(tbody, rows, ['agency_name', 'agency_hash'], {
      agency_hash: (val) => val ? val.substring(0, 16) + '...' : ''
    });
  } catch (err) {
    console.error('Error loading checksums:', err);
    const tbody = document.querySelector('#tbl_ck_body');
    if (tbody) renderError(tbody, 'Error loading data: ' + err.message, 2);
  }
}

async function loadDiff() {
  try {
    const tbody = document.querySelector('#tbl_diff_body');
    if (!tbody) {
      console.error('Table body #tbl_diff_body not found');
      return;
    }
    
    showLoading(tbody, 2);
    const f = document.getElementById('from').value;
    const t = document.getElementById('to').value;
    const rows = await jfetch(`${API_BASE}/api/changes?from=${encodeURIComponent(f)}&to=${encodeURIComponent(t)}`);
    renderRows(tbody, rows, ['section_citation', 'change_type'], {
      change_type: (val) => {
        const colors = { ADDED: 'text-green-600', REMOVED: 'text-red-600', MODIFIED: 'text-yellow-600' };
        const span = document.createElement('span');
        span.className = colors[val] || 'text-muted-foreground';
        span.textContent = val;
        return span.outerHTML;
      }
    });
  } catch (err) {
    console.error('Error loading diff:', err);
    const tbody = document.querySelector('#tbl_diff_body');
    if (tbody) renderError(tbody, 'Error loading data: ' + err.message, 2);
  }
}

async function loadPart() {
  try {
    const tbody = document.querySelector('#tbl_part_body');
    if (!tbody) {
      console.error('Table body #tbl_part_body not found');
      return;
    }
    
    showLoading(tbody, 4);
    const title = document.getElementById('title').value;
    const part = document.getElementById('part').value;
    const d = document.getElementById('date_part').value;
    const rows = await jfetch(`${API_BASE}/api/part?title=${encodeURIComponent(title)}&part=${encodeURIComponent(part)}&date=${encodeURIComponent(d)}`);
    
    tbody.innerHTML = '';
    
    if (!rows || rows.length === 0) {
      renderError(tbody, 'No sections found for this part', 4);
      return;
    }
    
    rows.forEach((r, idx) => {
      const tr = document.createElement('tr');
      tr.className = 'border-b border-border hover:bg-muted/50';
      tr.innerHTML = `
        <td class="p-4 align-middle">${idx+1}</td>
        <td class="p-4 align-middle font-mono text-sm">${r.section_citation}</td>
        <td class="p-4 align-middle">${r.section_heading || ''}</td>
        <td class="p-4 align-middle text-right">${r.word_count ? r.word_count.toLocaleString() : 0}</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error('Error loading part:', err);
    const tbody = document.querySelector('#tbl_part_body');
    if (tbody) renderError(tbody, 'Error loading data: ' + err.message, 4);
  }
}

// Tab functionality
function switchTab(tabName) {
  // Hide all tab panels
  document.querySelectorAll('[id$="-tab"]').forEach(tab => {
    tab.classList.add('hidden');
  });
  
  // Remove active class from all tab buttons
  document.querySelectorAll('[data-tab]').forEach(btn => {
    btn.classList.remove('bg-background', 'text-foreground', 'shadow-sm');
    btn.classList.add('text-muted-foreground', 'hover:text-foreground');
  });
  
  // Show selected tab panel
  const panel = document.getElementById(tabName + '-tab');
  if (panel) panel.classList.remove('hidden');
  
  // Activate selected tab button
  const btn = document.querySelector(`[data-tab="${tabName}"]`);
  if (btn) {
    btn.classList.add('bg-background', 'text-foreground', 'shadow-sm');
    btn.classList.remove('text-muted-foreground', 'hover:text-foreground');
  }
}

// Theme functionality
function toggleTheme() {
  const html = document.documentElement;
  const isDark = html.classList.contains('dark');
  
  if (isDark) {
    html.classList.remove('dark');
    localStorage.setItem('theme', 'light');
  } else {
    html.classList.add('dark');
    localStorage.setItem('theme', 'dark');
  }
}

// API Status check
async function checkApiStatus() {
  const statusEl = document.getElementById('api-status');
  const textEl = document.getElementById('status-text');
  
  if (!statusEl || !textEl) {
    console.warn('API status elements not found');
    return;
  }
  
  try {
    await jfetch(`${API_BASE}/healthz`);
    statusEl.className = 'flex items-center gap-2 px-3 py-1.5 text-sm bg-green-100 text-green-800 rounded-full dark:bg-green-900 dark:text-green-200';
    textEl.textContent = 'API Connected';
  } catch (err) {
    statusEl.className = 'flex items-center gap-2 px-3 py-1.5 text-sm bg-red-100 text-red-800 rounded-full dark:bg-red-900 dark:text-red-200';
    textEl.textContent = 'API Disconnected';
  }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
  // Set theme from localStorage
  const savedTheme = localStorage.getItem('theme');
  if (savedTheme === 'dark' || (!savedTheme && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
    document.documentElement.classList.add('dark');
  }
  
  // Check API status
  checkApiStatus();
  
  // Set default tab
  switchTab('overview');
  
  // Set default dates
  const today = '2025-08-22';
  const yesterday = '2025-08-21';
  
  document.getElementById('date_wc').value = today;
  document.getElementById('date_ck').value = today;
  document.getElementById('date_part').value = today;
  document.getElementById('from').value = yesterday;
  document.getElementById('to').value = today;
});

// Missing functions for Historical Analysis tab
async function loadHistoricalTrends() {
  try {
    const tbody = document.querySelector('#tbl_trends_body');
    if (!tbody) {
      console.error('Table body #tbl_trends_body not found');
      return;
    }
    
    showLoading(tbody, 3);
    const startDate = document.getElementById('trend_start').value;
    const endDate = document.getElementById('trend_end').value;
    
    const rows = await jfetch(`${API_BASE}/api/historical/agency-trends?start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`);
    renderRows(tbody, rows, ['version_date', 'agency_name', 'total_words'], {
      total_words: (val) => val ? val.toLocaleString() : 0,
      version_date: (val) => val ? new Date(val).toLocaleDateString() : ''
    });
  } catch (err) {
    console.error('Error loading historical trends:', err);
    const tbody = document.querySelector('#tbl_trends_body');
    if (tbody) renderError(tbody, 'Error loading trends: ' + err.message, 3);
  }
}

async function loadAvailableDates() {
  try {
    const container = document.getElementById('dates_container');
    if (!container) {
      console.error('Container #dates_container not found');
      return;
    }
    
    container.innerHTML = '<p class="text-center text-muted-foreground">Loading dates...</p>';
    
    const rows = await jfetch(`${API_BASE}/api/available-dates`);
    
    if (!rows || rows.length === 0) {
      container.innerHTML = '<p class="text-center text-muted-foreground">No dates available</p>';
      return;
    }
    
    container.innerHTML = rows.map(r => 
      `<div class="px-3 py-2 bg-muted/50 rounded text-sm">${new Date(r.date).toLocaleDateString()}</div>`
    ).join('');
    
  } catch (err) {
    console.error('Error loading available dates:', err);
    const container = document.getElementById('dates_container');
    if (container) container.innerHTML = `<p class="text-center text-red-600">Error: ${err.message}</p>`;
  }
}

async function loadRegulatoryBurden() {
  try {
    const tbody = document.querySelector('#tbl_burden_body');
    if (!tbody) {
      console.error('Table body #tbl_burden_body not found');
      return;
    }
    
    showLoading(tbody, 4);
    const startDate = document.getElementById('burden_start').value;
    const endDate = document.getElementById('burden_end').value;
    
    const rows = await jfetch(`${API_BASE}/api/historical/regulatory-burden?start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}`);
    renderRows(tbody, rows, ['version_date', 'agency_name', 'avg_burden_score', 'total_prohibitions'], {
      avg_burden_score: (val) => val ? val.toFixed(2) : '0.00',
      total_prohibitions: (val) => val ? val.toLocaleString() : 0,
      version_date: (val) => val ? new Date(val).toLocaleDateString() : ''
    });
  } catch (err) {
    console.error('Error loading regulatory burden:', err);
    const tbody = document.querySelector('#tbl_burden_body');
    if (tbody) renderError(tbody, 'Error loading burden data: ' + err.message, 4);
  }
}

async function loadBurdenDistribution() {
  try {
    const tbody = document.querySelector('#tbl_burden_body');
    if (!tbody) {
      console.error('Table body #tbl_burden_body not found');
      return;
    }
    
    showLoading(tbody, 4);
    const date = document.getElementById('date_burden').value || '2025-08-22';
    
    const rows = await jfetch(`${API_BASE}/api/metrics/burden-distribution?date=${encodeURIComponent(date)}`);
    renderRows(tbody, rows, ['agency_name', 'avg_burden', 'total_prohibitions', 'total_requirements'], {
      avg_burden: (val) => val ? val.toFixed(2) : '0.00',
      total_prohibitions: (val) => val ? val.toLocaleString() : 0,
      total_requirements: (val) => val ? val.toLocaleString() : 0
    });
  } catch (err) {
    console.error('Error loading burden distribution:', err);
    const tbody = document.querySelector('#tbl_burden_body');
    if (tbody) renderError(tbody, 'Error loading burden distribution: ' + err.message, 4);
  }
}
