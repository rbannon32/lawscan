// Set to wherever the FastAPI service is running:
const API_BASE = (location.port === '8080') ? 'http://localhost:8000' : '';
const AI_API_BASE = 'http://localhost:8001';

console.log('JavaScript loaded, API_BASE:', API_BASE, 'AI_API_BASE:', AI_API_BASE);

async function jfetch(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) {
      const errorText = await r.text();
      throw new Error(`API Error (${r.status}): ${errorText || 'Unknown error'}`);
    }
    return await r.json();
  } catch (err) {
    if (err.name === 'TypeError' && err.message.includes('fetch')) {
      throw new Error('API server unavailable. Please ensure the API is running on http://localhost:8000');
    }
    throw err;
  }
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

// Removed loadDiff function - no longer needed without Regulatory Changes widget

// Legacy function - replaced by new hierarchical browser
async function loadPartLegacy() {
  try {
    const tbody = document.querySelector('#tbl_part_body');
    if (!tbody) {
      console.log('Legacy part table not found - using new browser interface');
      return;
    }
    
    showLoading(tbody, 4);
    const title = document.getElementById('title')?.value;
    const part = document.getElementById('part')?.value;
    const d = document.getElementById('date_part')?.value;
    
    if (!title || !part || !d) {
      renderError(tbody, 'Please provide title, part, and date', 4);
      return;
    }
    
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
  console.log('Switching to tab:', tabName);
  
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
  
  // Check AI status when switching to chat tab
  if (tabName === 'chat') {
    console.log('Detected chat tab, checking AI status...');
    checkAiStatus();
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
  const statusEl = document.getElementById('apiStatus');
  const textEl = document.getElementById('apiStatusText');
  
  if (!statusEl || !textEl) {
    console.warn('API status elements not found');
    return;
  }
  
  try {
    await jfetch(`${API_BASE}/healthz`);
    statusEl.className = 'h-2 w-2 bg-green-500 rounded-full';
    textEl.textContent = 'Connected';
  } catch (err) {
    statusEl.className = 'h-2 w-2 bg-red-500 rounded-full';
    textEl.textContent = 'Disconnected';
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
  const today = '2025-08-28';
  const yesterday = '2025-08-27';
  
  // Set dates with null checks
  const setValueSafely = (id, value) => {
    const element = document.getElementById(id);
    if (element) element.value = value;
  };
  
  setValueSafely('date_wc', today);
  setValueSafely('date_ck', today);
  setValueSafely('date_part', today);
  setValueSafely('date_burden', today);
  
  // Add Enter key support for search
  const searchInput = document.getElementById('regulation_search');
  if (searchInput) {
    searchInput.addEventListener('keypress', function(e) {
      if (e.key === 'Enter') {
        searchRegulations();
      }
    });
  }
  
  // Add ESC key support for modal
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      const modal = document.getElementById('section-modal');
      if (modal && !modal.classList.contains('hidden')) {
        closeSectionModal();
      }
    }
  });
  
  // Close modal when clicking outside
  const modal = document.getElementById('section-modal');
  if (modal) {
    modal.addEventListener('click', function(e) {
      if (e.target === modal) {
        closeSectionModal();
      }
    });
  }
});

// Historical Analysis functions removed - no longer needed

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

// ========================== ENHANCED PART BROWSER ==========================

// Current state
const BROWSE_DATE = '2025-08-28';
let currentTitle = null;
let currentPart = null;
let currentSections = [];
let searchInProgress = false;

// Load all available titles with statistics
async function loadTitles() {
  try {
    
    const container = document.getElementById('titles-list');
    container.innerHTML = '<p class="text-center text-muted-foreground">Loading titles...</p>';
    
    let titles;
    try {
      titles = await jfetch(`${API_BASE}/api/browse/titles?date=${encodeURIComponent(BROWSE_DATE)}`);
    } catch (err) {
      console.log('New browse endpoint failed, using fallback approach:', err.message);
      // Fallback: Get available titles from existing endpoints
      container.innerHTML = `
        <div class="text-center py-8">
          <p class="text-muted-foreground mb-4">Enhanced browsing temporarily unavailable</p>
          <p class="text-sm text-muted-foreground">Using basic title list</p>
          <div class="mt-4 space-y-2">
            <div onclick="loadPartsLegacy(3)" class="p-3 rounded-lg border bg-background hover:bg-muted/50 cursor-pointer">
              <h4 class="font-semibold">Title 3</h4>
              <p class="text-sm text-muted-foreground">Click to browse (basic mode)</p>
            </div>
            <div onclick="loadPartsLegacy(7)" class="p-3 rounded-lg border bg-background hover:bg-muted/50 cursor-pointer">
              <h4 class="font-semibold">Title 7</h4>
              <p class="text-sm text-muted-foreground">Click to browse (basic mode)</p>
            </div>
            <div onclick="loadPartsLegacy(21)" class="p-3 rounded-lg border bg-background hover:bg-muted/50 cursor-pointer">
              <h4 class="font-semibold">Title 21</h4>
              <p class="text-sm text-muted-foreground">Click to browse (basic mode)</p>
            </div>
          </div>
        </div>
      `;
      return;
    }
    
    if (!titles || titles.length === 0) {
      container.innerHTML = '<p class="text-center text-muted-foreground">No titles available for this date</p>';
      return;
    }
    
    container.innerHTML = titles.map(title => `
      <div onclick="loadParts(${title.title_num})" 
           class="p-3 rounded-lg border bg-background hover:bg-muted/50 cursor-pointer transition-colors">
        <div class="flex items-center justify-between mb-2">
          <h4 class="font-semibold">Title ${title.title_num}</h4>
          <div class="flex items-center gap-1">
            <span class="px-2 py-1 text-xs bg-primary/10 text-primary rounded-full">
              ${title.parts_count} parts
            </span>
          </div>
        </div>
        <div class="grid grid-cols-2 gap-2 text-xs text-muted-foreground mb-2">
          <span>${title.total_words?.toLocaleString() || 0} words</span>
          <span>${title.sections_count?.toLocaleString() || 0} sections</span>
          <span>Avg burden: ${createBurdenScoreWithTooltip((title.avg_burden_score || 0).toFixed(1))}</span>
          <span>${title.total_prohibitions || 0} prohibitions</span>
        </div>
        <div class="text-xs text-muted-foreground">
          ${title.total_requirements || 0} requirements • ${title.total_enforcement || 0} enforcement terms
        </div>
      </div>
    `).join('');
    
    showWelcome();
    
  } catch (err) {
    console.error('Error loading titles:', err);
    document.getElementById('titles-list').innerHTML = 
      `<p class="text-center text-red-600">Error: ${err.message}</p>`;
  }
}

// Load parts for a specific title
async function loadParts(titleNum) {
  try {
    currentTitle = titleNum;
    
    const container = document.getElementById('parts-list');
    container.innerHTML = '<p class="text-center text-muted-foreground">Loading parts...</p>';
    
    const parts = await jfetch(`${API_BASE}/api/browse/parts?title=${titleNum}&date=${encodeURIComponent(BROWSE_DATE)}`);
    
    if (!parts || parts.length === 0) {
      container.innerHTML = '<p class="text-center text-muted-foreground">No parts found</p>';
      return;
    }
    
    container.innerHTML = parts.map(part => `
      <div onclick="loadSections(${titleNum}, '${part.part_num}')" 
           class="p-4 rounded-lg border bg-background hover:bg-muted/50 cursor-pointer transition-colors">
        <div class="flex items-center justify-between mb-2">
          <div>
            <h4 class="font-semibold">Part ${part.part_num}</h4>
            <p class="text-sm text-muted-foreground">${part.agency_name || 'Unknown Agency'}</p>
          </div>
          <div class="text-right">
            <div class="px-2 py-1 text-xs rounded-full ${
              part.avg_burden_score >= 60 ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200' :
              part.avg_burden_score >= 40 ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200' :
              'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
            }">
              ${createBurdenScoreWithTooltip((part.avg_burden_score || 0).toFixed(1))} burden
            </div>
          </div>
        </div>
        <div class="grid grid-cols-2 gap-2 text-xs text-muted-foreground mb-2">
          <span>${part.sections_count} sections</span>
          <span>${part.total_words?.toLocaleString() || 0} words</span>
          <span>${part.total_prohibitions || 0} prohibitions</span>
          <span>${part.total_requirements || 0} requirements</span>
        </div>
        ${part.highest_burden_section ? 
          `<p class="text-xs text-muted-foreground">📈 Highest burden: ${part.highest_burden_section}</p>` : 
          ''
        }
        ${part.total_cost_refs > 0 ? 
          `<p class="text-xs text-yellow-600">💰 ${part.total_cost_refs} cost references</p>` : 
          ''
        }
      </div>
    `).join('');
    
    // Update UI state
    document.getElementById('current-title').textContent = titleNum;
    document.getElementById('titles-panel').classList.add('hidden');
    document.getElementById('parts-panel').classList.remove('hidden');
    
    hideAllContentPanels();
    showBreadcrumb(`> Title ${titleNum}`);
    
  } catch (err) {
    console.error('Error loading parts:', err);
    document.getElementById('parts-list').innerHTML = 
      `<p class="text-center text-red-600">Error: ${err.message}</p>`;
  }
}

// Load sections for a specific part
async function loadSections(titleNum, partNum, sortBy = 'order') {
  console.log('loadSections called with:', titleNum, partNum, sortBy);
  try {
    currentTitle = titleNum;
    currentPart = partNum;
    
    const container = document.getElementById('sections-list');
    container.innerHTML = '<p class="text-center text-muted-foreground">Loading sections...</p>';
    
    const sections = await jfetch(
      `${API_BASE}/api/browse/sections?title=${titleNum}&part=${encodeURIComponent(partNum)}&date=${encodeURIComponent(BROWSE_DATE)}&sort_by=${sortBy}`
    );
    
    if (!sections || sections.length === 0) {
      container.innerHTML = '<p class="text-center text-muted-foreground">No sections found</p>';
      return;
    }
    
    currentSections = sections;
    
    // Calculate summary stats
    const totalWords = sections.reduce((sum, s) => sum + (s.word_count || 0), 0);
    const avgBurden = sections.reduce((sum, s) => sum + (s.regulatory_burden_score || 0), 0) / sections.length;
    const totalProhibitions = sections.reduce((sum, s) => sum + (s.prohibition_count || 0), 0);
    
    document.getElementById('part-summary').textContent = 
      `${sections.length} sections • ${totalWords.toLocaleString()} words • Avg burden: ${avgBurden.toFixed(1)} • ${totalProhibitions} prohibitions`;
    
    container.innerHTML = sections.map((section, idx) => {
      const riskColor = getRiskColor(section.risk_level);
      return `
        <div class="p-4 rounded-lg border bg-background hover:bg-muted/50 transition-colors cursor-pointer"
             onclick="showSectionText('${section.section_citation}', ${titleNum}, '${partNum}')">
          <div class="flex items-start justify-between mb-2">
            <div class="flex-1">
              <h5 class="font-medium font-mono text-sm mb-1">${section.section_citation}</h5>
              <p class="text-sm text-muted-foreground mb-2">${section.section_heading || 'No heading'}</p>
            </div>
            <div class="flex items-center gap-2 ml-4">
              <span class="px-2 py-1 text-xs rounded-full ${riskColor}">
                ${section.risk_level}
              </span>
              <span class="text-xs text-muted-foreground">
                ${createBurdenScoreWithTooltip((section.regulatory_burden_score || 0).toFixed(1))}
              </span>
              <i data-lucide="eye" class="h-4 w-4 text-muted-foreground ml-2"></i>
            </div>
          </div>
          
          <div class="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs text-muted-foreground mb-2">
            <span>${(section.word_count || 0).toLocaleString()} words</span>
            <span>${section.prohibition_count || 0} prohibitions</span>
            <span>${section.requirement_count || 0} requirements</span>
            <span>${section.enforcement_terms || 0} enforcement</span>
          </div>
          
          ${section.temporal_references > 0 || section.dollar_mentions > 0 ? `
            <div class="flex items-center gap-4 text-xs">
              ${section.temporal_references > 0 ? 
                `<span class="text-blue-600">⏰ ${section.temporal_references} deadlines</span>` : ''
              }
              ${section.dollar_mentions > 0 ? 
                `<span class="text-green-600">💰 ${section.dollar_mentions} cost refs</span>` : ''
              }
            </div>
          ` : ''}
        </div>
      `;
    }).join('');
    
    // Update UI state
    document.getElementById('current-part').textContent = partNum;
    document.getElementById('sort-sections').value = sortBy;
    
    hideAllContentPanels();
    document.getElementById('sections-panel').classList.remove('hidden');
    showBreadcrumb(`> Title ${titleNum} > Part ${partNum}`);
    
  } catch (err) {
    console.error('Error loading sections:', err);
    document.getElementById('sections-list').innerHTML = 
      `<p class="text-center text-red-600">Error: ${err.message}</p>`;
  }
}

// Search across regulations  
async function searchRegulations() {
  console.log('Search function called - UPDATED VERSION');
  
  // Prevent multiple simultaneous searches
  if (searchInProgress) {
    console.log('Search already in progress, ignoring');
    return;
  }
  
  try {
    const searchInput = document.getElementById('regulation_search');
    if (!searchInput) {
      console.error('Search input element not found');
      return;
    }
    const query = searchInput.value.trim();
    console.log('Search query:', query);
    if (!query) {
      console.log('No query provided');
      return;
    }
    
    searchInProgress = true;
    console.log('Search date:', BROWSE_DATE);
    const container = document.getElementById('search-list');
    
    container.innerHTML = '<p class="text-center text-muted-foreground">Searching...</p>';
    
    const results = await jfetch(
      `${API_BASE}/api/browse/search?query=${encodeURIComponent(query)}&date=${encodeURIComponent(BROWSE_DATE)}`
    );
    
    console.log('Search results:', results);
    
    if (!results || results.length === 0) {
      container.innerHTML = `
        <div class="text-center py-8">
          <i data-lucide="search-x" class="h-12 w-12 text-muted-foreground mx-auto mb-4"></i>
          <h4 class="text-lg font-semibold mb-2">No results found</h4>
          <p class="text-muted-foreground mb-4">No regulations found matching "${query}"</p>
          <p class="text-sm text-muted-foreground">Try different keywords or check if data exists for ${BROWSE_DATE}</p>
        </div>
      `;
      // Recreate icons for the new content
      if (window.lucide) window.lucide.createIcons();
      return;
    }
    
    container.innerHTML = results.map(result => {
      const riskColor = getRiskColor(getRiskLevel(result.regulatory_burden_score));
      return `
        <div class="p-4 rounded-lg border bg-background hover:bg-muted/50 transition-colors cursor-pointer"
             onclick="loadSections(${result.title_num}, '${result.part_num}')">
          <div class="flex items-start justify-between mb-2">
            <div class="flex-1">
              <h5 class="font-medium font-mono text-sm mb-1">${result.section_citation}</h5>
              <p class="text-sm text-muted-foreground mb-1">${result.section_heading || 'No heading'}</p>
              <p class="text-xs text-muted-foreground">${result.agency_name} • Title ${result.title_num} Part ${result.part_num}</p>
            </div>
            <div class="flex items-center gap-2 ml-4">
              <span class="px-2 py-1 text-xs rounded-full ${riskColor}">
                ${getRiskLevel(result.regulatory_burden_score)}
              </span>
              <span class="text-xs text-muted-foreground">
                ${createBurdenScoreWithTooltip((result.regulatory_burden_score || 0).toFixed(1))}
              </span>
            </div>
          </div>
          <div class="text-xs text-muted-foreground">
            ${(result.word_count || 0).toLocaleString()} words • Complexity: ${result.complexity_score || 0}
          </div>
        </div>
      `;
    }).join('');
    
    hideAllContentPanels();
    document.getElementById('search-results').classList.remove('hidden');
    showBreadcrumb(`> Search: "${query}"`);
    
  } catch (err) {
    console.error('Error searching regulations:', err);
    const container = document.getElementById('search-list');
    if (container) {
      container.innerHTML = `
        <div class="text-center py-8">
          <i data-lucide="alert-circle" class="h-12 w-12 text-red-500 mx-auto mb-4"></i>
          <h4 class="text-lg font-semibold mb-2 text-red-600">Search Error</h4>
          <p class="text-muted-foreground mb-4">${err.message}</p>
          <p class="text-sm text-muted-foreground">Please check your connection and try again</p>
        </div>
      `;
      // Recreate icons for the new content
      if (window.lucide) window.lucide.createIcons();
    }
  } finally {
    searchInProgress = false;
  }
}

// Helper functions for the new browser
function getRiskLevel(score) {
  if (score >= 80) return 'Very High';
  if (score >= 60) return 'High';  
  if (score >= 40) return 'Medium';
  if (score >= 20) return 'Low';
  return 'Very Low';
}

function getRiskColor(riskLevel) {
  const colors = {
    'Very High': 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    'High': 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
    'Medium': 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
    'Low': 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
    'Very Low': 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
  };
  return colors[riskLevel] || colors['Very Low'];
}

function hideAllContentPanels() {
  document.getElementById('welcome-panel').classList.add('hidden');
  document.getElementById('sections-panel').classList.add('hidden');
  document.getElementById('search-results').classList.add('hidden');
}

function showWelcome() {
  hideAllContentPanels();
  document.getElementById('welcome-panel').classList.remove('hidden');
  hideBreadcrumb();
}

function showTitles() {
  document.getElementById('parts-panel').classList.add('hidden');
  document.getElementById('titles-panel').classList.remove('hidden');
  showWelcome();
  currentTitle = null;
  currentPart = null;
}

function showParts() {
  if (!currentTitle) {
    showTitles();
    return;
  }
  hideAllContentPanels();
  showBreadcrumb(`> Title ${currentTitle}`);
}

function showBreadcrumb(content) {
  document.getElementById('breadcrumb-content').innerHTML = content;
  document.getElementById('breadcrumb').classList.remove('hidden');
}

function hideBreadcrumb() {
  document.getElementById('breadcrumb').classList.add('hidden');
}

function clearSearch() {
  document.getElementById('regulation_search').value = '';
  showWelcome();
}

function sortSections() {
  if (!currentSections || !currentTitle || !currentPart) return;
  
  const sortBy = document.getElementById('sort-sections').value;
  loadSections(currentTitle, currentPart, sortBy);
}

// ========================== SECTION TEXT DISPLAY ==========================

async function showSectionText(sectionCitation, title, part) {
  try {
    // Show modal immediately
    document.getElementById('section-modal').classList.remove('hidden');
    document.getElementById('modal-section-citation').textContent = sectionCitation;
    document.getElementById('modal-section-title').textContent = 'Loading...';
    document.getElementById('modal-section-content').innerHTML = 
      '<p class="text-center text-muted-foreground">Loading section content...</p>';
    
    const date = BROWSE_DATE;
    
    let sectionData;
    try {
      sectionData = await jfetch(
        `${API_BASE}/api/section/text?title=${title}&part=${encodeURIComponent(part)}&section=${encodeURIComponent(sectionCitation)}&date=${encodeURIComponent(date)}`
      );
    } catch (err) {
      console.log('New section endpoint failed, using basic part data');
      // Fallback to existing API
      const partData = await jfetch(`${API_BASE}/api/part?title=${title}&part=${encodeURIComponent(part)}&date=${encodeURIComponent(date)}`);
      const section = partData.find(s => s.section_citation === sectionCitation);
      
      if (!section) {
        throw new Error('Section not found');
      }
      
      sectionData = {
        section_citation: section.section_citation,
        section_heading: section.section_heading,
        section_text: 'Full section text not available in basic mode',
        word_count: section.word_count,
        regulatory_burden_score: section.regulatory_burden_score || 0,
        prohibition_count: section.prohibition_count || 0,
        requirement_count: section.requirement_count || 0
      };
    }
    
    // Update modal content
    document.getElementById('modal-section-title').textContent = sectionData.section_heading || 'Regulation Text';
    document.getElementById('modal-burden-score').textContent = (sectionData.regulatory_burden_score || 0).toFixed(1);
    document.getElementById('modal-word-count').textContent = (sectionData.word_count || 0).toLocaleString();
    document.getElementById('modal-prohibitions').textContent = sectionData.prohibition_count || 0;
    document.getElementById('modal-requirements').textContent = sectionData.requirement_count || 0;
    
    // Format and display section text
    const content = document.getElementById('modal-section-content');
    if (sectionData.section_text && sectionData.section_text !== 'Full section text not available in basic mode') {
      content.innerHTML = `
        <div class="prose prose-sm max-w-none dark:prose-invert">
          <h4 class="text-lg font-semibold mb-3">${sectionData.section_heading || 'Section Text'}</h4>
          <div class="whitespace-pre-wrap text-sm leading-relaxed bg-muted/30 p-4 rounded-lg">
            ${sectionData.section_text}
          </div>
        </div>
        
        ${sectionData.enforcement_terms > 0 || sectionData.temporal_references > 0 || sectionData.dollar_mentions > 0 ? `
          <div class="mt-6 p-4 bg-yellow-50 dark:bg-yellow-950 rounded-lg">
            <h5 class="font-semibold mb-2 text-yellow-800 dark:text-yellow-200">Key Indicators</h5>
            <div class="space-y-1 text-sm">
              ${sectionData.enforcement_terms > 0 ? `<div>⚖️ ${sectionData.enforcement_terms} enforcement terms</div>` : ''}
              ${sectionData.temporal_references > 0 ? `<div>⏰ ${sectionData.temporal_references} time references</div>` : ''}
              ${sectionData.dollar_mentions > 0 ? `<div>💰 ${sectionData.dollar_mentions} cost references</div>` : ''}
            </div>
          </div>
        ` : ''}
        
        <div class="mt-6 flex justify-center">
          <button 
            onclick="askAIAboutSection('${sectionData.section_citation}', '${title}', '${part}', event)"
            class="inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 bg-purple-600 text-white hover:bg-purple-700 h-10 px-6 py-2 shadow-lg"
          >
            <i data-lucide="brain-circuit" class="h-4 w-4 mr-2"></i>
            Ask AI
          </button>
        </div>
      `;
    } else {
      content.innerHTML = `
        <div class="text-center py-8">
          <i data-lucide="file-text" class="h-12 w-12 text-muted-foreground mx-auto mb-4"></i>
          <h4 class="text-lg font-semibold mb-2">Section Preview</h4>
          <p class="text-muted-foreground mb-4">Full text not available in basic mode</p>
          <div class="bg-muted/30 p-4 rounded-lg text-left">
            <p class="font-semibold">${sectionData.section_heading || 'No heading available'}</p>
            <p class="text-sm text-muted-foreground mt-2">Citation: ${sectionData.section_citation}</p>
          </div>
          
          <div class="mt-6 flex justify-center">
            <button 
              onclick="askAIAboutSection('${sectionData.section_citation}', '${title}', '${part}', event)"
              class="inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 bg-purple-600 text-white hover:bg-purple-700 h-10 px-6 py-2 shadow-lg"
            >
              <i data-lucide="brain-circuit" class="h-4 w-4 mr-2"></i>
              Ask AI
            </button>
          </div>
        </div>
      `;
    }
    
    // Recreate icons
    if (window.lucide) window.lucide.createIcons();
    
  } catch (err) {
    console.error('Error loading section text:', err);
    document.getElementById('modal-section-content').innerHTML = `
      <div class="text-center py-8">
        <i data-lucide="alert-circle" class="h-12 w-12 text-red-500 mx-auto mb-4"></i>
        <h4 class="text-lg font-semibold mb-2 text-red-600">Error Loading Section</h4>
        <p class="text-muted-foreground">${err.message}</p>
      </div>
    `;
    if (window.lucide) window.lucide.createIcons();
  }
}

function closeSectionModal() {
  document.getElementById('section-modal').classList.add('hidden');
}

// Helper function to create burden score display with tooltip
function createBurdenScoreWithTooltip(score, showFullTooltip = false) {
  const tooltip = showFullTooltip 
    ? "Weighted score: Modal terms (×2) + Prohibitions (×5) + Requirements (×3) + Enforcement (×4) + Time refs (×1) + Cost refs (×2). Scale: 0-25 Low, 26-50 Medium, 51+ High complexity."
    : "Composite 0-100 score measuring regulatory complexity. Higher scores indicate more compliance obligations, prohibitions, enforcement terms, and penalties.";
  
  return `<span class="tooltip" data-tooltip="${tooltip}">${score}<i data-lucide="help-circle" class="h-3 w-3 ml-1 inline opacity-60"></i></span>`;
}

// AI Analysis function  
async function askAIAboutSection(sectionCitation, title, part, event) {
  console.log('Asking AI about section:', sectionCitation, 'Title:', title, 'Part:', part);
  
  try {
    // Show loading state
    const button = event ? event.target.closest('button') : document.querySelector(`[onclick*="askAIAboutSection('${sectionCitation}'"]`);
    const originalContent = button.innerHTML;
    button.innerHTML = '<i data-lucide="loader-2" class="h-4 w-4 mr-2 animate-spin"></i>Analyzing...';
    button.disabled = true;
    
    // Recreate loading icon
    if (window.lucide) window.lucide.createIcons();
    
    const response = await fetch(`${API_BASE}/api/ai/analyze-section`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        section_citation: sectionCitation,
        title: title,
        part: part,
        date: BROWSE_DATE
      })
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API Error (${response.status}): ${errorText || 'Unknown error'}`);
    }
    
    const result = await response.json();
    
    // Display AI response
    showAIAnalysis(result.analysis, sectionCitation);
    
    // Restore button
    button.innerHTML = originalContent;
    button.disabled = false;
    if (window.lucide) window.lucide.createIcons();
    
  } catch (err) {
    console.error('Error getting AI analysis:', err);
    
    // Show error message
    const errorDiv = document.createElement('div');
    errorDiv.className = 'mt-4 p-4 bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 rounded-lg';
    errorDiv.innerHTML = `
      <div class="flex items-center">
        <i data-lucide="alert-circle" class="h-5 w-5 text-red-600 mr-2"></i>
        <p class="text-red-800 dark:text-red-200 text-sm">
          <strong>AI Analysis Failed:</strong> ${err.message}
        </p>
      </div>
    `;
    
    // Insert error after the button  
    const button = event ? event.target.closest('button') : document.querySelector(`[onclick*="askAIAboutSection('${sectionCitation}'"]`);
    button.parentNode.insertBefore(errorDiv, button.nextSibling);
    
    // Restore button
    button.innerHTML = originalContent;
    button.disabled = false;
    if (window.lucide) window.lucide.createIcons();
    
    // Remove error after 10 seconds
    setTimeout(() => {
      if (errorDiv.parentNode) {
        errorDiv.parentNode.removeChild(errorDiv);
      }
    }, 10000);
  }
}

// Display AI Analysis in a new section
function showAIAnalysis(analysis, sectionCitation) {
  // Create or update AI analysis section
  let aiSection = document.getElementById('ai-analysis-section');
  
  if (!aiSection) {
    aiSection = document.createElement('div');
    aiSection.id = 'ai-analysis-section';
    aiSection.className = 'mt-6 p-6 bg-gradient-to-r from-purple-50 to-blue-50 dark:from-purple-950 dark:to-blue-950 rounded-lg border border-purple-200 dark:border-purple-800';
    
    // Insert after the modal content
    const modalContent = document.getElementById('modal-section-content');
    modalContent.appendChild(aiSection);
  }
  
  aiSection.innerHTML = `
    <div class="flex items-center mb-4">
      <i data-lucide="brain-circuit" class="h-6 w-6 text-purple-600 mr-2"></i>
      <h5 class="text-lg font-semibold text-purple-900 dark:text-purple-100">AI Analysis for ${sectionCitation}</h5>
    </div>
    
    <div class="prose prose-sm max-w-none dark:prose-invert text-gray-800 dark:text-gray-200">
      <div class="whitespace-pre-wrap leading-relaxed">${analysis}</div>
    </div>
    
    <div class="mt-4 pt-4 border-t border-purple-200 dark:border-purple-700">
      <p class="text-xs text-purple-600 dark:text-purple-400 flex items-center">
        <i data-lucide="sparkles" class="h-3 w-3 mr-1"></i>
        Generated by Gemini 2.5 Flash
      </p>
    </div>
  `;
  
  // Recreate icons
  if (window.lucide) window.lucide.createIcons();
  
  // Scroll to the analysis
  aiSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// Add fallback functions for basic browsing
async function loadPartsLegacy(titleNum) {
  console.log(`Loading parts for title ${titleNum} using legacy method`);
  alert(`Title ${titleNum} parts would be loaded here. This is a fallback when enhanced browsing fails.`);
}

// ========================== AI CHAT FUNCTIONALITY ==========================

let conversationHistory = [];
let isAiConnected = false;

// Check AI service status
async function checkAiStatus() {
  const statusEl = document.getElementById('ai-status');
  console.log('Checking AI status at:', `${AI_API_BASE}/health`);
  
  try {
    const response = await fetch(`${AI_API_BASE}/health`);
    console.log('AI health response:', response.status, response.ok);
    
    if (response.ok) {
      isAiConnected = true;
      statusEl.innerHTML = `
        <div class="h-2 w-2 bg-green-500 rounded-full"></div>
        <span>AI Ready</span>
      `;
      statusEl.className = 'flex items-center gap-2 px-3 py-1.5 text-sm bg-green-100 text-green-800 rounded-full dark:bg-green-900 dark:text-green-200';
      console.log('AI status set to ready');
    } else {
      throw new Error('AI service unavailable');
    }
  } catch (err) {
    console.error('AI status check failed:', err);
    isAiConnected = false;
    statusEl.innerHTML = `
      <div class="h-2 w-2 bg-red-500 rounded-full"></div>
      <span>AI Offline</span>
    `;
    statusEl.className = 'flex items-center gap-2 px-3 py-1.5 text-sm bg-red-100 text-red-800 rounded-full dark:bg-red-900 dark:text-red-200';
  }
}

// Handle chat input keydown
function handleChatKeyDown(event) {
  const textarea = event.target;
  
  // Update character count
  const charCount = document.getElementById('char-count');
  charCount.textContent = `${textarea.value.length}/500`;
  
  // Handle Enter key
  if (event.key === 'Enter') {
    if (event.shiftKey) {
      // Allow new line with Shift+Enter
      return;
    } else {
      // Send message with Enter
      event.preventDefault();
      sendMessage();
    }
  }
}

// Add message to chat
function addMessage(content, isUser = false, sources = []) {
  const messagesContainer = document.getElementById('chat-messages');
  
  const messageDiv = document.createElement('div');
  messageDiv.className = 'flex items-start gap-3';
  
  const avatarClass = isUser 
    ? 'h-8 w-8 bg-muted rounded-full flex items-center justify-center'
    : 'h-8 w-8 bg-primary rounded-full flex items-center justify-center';
  
  const avatarIcon = isUser 
    ? 'user'
    : 'bot';
  
  const iconColor = isUser 
    ? 'text-muted-foreground'
    : 'text-primary-foreground';
  
  messageDiv.innerHTML = `
    <div class="${avatarClass}">
      <i data-lucide="${avatarIcon}" class="h-4 w-4 ${iconColor}"></i>
    </div>
    <div class="flex-1">
      <div class="${isUser ? 'bg-primary text-primary-foreground' : 'bg-muted/50'} rounded-lg p-4">
        <div class="text-sm whitespace-pre-wrap">${content}</div>
      </div>
      ${sources.length > 0 ? `
        <div class="mt-2 text-xs">
          <details class="cursor-pointer">
            <summary class="text-muted-foreground hover:text-foreground">Sources (${sources.length})</summary>
            <div class="mt-2 space-y-2">
              ${sources.map(source => `
                <div class="bg-muted/30 rounded p-2">
                  <div class="font-semibold">${source.citation}</div>
                  <div class="text-muted-foreground">${source.agency} • Burden: ${source.burden_score}/100</div>
                  <div class="text-xs mt-1">${source.summary}</div>
                </div>
              `).join('')}
            </div>
          </details>
        </div>
      ` : ''}
    </div>
  `;
  
  messagesContainer.appendChild(messageDiv);
  
  // Recreate icons
  if (window.lucide) window.lucide.createIcons();
  
  // Scroll to bottom
  messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

// Add typing indicator
function addTypingIndicator() {
  const messagesContainer = document.getElementById('chat-messages');
  
  const typingDiv = document.createElement('div');
  typingDiv.id = 'typing-indicator';
  typingDiv.className = 'flex items-start gap-3';
  typingDiv.innerHTML = `
    <div class="h-8 w-8 bg-primary rounded-full flex items-center justify-center">
      <i data-lucide="bot" class="h-4 w-4 text-primary-foreground"></i>
    </div>
    <div class="flex-1">
      <div class="bg-muted/50 rounded-lg p-4">
        <div class="flex items-center space-x-1">
          <div class="flex space-x-1">
            <div class="h-2 w-2 bg-muted-foreground rounded-full animate-bounce"></div>
            <div class="h-2 w-2 bg-muted-foreground rounded-full animate-bounce" style="animation-delay: 0.1s"></div>
            <div class="h-2 w-2 bg-muted-foreground rounded-full animate-bounce" style="animation-delay: 0.2s"></div>
          </div>
          <span class="text-xs text-muted-foreground ml-2">AI is thinking...</span>
        </div>
      </div>
    </div>
  `;
  
  messagesContainer.appendChild(typingDiv);
  if (window.lucide) window.lucide.createIcons();
  messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

// Remove typing indicator
function removeTypingIndicator() {
  const indicator = document.getElementById('typing-indicator');
  if (indicator) {
    indicator.remove();
  }
}

// Send message to AI
async function sendMessage() {
  const input = document.getElementById('chat-input');
  const message = input.value.trim();
  
  if (!message) return;
  
  if (!isAiConnected) {
    addMessage('AI service is not available. Please check the connection and try again.');
    return;
  }
  
  // Add user message
  addMessage(message, true);
  
  // Clear input
  input.value = '';
  document.getElementById('char-count').textContent = '0/500';
  
  // Disable send button
  const sendButton = document.getElementById('send-button');
  sendButton.disabled = true;
  
  // Add typing indicator
  addTypingIndicator();
  
  try {
    const response = await fetch(`${AI_API_BASE}/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message: message,
        conversation_history: conversationHistory,
        date: BROWSE_DATE,
        max_context_sections: 5
      })
    });
    
    if (!response.ok) {
      throw new Error(`AI service error: ${response.status}`);
    }
    
    const result = await response.json();
    
    // Remove typing indicator
    removeTypingIndicator();
    
    // Add AI response
    addMessage(result.response, false, result.sources);
    
    // Update conversation history
    conversationHistory.push({
      user: message,
      assistant: result.response
    });
    
    // Keep only last 10 exchanges to manage context size
    if (conversationHistory.length > 10) {
      conversationHistory = conversationHistory.slice(-10);
    }
    
    // Show sources if available
    if (result.sources && result.sources.length > 0) {
      showSources(result.sources);
    }
    
  } catch (err) {
    console.error('Chat error:', err);
    removeTypingIndicator();
    addMessage(`Sorry, I encountered an error: ${err.message}. Please try again.`);
  } finally {
    // Re-enable send button
    sendButton.disabled = false;
  }
}

// Show sources panel
function showSources(sources) {
  const panel = document.getElementById('sources-panel');
  const content = document.getElementById('sources-content');
  
  content.innerHTML = sources.map(source => `
    <div class="border rounded-lg p-4">
      <div class="flex items-center justify-between mb-2">
        <h5 class="font-semibold">${source.citation}</h5>
        <div class="flex items-center gap-2">
          <span class="px-2 py-1 text-xs rounded-full ${
            source.burden_score >= 60 ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200' :
            source.burden_score >= 40 ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200' :
            'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
          }">
            ${source.burden_score}/100
          </span>
        </div>
      </div>
      <p class="text-sm text-muted-foreground mb-2">${source.agency} • ${source.heading}</p>
      <p class="text-sm">${source.summary}</p>
      <button onclick="showSectionText('${source.citation}', ${source.title}, '${source.part}')" 
              class="text-xs text-primary hover:text-primary/80 mt-2">
        View Full Text →
      </button>
    </div>
  `).join('');
  
  panel.classList.remove('hidden');
  
  // Recreate icons
  if (window.lucide) window.lucide.createIcons();
}

// Clear chat
function clearChat() {
  const messagesContainer = document.getElementById('chat-messages');
  
  // Clear all messages except the welcome message
  const welcomeMessage = messagesContainer.firstElementChild;
  messagesContainer.innerHTML = '';
  messagesContainer.appendChild(welcomeMessage);
  
  // Clear conversation history
  conversationHistory = [];
  
  // Hide sources panel
  document.getElementById('sources-panel').classList.add('hidden');
  
  // Recreate icons
  if (window.lucide) window.lucide.createIcons();
}
