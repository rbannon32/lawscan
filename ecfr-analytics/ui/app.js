// Set to wherever the FastAPI service is running:
const API_BASE = (location.port === '8080') ? 'http://localhost:8000' : '';

async function jfetch(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

function renderRows(tbody, rows, cols) {
  tbody.innerHTML = '';
  for (const r of rows) {
    const tr = document.createElement('tr');
    for (const c of cols) {
      const td = document.createElement('td');
      td.textContent = r[c] ?? '';
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

async function loadWordcount() {
  try {
    const d = document.getElementById('date_wc').value;
    const rows = await jfetch(`${API_BASE}/api/agency/wordcount?date=${encodeURIComponent(d)}`);
    renderRows(document.querySelector('#tbl_wc tbody'), rows, ['agency_name', 'total_words']);
  } catch (err) {
    console.error('Error loading wordcount:', err);
    alert('Error loading data: ' + err.message);
  }
}

async function loadChecksums() {
  try {
    const d = document.getElementById('date_ck').value;
    const rows = await jfetch(`${API_BASE}/api/agency/checksum?date=${encodeURIComponent(d)}`);
    renderRows(document.querySelector('#tbl_ck tbody'), rows, ['agency_name', 'agency_hash']);
  } catch (err) {
    console.error('Error loading checksums:', err);
    alert('Error loading data: ' + err.message);
  }
}

async function loadDiff() {
  try {
    const f = document.getElementById('from').value;
    const t = document.getElementById('to').value;
    const rows = await jfetch(`${API_BASE}/api/changes?from=${encodeURIComponent(f)}&to=${encodeURIComponent(t)}`);
    renderRows(document.querySelector('#tbl_diff tbody'), rows, ['section_citation', 'change_type']);
  } catch (err) {
    console.error('Error loading diff:', err);
    alert('Error loading data: ' + err.message);
  }
}

async function loadPart() {
  try {
    const title = document.getElementById('title').value;
    const part = document.getElementById('part').value;
    const d = document.getElementById('date_part').value;
    const rows = await jfetch(`${API_BASE}/api/part?title=${encodeURIComponent(title)}&part=${encodeURIComponent(part)}&date=${encodeURIComponent(d)}`);
    const tbody = document.querySelector('#tbl_part tbody');
    tbody.innerHTML = '';
    rows.forEach((r, idx) => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${idx+1}</td><td class="mono">${r.section_citation}</td><td>${r.section_heading || ''}</td><td>${r.word_count}</td>`;
      tbody.appendChild(tr);
    });
  } catch (err) {
    console.error('Error loading part:', err);
    alert('Error loading data: ' + err.message);
  }
}
