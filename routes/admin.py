"""
routes/admin.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Admin routes — protected by ADMIN_KEY in .env

Available routes:
  /admin                       → dashboard
  /admin/fetch                 → trigger fetch from browser
  /admin/schemes               → view all schemes in DB
  /admin/add                   → manually add a scheme

Access: http://localhost:5000/admin?key=YOUR_ADMIN_KEY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from flask import Blueprint, request, jsonify, render_template_string, redirect, url_for
import os, json, threading

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

ADMIN_KEY   = os.getenv("ADMIN_KEY", "admin123")
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "schemes_data.json")
LOG_PATH    = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "fetch_log.json")

fetch_status = {"running": False, "last": None, "result": None}


def require_admin(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.args.get("key") or request.form.get("key") or request.headers.get("X-Admin-Key")
        if key != ADMIN_KEY:
            return "<h2>403 — Unauthorized. Add ?key=YOUR_ADMIN_KEY to URL.</h2>", 403
        return f(*args, **kwargs)
    return decorated


def load_schemes():
    from pymongo import MongoClient
    from dotenv import load_dotenv
    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/schemefinder")
    client = MongoClient(mongo_uri)
    db  = client.get_default_database()
    schemes = list(db["schemes"].find({}, {"_id": 0}))
    client.close()
    return schemes


def load_log():
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH) as f:
            return json.load(f)
    return {}


# ── Admin Dashboard ───────────────────────────────────────────────────────────
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>SchemeSaathi Admin</title>
  <style>
    body { font-family: sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; background: #f8f9fc; }
    h1 { color: #0A1628; }
    .card { background: white; border-radius: 12px; padding: 24px; margin: 16px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.07); }
    .stat { display: inline-block; background: #FFF3EE; color: #FF6B35; border-radius: 8px; padding: 8px 16px; margin: 6px; font-weight: 700; font-size: 1.1rem; }
    .btn { display: inline-block; padding: 10px 20px; border-radius: 8px; background: #FF6B35; color: white; text-decoration: none; font-weight: 600; margin: 6px; cursor: pointer; border: none; font-size: 0.9rem; }
    .btn:hover { background: #FF8C5A; }
    .btn-green { background: #27AE60; }
    .btn-navy  { background: #0A1628; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #f0f2f5; font-weight: 600; }
    tr:hover { background: #fafafa; }
    .badge { padding: 2px 8px; border-radius: 100px; font-size: 0.7rem; font-weight: 700; }
    .central { background: #E8F4FD; color: #1A73E8; }
    .state   { background: #E9F7EF; color: #27AE60; }
    pre { background: #f0f2f5; padding: 12px; border-radius: 8px; font-size: 0.8rem; overflow: auto; }
  </style>
</head>
<body>
  <h1>🇮🇳 SchemeSaathi Admin Panel</h1>
  <p style="color:#666">Logged in as Admin &nbsp;·&nbsp; <a href="/">← Back to App</a></p>

  <!-- Stats -->
  <div class="card">
    <h2 style="margin-top:0">📊 Database Stats</h2>
    <span class="stat">{{ total_schemes }} Total Schemes</span>
    <span class="stat">{{ central_count }} Central</span>
    <span class="stat">{{ state_count }} State</span>
    <span class="stat">{{ categories|length }} Categories</span>
  </div>

  <!-- Actions -->
  <div class="card">
    <h2 style="margin-top:0">⚡ Actions</h2>
    <a href="/admin/fetch?key={{ key }}" class="btn">🔄 Fetch from MyScheme API</a>
    <a href="/admin/fetch?key={{ key }}&query=women" class="btn btn-green">👩 Fetch Women Schemes</a>
    <a href="/admin/fetch?key={{ key }}&query=maharashtra&state=maharashtra" class="btn btn-green">📍 Fetch MH Schemes</a>
    <a href="/admin/add?key={{ key }}" class="btn btn-navy">➕ Add Manually</a>
    <p style="color:#999;font-size:0.8rem;margin-top:10px">
      Fetch status: <strong>{{ fetch_status }}</strong>
      {% if last_fetch %} · Last run: {{ last_fetch }}{% endif %}
    </p>
  </div>

  <!-- Last Fetch Log -->
  {% if log %}
  <div class="card">
    <h2 style="margin-top:0">📋 Last Fetch Log</h2>
    <pre>{{ log }}</pre>
  </div>
  {% endif %}

  <!-- Scheme Table -->
  <div class="card">
    <h2 style="margin-top:0">📜 All Schemes ({{ total_schemes }})</h2>
    <table>
      <thead><tr><th>#</th><th>Name</th><th>Category</th><th>Ministry</th><th>State</th><th>Match Rules</th></tr></thead>
      <tbody>
        {% for s in schemes %}
        <tr>
          <td style="color:#999">{{ loop.index }}</td>
          <td><strong>{{ s.name }}</strong></td>
          <td>{{ s.category }}</td>
          <td style="font-size:0.8rem;color:#666">{{ s.ministry[:40] }}</td>
          <td>
            <span class="badge {{ 'state' if s.state != 'central' else 'central' }}">
              {{ s.state|title }}
            </span>
          </td>
          <td style="font-size:0.75rem;color:#666">{{ s.eligibility.keys()|list|join(', ') }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>
</body>
</html>
"""

@admin_bp.route("/")
@require_admin
def dashboard():
    from jinja2 import Template
    schemes  = load_schemes()
    log_data = load_log()
    categories = list(set(s.get("category","") for s in schemes))
    html = Template(DASHBOARD_HTML).render(
        schemes        = schemes,
        total_schemes  = len(schemes),
        central_count  = sum(1 for s in schemes if s.get("state") == "central"),
        state_count    = sum(1 for s in schemes if s.get("state") != "central"),
        categories     = categories,
        key            = ADMIN_KEY,
        fetch_status   = "Running..." if fetch_status["running"] else "Idle",
        last_fetch     = log_data.get("timestamp", ""),
        log            = json.dumps(log_data, indent=2) if log_data else ""
    )
    return html


# ── Trigger Fetch ─────────────────────────────────────────────────────────────
@admin_bp.route("/fetch")
@require_admin
def trigger_fetch():
    if fetch_status["running"]:
        return "<h3>⏳ Fetch already running. Please wait...</h3><a href='/admin?key=" + ADMIN_KEY + "'>← Back</a>"

    query = request.args.get("query", "")
    state = request.args.get("state", "")
    size  = int(request.args.get("size", 100))

    def run_fetch():
        fetch_status["running"] = True
        fetch_status["result"]  = None
        try:
            import sys
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
            from fetch_schemes import run as fetch_run
            fetch_run(query=query, state_filter=state, size=size, preview=False, replace=False)
            fetch_status["result"] = "success"
        except Exception as e:
            fetch_status["result"] = f"error: {str(e)}"
        finally:
            fetch_status["running"] = False

    # Run in background thread so browser doesn't time out
    thread = threading.Thread(target=run_fetch, daemon=True)
    thread.start()

    return f"""
    <h2>🔄 Fetch started in background!</h2>
    <p>Query: <strong>{query or 'all'}</strong> | State: <strong>{state or 'all'}</strong> | Size: <strong>{size}</strong></p>
    <p>The fetch runs in background. Check the admin dashboard in ~30 seconds for results.</p>
    <a href="/admin?key={ADMIN_KEY}">← Back to Dashboard</a>
    <script>setTimeout(() => window.location='/admin?key={ADMIN_KEY}', 5000);</script>
    """


# ── Manual Add ────────────────────────────────────────────────────────────────
ADD_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Add Scheme — Admin</title>
  <style>
    body { font-family: sans-serif; max-width: 700px; margin: 40px auto; padding: 0 20px; background: #f8f9fc; }
    .card { background: white; border-radius: 12px; padding: 28px; box-shadow: 0 2px 8px rgba(0,0,0,0.07); }
    label { display: block; font-size: 0.82rem; font-weight: 700; color: #4A5568; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 5px; margin-top: 16px; }
    input, select, textarea { width: 100%; padding: 10px 14px; border: 2px solid #EEF1F7; border-radius: 8px; font-size: 0.93rem; box-sizing: border-box; font-family: sans-serif; }
    input:focus, textarea:focus, select:focus { border-color: #FF6B35; outline: none; }
    .hint { font-size: 0.75rem; color: #9AA3B2; margin-top: 4px; }
    .btn { padding: 12px 28px; background: #FF6B35; color: white; border: none; border-radius: 8px; font-size: 1rem; font-weight: 700; cursor: pointer; margin-top: 20px; }
    h2 { color: #0A1628; margin-bottom: 0; }
  </style>
</head>
<body>
  <h2>➕ Add New Scheme Manually</h2>
  <p><a href="/admin?key={{ key }}">← Back to Dashboard</a></p>
  <div class="card">
    <form method="POST">
      <input type="hidden" name="key" value="{{ key }}">

      <label>Scheme ID (unique, lowercase_underscore)</label>
      <input name="id" placeholder="e.g. pm_svamitva_yojana" required>

      <label>Full Scheme Name</label>
      <input name="name" placeholder="e.g. PM SVAMITVA Yojana" required>

      <label>Category</label>
      <select name="category">
        <option>Housing</option><option>Health</option><option>Education</option>
        <option>Employment</option><option>Agriculture</option><option>Banking</option>
        <option>Insurance</option><option>Livelihood</option><option>Women & Child</option>
        <option>Social Security</option><option>Energy</option><option>General</option>
      </select>

      <label>Ministry</label>
      <input name="ministry" placeholder="e.g. Ministry of Rural Development">

      <label>Description (1-2 sentences)</label>
      <textarea name="description" rows="3" placeholder="What does this scheme do?"></textarea>

      <label>Benefits</label>
      <textarea name="benefits" rows="2" placeholder="What does the beneficiary get?"></textarea>

      <label>Max Annual Income (₹) — leave blank if no limit</label>
      <input name="max_income" type="number" placeholder="e.g. 200000">

      <label>Documents Required (one per line)</label>
      <textarea name="documents" rows="4" placeholder="Aadhaar Card&#10;Income Certificate&#10;Bank Account Details"></textarea>

      <label>Apply Steps (one per line)</label>
      <textarea name="apply_steps" rows="4" placeholder="Visit the official portal&#10;Fill the application form&#10;Submit documents"></textarea>

      <label>Official Apply Link</label>
      <input name="apply_link" placeholder="https://...">

      <label>State (use 'central' for national schemes)</label>
      <input name="state" value="central" placeholder="central / maharashtra / etc.">
      <div class="hint">Use lowercase — e.g. central, maharashtra, gujarat</div>

      <label>Tags (comma separated)</label>
      <input name="tags" placeholder="health, women, rural">

      <button type="submit" class="btn">💾 Save Scheme</button>
    </form>
  </div>
</body>
</html>
"""

@admin_bp.route("/add", methods=["GET", "POST"])
@require_admin
def add_scheme():
    from jinja2 import Template
    if request.method == "POST":
        schemes = load_schemes()
        existing_ids = {s["id"] for s in schemes}
        new_id = request.form.get("id", "").strip().lower().replace(" ", "_")

        if new_id in existing_ids:
            return f"<h3>❌ ID '{new_id}' already exists. Go back and use a different ID.</h3>"

        max_income = request.form.get("max_income", "").strip()
        eligibility = {}
        if max_income:
            eligibility["max_income"] = int(max_income)

        new_scheme = {
            "id":          new_id,
            "name":        request.form.get("name", "").strip(),
            "category":    request.form.get("category", "General"),
            "ministry":    request.form.get("ministry", "").strip(),
            "description": request.form.get("description", "").strip(),
            "benefits":    request.form.get("benefits", "").strip(),
            "eligibility": eligibility,
            "documents":   [d.strip() for d in request.form.get("documents", "").splitlines() if d.strip()],
            "apply_link":  request.form.get("apply_link", "").strip(),
            "apply_steps": [s.strip() for s in request.form.get("apply_steps", "").splitlines() if s.strip()],
            "state":       request.form.get("state", "central").strip().lower(),
            "tags":        [t.strip() for t in request.form.get("tags", "").split(",") if t.strip()],
            "source":      "manual"
        }

        from pymongo import MongoClient
        from dotenv import load_dotenv
        load_dotenv()
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/schemefinder")
        client = MongoClient(mongo_uri)
        db = client.get_default_database()
        db["schemes"].insert_one(new_scheme)
        client.close()

        return f"<h2>✅ Scheme '{new_scheme['name']}' added!</h2><a href='/admin?key={ADMIN_KEY}'>← Dashboard</a>"

    return Template(ADD_HTML).render(key=ADMIN_KEY)
