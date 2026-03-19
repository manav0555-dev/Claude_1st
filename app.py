import os
import sqlite3
import hashlib
import secrets
import random
import string
from datetime import datetime
from functools import wraps
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, jsonify, g
)
import google_sheets

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hvac_tickets.db")


# ── Database helpers ────────────────────────────────────────────────────────

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def generate_ticket_id():
    """Generate a unique ticket ID like HVAC-A3X7K2."""
    chars = string.ascii_uppercase + string.digits
    suffix = ''.join(random.choices(chars, k=6))
    return f"HVAC-{suffix}"


def get_complaint_for_sheets(db, ticket_id):
    """Fetch a complaint with joined fields for Google Sheets sync."""
    return db.execute("""
        SELECT c.*, u.full_name as technician_name, js.name as site_name
        FROM complaints c
        LEFT JOIN users u ON c.technician_id = u.id
        LEFT JOIN job_sites js ON c.job_site_id = js.id
        WHERE c.ticket_id = ?
    """, (ticket_id,)).fetchone()


def init_db():
    db = sqlite3.connect(DATABASE)
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA)

    # Migrate: add ticket_id column if missing (existing databases)
    cols = [r[1] for r in db.execute("PRAGMA table_info(complaints)").fetchall()]
    if "ticket_id" not in cols:
        db.execute("ALTER TABLE complaints ADD COLUMN ticket_id TEXT")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ticket_id ON complaints(ticket_id)")
    if "customer_email" not in cols:
        db.execute("ALTER TABLE complaints ADD COLUMN customer_email TEXT")
    # Migrate: add site_type column to job_sites if missing
    site_cols = [r[1] for r in db.execute("PRAGMA table_info(job_sites)").fetchall()]
    if "site_type" not in site_cols:
        db.execute("ALTER TABLE job_sites ADD COLUMN site_type TEXT NOT NULL DEFAULT 'AMC'")
    # Back-fill ticket IDs for any existing complaints without one
    rows = db.execute("SELECT id FROM complaints WHERE ticket_id IS NULL").fetchall()
    for row in rows:
        tid = generate_ticket_id()
        db.execute("UPDATE complaints SET ticket_id = ? WHERE id = ?", (tid, row[0]))
    if rows:
        db.commit()

    # Seed default admin if no users exist
    row = db.execute("SELECT COUNT(*) FROM users").fetchone()
    if row[0] == 0:
        pw_hash = hashlib.sha256("admin123".encode()).hexdigest()
        db.execute(
            "INSERT INTO users (username, password_hash, role, full_name) VALUES (?,?,?,?)",
            ("admin", pw_hash, "admin", "System Admin"),
        )
        db.commit()

    # Seed technicians if none exist
    tech_count = db.execute("SELECT COUNT(*) FROM users WHERE role='technician'").fetchone()[0]
    if tech_count == 0:
        technicians = [
            ("nkumar", "tech123", "technician", "Narendra Kumar"),
            ("shahrukh", "tech123", "technician", "Shahrukh"),
            ("sonu", "tech123", "technician", "Sonu"),
            ("rahul", "tech123", "technician", "Rahul"),
            ("amjad", "tech123", "technician", "Amjad"),
        ]
        for uname, pw, role, name in technicians:
            try:
                db.execute(
                    "INSERT INTO users (username, password_hash, role, full_name) VALUES (?,?,?,?)",
                    (uname, hashlib.sha256(pw.encode()).hexdigest(), role, name),
                )
            except sqlite3.IntegrityError:
                pass
        db.commit()

    # Seed job sites if none exist
    site_count = db.execute("SELECT COUNT(*) FROM job_sites").fetchone()[0]
    if site_count == 0:
        installation_sites = [
            "Nobel Jewellers Kamla Nagar",
            "AIHP - Mandi Goan",
            "RML Hospital",
            "Kalyan - Palwal",
            "Sunrydge",
        ]
        amc_sites = [
            "Sunrise Sports - Delhi",
            "Sunrise Sports - Noida",
            "RINL",
            "Allied Agency",
            "Raghav Bindal",
            "Delhi Sikh Gurudwara",
            "Furniturewalla",
            "ITL Public School - Dwarka",
            "ITL Public School - Candy Flow",
            "The Indian School",
            "SSP Pvt Ltd",
            "World Wide Fund",
            "Link & Time",
            "Lokyaman Multi Purpose",
            "ICICI Bank",
            "Kalyan - Kamla Nagar",
            "Kalyan - Rajouri Garden",
            "Kalyan - Janak Puri",
            "Kalyan - South Ext",
            "Kalyan - NSP Pitampura",
            "Kalyan - Kohat Enclave",
            "Kalyan - Nirman Vihar",
            "Kalyan - Karol Bagh",
            "Kalyan - Shahdra",
            "Kalyan - Kailash Colony",
            "Kalyan - Paschim Vihar",
            "Kalyan - Ghaziabad",
            "Kalyan - Rohini",
            "Haldiram Central Market",
            "Haldiram - Ring Road",
            "Haldiram - Gwal Pahari",
            "Haldiram - Paras Trade Centre",
            "Tata Starbucks - GK 1 & Pusa Road",
            "Tata Starbucks - Paschim Vihar",
            "Tata Starbucks - Green Park",
            "Tata Starbucks - Model Town",
            "Jai Shree Bindal",
            "Tata Starbucks - GK 1",
            "New Delhi Centre For Sight",
            "Hora Art Centre - Noida",
            "Allied Agency - 2nd Site",
            "Lord Education",
            "Raghav Bindal - 2nd Site",
            "Allied Agency - 3rd Site",
            "SSAR - Darya Ganj",
            "Amazon",
        ]
        for name in installation_sites:
            try:
                db.execute("INSERT INTO job_sites (name, site_type) VALUES (?,?)", (name, "Installation"))
            except sqlite3.IntegrityError:
                pass
        for name in amc_sites:
            try:
                db.execute("INSERT INTO job_sites (name, site_type) VALUES (?,?)", (name, "AMC"))
            except sqlite3.IntegrityError:
                pass
        db.commit()

    db.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('admin', 'technician')),
    full_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    address TEXT,
    site_type TEXT NOT NULL DEFAULT 'AMC' CHECK(site_type IN ('Installation', 'AMC', 'Other')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS complaints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id TEXT UNIQUE,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_phone TEXT,
    customer_email TEXT,
    job_site_id INTEGER,
    technician_id INTEGER,
    priority INTEGER NOT NULL DEFAULT 3 CHECK(priority BETWEEN 1 AND 5),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','in_progress','resolved','closed')),
    category TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    created_by INTEGER,
    FOREIGN KEY (job_site_id) REFERENCES job_sites(id),
    FOREIGN KEY (technician_id) REFERENCES users(id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS complaint_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    complaint_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    note TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (complaint_id) REFERENCES complaints(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
"""


# ── Auth helpers ────────────────────────────────────────────────────────────

def hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in first.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in first.", "warning")
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            flash("Admin access required.", "danger")
            return redirect(url_for("dashboard"))
        return f(*args, **kwargs)
    return decorated


# ── Auth routes ─────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        db = get_db()
        user = db.execute(
            "SELECT * FROM users WHERE username = ? AND password_hash = ?",
            (username, hash_password(password)),
        ).fetchone()
        if user:
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            session["full_name"] = user["full_name"]
            flash(f"Welcome back, {user['full_name']}!", "success")
            return redirect(url_for("dashboard"))
        flash("Invalid username or password.", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "info")
    return redirect(url_for("login"))


@app.route("/register", methods=["GET", "POST"])
@admin_required
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        role = request.form.get("role", "technician")
        full_name = request.form.get("full_name", "").strip()
        if not all([username, password, full_name]):
            flash("All fields are required.", "danger")
        else:
            db = get_db()
            try:
                db.execute(
                    "INSERT INTO users (username, password_hash, role, full_name) VALUES (?,?,?,?)",
                    (username, hash_password(password), role, full_name),
                )
                db.commit()
                flash(f"User '{username}' created.", "success")
                return redirect(url_for("manage_users"))
            except sqlite3.IntegrityError:
                flash("Username already exists.", "danger")
    return render_template("register.html")


# ── Dashboard ───────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    db = get_db()

    # Stats
    total = db.execute("SELECT COUNT(*) FROM complaints").fetchone()[0]
    open_count = db.execute("SELECT COUNT(*) FROM complaints WHERE status='open'").fetchone()[0]
    in_progress = db.execute("SELECT COUNT(*) FROM complaints WHERE status='in_progress'").fetchone()[0]
    resolved = db.execute("SELECT COUNT(*) FROM complaints WHERE status IN ('resolved','closed')").fetchone()[0]

    # Priority breakdown
    priority_data = db.execute(
        "SELECT priority, COUNT(*) as cnt FROM complaints WHERE status NOT IN ('closed') GROUP BY priority ORDER BY priority"
    ).fetchall()

    # Top repeat offenders (technicians with most complaints)
    repeat_technicians = db.execute("""
        SELECT u.full_name, COUNT(c.id) as complaint_count
        FROM complaints c
        JOIN users u ON c.technician_id = u.id
        GROUP BY c.technician_id
        HAVING COUNT(c.id) > 0
        ORDER BY complaint_count DESC
        LIMIT 10
    """).fetchall()

    # Repeat customers
    repeat_customers = db.execute("""
        SELECT customer_name, COUNT(*) as complaint_count
        FROM complaints
        GROUP BY LOWER(customer_name)
        HAVING COUNT(*) > 1
        ORDER BY complaint_count DESC
        LIMIT 10
    """).fetchall()

    # Category breakdown
    category_data = db.execute("""
        SELECT COALESCE(category, 'Uncategorized') as cat, COUNT(*) as cnt
        FROM complaints
        GROUP BY cat
        ORDER BY cnt DESC
    """).fetchall()

    # Recent complaints
    recent = db.execute("""
        SELECT c.*, u.full_name as technician_name, js.name as site_name
        FROM complaints c
        LEFT JOIN users u ON c.technician_id = u.id
        LEFT JOIN job_sites js ON c.job_site_id = js.id
        ORDER BY c.created_at DESC LIMIT 10
    """).fetchall()

    # Job site breakdown
    site_data = db.execute("""
        SELECT COALESCE(js.name, 'Unassigned') as site_name, COUNT(c.id) as cnt
        FROM complaints c
        LEFT JOIN job_sites js ON c.job_site_id = js.id
        GROUP BY site_name
        ORDER BY cnt DESC
    """).fetchall()

    return render_template("dashboard.html",
        total=total, open_count=open_count, in_progress=in_progress,
        resolved=resolved, priority_data=priority_data,
        repeat_technicians=repeat_technicians, repeat_customers=repeat_customers,
        category_data=category_data, recent=recent, site_data=site_data)


# ── Complaints ──────────────────────────────────────────────────────────────

@app.route("/complaints")
@login_required
def complaints_list():
    db = get_db()
    sort = request.args.get("sort", "priority")
    status_filter = request.args.get("status", "")
    tech_filter = request.args.get("technician", "")
    search = request.args.get("search", "").strip()

    query = """
        SELECT c.*, u.full_name as technician_name, js.name as site_name,
            CAST(julianday('now') - julianday(c.created_at) AS INTEGER) as days_open,
            CASE WHEN c.resolved_at IS NOT NULL
                THEN CAST(julianday(c.resolved_at) - julianday(c.created_at) AS INTEGER)
                ELSE NULL END as days_to_close
        FROM complaints c
        LEFT JOIN users u ON c.technician_id = u.id
        LEFT JOIN job_sites js ON c.job_site_id = js.id
        WHERE 1=1
    """
    params = []

    if status_filter:
        query += " AND c.status = ?"
        params.append(status_filter)

    if tech_filter:
        query += " AND c.technician_id = ?"
        params.append(int(tech_filter))

    if search:
        query += " AND (c.title LIKE ? OR c.description LIKE ? OR c.customer_name LIKE ?)"
        params.extend([f"%{search}%"] * 3)

    # Technicians only see their own complaints
    if session.get("role") == "technician":
        query += " AND c.technician_id = ?"
        params.append(session["user_id"])

    if sort == "priority":
        query += " ORDER BY c.priority ASC, c.created_at ASC"
    elif sort == "date_newest":
        query += " ORDER BY c.created_at DESC"
    elif sort == "date_oldest":
        query += " ORDER BY c.created_at ASC"
    elif sort == "status":
        query += " ORDER BY c.status ASC, c.priority ASC"
    else:
        query += " ORDER BY c.priority ASC, c.created_at ASC"

    complaints = db.execute(query, params).fetchall()
    technicians = db.execute("SELECT id, full_name FROM users WHERE role='technician' ORDER BY full_name").fetchall()

    return render_template("complaints.html", complaints=complaints,
        technicians=technicians, sort=sort, status_filter=status_filter,
        tech_filter=tech_filter, search=search)


@app.route("/complaints/new", methods=["GET", "POST"])
@login_required
def new_complaint():
    db = get_db()
    if request.method == "POST":
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        customer_name = request.form.get("customer_name", "").strip()
        customer_phone = request.form.get("customer_phone", "").strip()
        job_site_id = request.form.get("job_site_id") or None
        other_site_name = request.form.get("other_site_name", "").strip()
        if job_site_id == "other" and other_site_name:
            cursor = db.execute("INSERT INTO job_sites (name, site_type) VALUES (?,?)", (other_site_name, "Other"))
            job_site_id = cursor.lastrowid
        elif job_site_id == "other":
            job_site_id = None
        technician_id = request.form.get("technician_id") or None
        priority = int(request.form.get("priority", 3))
        category = request.form.get("category", "").strip() or None

        if not all([title, description, customer_name]):
            flash("Title, description, and customer name are required.", "danger")
        else:
            ticket_id = generate_ticket_id()
            db.execute("""
                INSERT INTO complaints
                (ticket_id, title, description, customer_name, customer_phone, job_site_id,
                 technician_id, priority, category, created_by)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (ticket_id, title, description, customer_name, customer_phone,
                  job_site_id, technician_id, priority, category, session["user_id"]))
            db.commit()
            # Sync to Google Sheets
            complaint_row = get_complaint_for_sheets(db, ticket_id)
            if complaint_row:
                google_sheets.sync_complaint(dict(complaint_row))
            flash("Complaint created successfully.", "success")
            return redirect(url_for("complaints_list"))

    technicians = db.execute("SELECT id, full_name FROM users WHERE role='technician' ORDER BY full_name").fetchall()
    job_sites = db.execute("SELECT id, name, site_type FROM job_sites ORDER BY site_type, name").fetchall()
    return render_template("new_complaint.html", technicians=technicians, job_sites=job_sites)


@app.route("/complaints/<int:complaint_id>")
@login_required
def view_complaint(complaint_id):
    db = get_db()
    complaint = db.execute("""
        SELECT c.*, u.full_name as technician_name, js.name as site_name,
               creator.full_name as created_by_name,
               CAST(julianday('now') - julianday(c.created_at) AS INTEGER) as days_open,
               CASE WHEN c.resolved_at IS NOT NULL
                   THEN CAST(julianday(c.resolved_at) - julianday(c.created_at) AS INTEGER)
                   ELSE NULL END as days_to_close
        FROM complaints c
        LEFT JOIN users u ON c.technician_id = u.id
        LEFT JOIN job_sites js ON c.job_site_id = js.id
        LEFT JOIN users creator ON c.created_by = creator.id
        WHERE c.id = ?
    """, (complaint_id,)).fetchone()

    if not complaint:
        flash("Complaint not found.", "danger")
        return redirect(url_for("complaints_list"))

    notes = db.execute("""
        SELECT n.*, u.full_name as author_name
        FROM complaint_notes n
        JOIN users u ON n.user_id = u.id
        WHERE n.complaint_id = ?
        ORDER BY n.created_at DESC
    """, (complaint_id,)).fetchall()

    technicians = db.execute("SELECT id, full_name FROM users WHERE role='technician' ORDER BY full_name").fetchall()
    job_sites = db.execute("SELECT id, name, site_type FROM job_sites ORDER BY site_type, name").fetchall()

    return render_template("view_complaint.html", complaint=complaint,
        notes=notes, technicians=technicians, job_sites=job_sites)


@app.route("/complaints/<int:complaint_id>/update", methods=["POST"])
@login_required
def update_complaint(complaint_id):
    if session.get("role") != "admin":
        flash("Only admins can update complaints.", "danger")
        return redirect(url_for("view_complaint", complaint_id=complaint_id))
    db = get_db()
    status = request.form.get("status")
    technician_id = request.form.get("technician_id") or None
    priority = request.form.get("priority")

    updates = ["updated_at = CURRENT_TIMESTAMP"]
    params = []

    if status:
        updates.append("status = ?")
        params.append(status)
        if status in ("resolved", "closed"):
            updates.append("resolved_at = CURRENT_TIMESTAMP")

    if technician_id is not None:
        updates.append("technician_id = ?")
        params.append(technician_id if technician_id else None)

    if priority:
        updates.append("priority = ?")
        params.append(int(priority))

    params.append(complaint_id)
    db.execute(f"UPDATE complaints SET {', '.join(updates)} WHERE id = ?", params)
    db.commit()
    # Sync updated complaint to Google Sheets
    row = db.execute("SELECT ticket_id FROM complaints WHERE id = ?", (complaint_id,)).fetchone()
    if row:
        complaint_row = get_complaint_for_sheets(db, row["ticket_id"])
        if complaint_row:
            google_sheets.sync_complaint(dict(complaint_row))
    flash("Complaint updated.", "success")
    return redirect(url_for("view_complaint", complaint_id=complaint_id))


@app.route("/complaints/<int:complaint_id>/note", methods=["POST"])
@login_required
def add_note(complaint_id):
    note = request.form.get("note", "").strip()
    if note:
        db = get_db()
        db.execute(
            "INSERT INTO complaint_notes (complaint_id, user_id, note) VALUES (?,?,?)",
            (complaint_id, session["user_id"], note),
        )
        db.commit()
        flash("Note added.", "success")
    return redirect(url_for("view_complaint", complaint_id=complaint_id))


# ── Job Sites ───────────────────────────────────────────────────────────────

@app.route("/sites")
@admin_required
def manage_sites():
    db = get_db()
    sites = db.execute("""
        SELECT js.*, COUNT(c.id) as complaint_count
        FROM job_sites js
        LEFT JOIN complaints c ON c.job_site_id = js.id
        GROUP BY js.id
        ORDER BY js.name
    """).fetchall()
    return render_template("sites.html", sites=sites)


@app.route("/sites/add", methods=["POST"])
@admin_required
def add_site():
    name = request.form.get("name", "").strip()
    address = request.form.get("address", "").strip()
    site_type = request.form.get("site_type", "AMC").strip()
    if site_type not in ("Installation", "AMC", "Other"):
        site_type = "AMC"
    if name:
        db = get_db()
        try:
            db.execute("INSERT INTO job_sites (name, address, site_type) VALUES (?,?,?)", (name, address, site_type))
            db.commit()
            flash(f"Site '{name}' added.", "success")
        except sqlite3.IntegrityError:
            flash("Site name already exists.", "danger")
    return redirect(url_for("manage_sites"))


# ── Users ───────────────────────────────────────────────────────────────────

@app.route("/users")
@admin_required
def manage_users():
    db = get_db()
    users = db.execute("""
        SELECT u.*, COUNT(c.id) as complaint_count
        FROM users u
        LEFT JOIN complaints c ON c.technician_id = u.id
        GROUP BY u.id
        ORDER BY u.full_name
    """).fetchall()
    return render_template("users.html", users=users)


# ── Insights API (for charts) ──────────────────────────────────────────────

@app.route("/api/insights")
@login_required
def api_insights():
    db = get_db()

    # Monthly trend (last 12 months)
    monthly = db.execute("""
        SELECT strftime('%Y-%m', created_at) as month, COUNT(*) as cnt
        FROM complaints
        GROUP BY month
        ORDER BY month DESC
        LIMIT 12
    """).fetchall()

    # Avg resolution time (in hours)
    avg_resolution = db.execute("""
        SELECT ROUND(AVG(
            (julianday(resolved_at) - julianday(created_at)) * 24
        ), 1) as avg_hours
        FROM complaints
        WHERE resolved_at IS NOT NULL
    """).fetchone()

    return jsonify({
        "monthly_trend": [{"month": r["month"], "count": r["cnt"]} for r in monthly],
        "avg_resolution_hours": avg_resolution["avg_hours"] if avg_resolution["avg_hours"] else 0,
    })


# ── Google Sheets Sync ─────────────────────────────────────────────────────

@app.route("/admin/sync-sheets", methods=["POST"])
@admin_required
def sync_sheets():
    """Full sync of all complaints to Google Sheets."""
    if not google_sheets.is_configured():
        flash("Google Sheets is not configured. Set GOOGLE_SHEETS_CREDENTIALS and GOOGLE_SHEET_ID environment variables.", "danger")
        return redirect(url_for("dashboard"))

    db = get_db()
    complaints = db.execute("""
        SELECT c.*, u.full_name as technician_name, js.name as site_name
        FROM complaints c
        LEFT JOIN users u ON c.technician_id = u.id
        LEFT JOIN job_sites js ON c.job_site_id = js.id
        ORDER BY c.created_at DESC
    """).fetchall()

    success = google_sheets.sync_all_complaints([dict(c) for c in complaints])
    if success:
        flash(f"Successfully synced {len(complaints)} complaints to Google Sheets.", "success")
    else:
        flash("Failed to sync to Google Sheets. Check server logs.", "danger")
    return redirect(url_for("dashboard"))


@app.route("/admin/sheets-status")
@admin_required
def sheets_status():
    """Check if Google Sheets integration is configured."""
    return jsonify({"configured": google_sheets.is_configured()})


# ── Client-facing pages (no login required) ────────────────────────────────

@app.route("/client")
def client_home():
    return render_template("client_home.html")


@app.route("/client/submit", methods=["GET", "POST"])
def client_submit():
    db = get_db()
    if request.method == "POST":
        customer_name = request.form.get("customer_name", "").strip()
        customer_phone = request.form.get("customer_phone", "").strip()
        customer_email = request.form.get("customer_email", "").strip()
        job_site_id = request.form.get("job_site_id") or None
        other_site_name = request.form.get("other_site_name", "").strip()
        if job_site_id == "other" and other_site_name:
            cursor = db.execute("INSERT INTO job_sites (name, site_type) VALUES (?,?)", (other_site_name, "Other"))
            job_site_id = cursor.lastrowid
        elif job_site_id == "other":
            job_site_id = None
        category = request.form.get("category", "").strip() or None
        description = request.form.get("description", "").strip()

        if not all([customer_name, description]):
            flash("Your name and a description of the issue are required.", "danger")
        else:
            ticket_id = generate_ticket_id()
            title = category or "General Complaint"
            db.execute("""
                INSERT INTO complaints
                (ticket_id, title, description, customer_name, customer_phone,
                 customer_email, job_site_id, category, priority, status)
                VALUES (?,?,?,?,?,?,?,?,3,'open')
            """, (ticket_id, title, description, customer_name, customer_phone,
                  customer_email, job_site_id, category))
            db.commit()
            # Sync to Google Sheets
            complaint_row = get_complaint_for_sheets(db, ticket_id)
            if complaint_row:
                google_sheets.sync_complaint(dict(complaint_row))
            return redirect(url_for("client_success", ticket_id=ticket_id))

    job_sites = db.execute("SELECT id, name, site_type FROM job_sites ORDER BY site_type, name").fetchall()
    return render_template("client_submit.html", job_sites=job_sites)


@app.route("/client/success/<ticket_id>")
def client_success(ticket_id):
    return render_template("client_success.html", ticket_id=ticket_id)


@app.route("/client/track", methods=["GET", "POST"])
def client_track():
    complaint = None
    searched = False
    if request.method == "POST" or request.args.get("ticket_id"):
        searched = True
        ticket_id = (request.form.get("ticket_id") or request.args.get("ticket_id", "")).strip().upper()
        if ticket_id:
            db = get_db()
            complaint = db.execute("""
                SELECT c.ticket_id, c.status, c.category, c.description,
                       c.created_at, c.updated_at, c.resolved_at,
                       js.name as site_name, u.full_name as technician_name
                FROM complaints c
                LEFT JOIN job_sites js ON c.job_site_id = js.id
                LEFT JOIN users u ON c.technician_id = u.id
                WHERE c.ticket_id = ?
            """, (ticket_id,)).fetchone()
    return render_template("client_track.html", complaint=complaint, searched=searched)


# ── Init DB on import (needed for gunicorn) ─────────────────────────────────
init_db()

# ── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG", "1") == "1", host="0.0.0.0", port=5000)
