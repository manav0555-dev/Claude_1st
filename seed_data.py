"""Seed the database with sample data for demo/testing."""
import sqlite3
import hashlib
import os
import random
import string

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hvac_tickets.db")

def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def gen_ticket_id():
    chars = string.ascii_uppercase + string.digits
    return f"HVAC-{''.join(random.choices(chars, k=6))}"

def seed():
    db = sqlite3.connect(DATABASE)
    db.execute("PRAGMA foreign_keys = ON")

    # Technicians
    technicians = [
        ("jsmith", "tech123", "technician", "John Smith"),
        ("mwilson", "tech123", "technician", "Mike Wilson"),
        ("ljones", "tech123", "technician", "Lisa Jones"),
        ("rbrown", "tech123", "technician", "Robert Brown"),
        ("akumar", "tech123", "technician", "Anil Kumar"),
    ]
    for uname, pw, role, name in technicians:
        try:
            db.execute(
                "INSERT INTO users (username, password_hash, role, full_name) VALUES (?,?,?,?)",
                (uname, hash_pw(pw), role, name),
            )
        except sqlite3.IntegrityError:
            pass

    # Job sites
    sites = [
        ("Riverside Office Complex", "1200 River Rd, Suite 100"),
        ("Greenfield Mall", "450 Greenfield Blvd"),
        ("Sunset Apartments", "789 Sunset Ave, Bldg A"),
        ("Downtown Medical Center", "55 Main St"),
        ("Lakewood School District", "300 Lakewood Dr"),
    ]
    for name, addr in sites:
        try:
            db.execute("INSERT INTO job_sites (name, address) VALUES (?,?)", (name, addr))
        except sqlite3.IntegrityError:
            pass

    db.commit()

    # Get IDs
    techs = db.execute("SELECT id, username FROM users WHERE role='technician'").fetchall()
    site_rows = db.execute("SELECT id, name FROM job_sites").fetchall()
    admin = db.execute("SELECT id FROM users WHERE role='admin'").fetchone()

    tech_ids = [t[0] for t in techs]
    site_ids = [s[0] for s in site_rows]
    admin_id = admin[0] if admin else 1

    # Complaints
    complaints = [
        ("AC unit not cooling properly", "Temperature in the 3rd floor server room stays at 82F despite thermostat set to 68F. Unit is running but blowing warm air.", "Sarah Johnson", "(555) 101-2001", site_ids[0], tech_ids[0], 1, "Temperature", "open"),
        ("Excessive noise from rooftop unit", "Loud rattling noise coming from RTU-4 on the roof. Tenants on the top floor are complaining.", "Mark Davis", "(555) 101-2002", site_ids[0], tech_ids[1], 2, "Noise/Vibration", "in_progress"),
        ("Water leak near air handler", "Water pooling under AHU-2 in the mechanical room. Appears to be condensate drain issue.", "Tom Chen", "(555) 201-3001", site_ids[1], tech_ids[2], 1, "Leak", "open"),
        ("Thermostat not responding", "Zone 4 thermostat screen is blank. Batteries replaced but still not working.", "Angela White", "(555) 201-3002", site_ids[1], tech_ids[0], 3, "Electrical", "open"),
        ("Poor airflow in unit 205", "Very weak airflow from all vents in apartment 205. Other units seem fine.", "James Miller", "(555) 301-4001", site_ids[2], tech_ids[3], 3, "Airflow", "open"),
        ("Technician was late to appointment", "Scheduled service for 9am, technician arrived at 11:30am with no prior communication.", "Patricia Moore", "(555) 301-4002", site_ids[2], tech_ids[1], 4, "Timeliness", "resolved"),
        ("Heating not working in exam rooms", "Exam rooms 3, 4, and 5 have no heat. Patients are uncomfortable. Urgent fix needed.", "Dr. Williams", "(555) 401-5001", site_ids[3], tech_ids[4], 1, "Temperature", "in_progress"),
        ("Refrigerant smell in hallway", "Strong chemical smell near the HVAC closet on the 2nd floor. Possible refrigerant leak.", "Nancy Taylor", "(555) 401-5002", site_ids[3], tech_ids[2], 2, "Leak", "open"),
        ("Gym HVAC running during off hours", "The gym building HVAC runs all night even though the building is empty after 9pm.", "Principal Anderson", "(555) 501-6001", site_ids[4], tech_ids[3], 4, "Maintenance", "open"),
        ("Incomplete installation of new units", "New split units in classrooms 101-103 were installed but never tested. One is leaking.", "VP Martinez", "(555) 501-6002", site_ids[4], tech_ids[0], 2, "Installation", "open"),
        ("AC blowing warm air again", "Same unit from last month is blowing warm air again. This is the 3rd time.", "Sarah Johnson", "(555) 101-2001", site_ids[0], tech_ids[0], 2, "Temperature", "open"),
        ("Unprofessional behavior on site", "Technician left debris and packaging materials in the hallway after service.", "Tom Chen", "(555) 201-3001", site_ids[1], tech_ids[1], 3, "Professionalism", "open"),
        ("Billing discrepancy", "Invoice shows 6 hours of labor but technician was on-site for 2 hours.", "Angela White", "(555) 201-3002", site_ids[1], tech_ids[4], 5, "Billing", "open"),
        ("Ductwork making popping sounds", "Metal ductwork in the ceiling makes loud popping sounds when HVAC cycles on/off.", "James Miller", "(555) 301-4001", site_ids[2], tech_ids[3], 3, "Noise/Vibration", "open"),
        ("Follow-up repair not scheduled", "Was told a follow-up visit would be scheduled within a week. It has been 3 weeks.", "Dr. Williams", "(555) 401-5001", site_ids[3], tech_ids[4], 2, "Timeliness", "open"),
    ]

    existing = db.execute("SELECT COUNT(*) FROM complaints").fetchone()[0]
    if existing == 0:
        for c in complaints:
            ticket_id = gen_ticket_id()
            db.execute("""
                INSERT INTO complaints
                (ticket_id, title, description, customer_name, customer_phone, job_site_id,
                 technician_id, priority, category, status, created_by)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (ticket_id, *c, admin_id))

        # Add some notes
        db.execute("INSERT INTO complaint_notes (complaint_id, user_id, note) VALUES (?,?,?)",
                   (1, admin_id, "Dispatched John Smith to investigate. Customer says this has been going on for 2 days."))
        db.execute("INSERT INTO complaint_notes (complaint_id, user_id, note) VALUES (?,?,?)",
                   (2, tech_ids[1], "On-site now. Found loose fan belt on RTU-4. Replacing belt and checking bearings."))
        db.execute("INSERT INTO complaint_notes (complaint_id, user_id, note) VALUES (?,?,?)",
                   (7, tech_ids[4], "Checked the heat pump. Defrost board appears faulty. Ordered replacement part."))

    db.commit()
    db.close()
    print("Seed data loaded successfully!")

if __name__ == "__main__":
    seed()
