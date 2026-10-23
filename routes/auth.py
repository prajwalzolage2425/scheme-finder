from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

auth_bp = Blueprint("auth", __name__)

def get_db():
    from flask import current_app
    return current_app.mongo.db

# ─── Register ────────────────────────────────────────────────────────────────
@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if "user_id" in session:
        return redirect(url_for("main_home"))

    if request.method == "POST":
        db = get_db()
        name  = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip().lower()
        phone = request.form.get("phone", "").strip()
        password = request.form.get("password", "")
        confirm  = request.form.get("confirm_password", "")

        if not all([name, email, phone, password]):
            flash("All fields are required.", "error")
            return render_template("login.html", mode="register")

        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("login.html", mode="register")

        if db.users.find_one({"email": email}):
            flash("Email already registered. Please login.", "error")
            return render_template("login.html", mode="register")

        hashed = generate_password_hash(password)
        user_doc = {
            "name": name,
            "email": email,
            "phone": phone,
            "password": hashed,
            "created_at": datetime.utcnow(),
            "profile_complete": False
        }
        result = db.users.insert_one(user_doc)
        session["user_id"] = str(result.inserted_id)
        session["user_name"] = name

        flash("Account created successfully! Let's find your schemes.", "success")
        return redirect(url_for("main_home"))  # ← fixed

    return render_template("login.html", mode="register")


# ─── Login ────────────────────────────────────────────────────────────────────
@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("main_home"))  # ← fixed

    if request.method == "POST":
        db = get_db()
        email    = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        user = db.users.find_one({"email": email})
        if not user or not check_password_hash(user["password"], password):
            flash("Invalid email or password.", "error")
            return render_template("login.html", mode="login")

        session["user_id"] = str(user["_id"])
        session["user_name"] = user["name"]

        # Restore persistent user profile from database into session
        user_profile = user.get("profile", {})
        if user_profile:
            session["user_data"] = user_profile
            session["profile_complete"] = user.get("profile_complete", True)
        else:
            session["profile_complete"] = False

        flash(f"Welcome back, {user['name']}!", "success")
        return redirect(url_for("schemes.results" if user_profile else "main_home"))

    return render_template("login.html", mode="login")


# ─── Profile ──────────────────────────────────────────────────────────────────
@auth_bp.route("/profile", methods=["GET", "POST"])
def profile():
    if "user_id" not in session:
        return redirect(url_for("auth.login"))

    db = get_db()
    user_id = session.get("user_id")
    user = None
    if db is not None and user_id:
        try:
            from bson.objectid import ObjectId
            user = db.users.find_one({"_id": ObjectId(user_id)})
        except Exception:
            pass

    if request.method == "POST":
        name = request.form.get("name", "").strip() or (user.get("name") if user else session.get("user_name", ""))
        phone = request.form.get("phone", "").strip() or (user.get("phone") if user else "")
        user_data = {
            "annual_income": request.form.get("annual_income", 0),
            "age":           request.form.get("age", 25),
            "gender":        request.form.get("gender", "").lower(),
            "caste":         request.form.get("caste", "general").lower(),
            "state":         request.form.get("state", "").lower().replace(" ", "_"),
            "area_type":     request.form.get("area_type", "urban").lower(),
            "occupation":    request.form.get("occupation", "").lower().replace(" ", "_"),
            "bpl_card":      request.form.get("bpl_card", "no").lower(),
            "has_bank_account": request.form.get("has_bank_account", "yes").lower()
        }

        if db is not None and user_id:
            try:
                from bson.objectid import ObjectId
                db.users.update_one(
                    {"_id": ObjectId(user_id)},
                    {"$set": {
                        "name": name,
                        "phone": phone,
                        "profile": user_data,
                        "profile_complete": True,
                        "updated_at": datetime.utcnow()
                    }}
                )
            except Exception as e:
                print(f"Error updating user profile in MongoDB: {e}")

        session["user_name"] = name
        session["user_data"] = user_data
        session["profile_complete"] = True
        flash("Profile updated successfully! Here are your matched schemes.", "success")
        return redirect(url_for("schemes.results"))

    user_profile = (user.get("profile") if user else {}) or session.get("user_data", {})
    return render_template("profile.html", user=user, profile=user_profile)


# ─── Logout ───────────────────────────────────────────────────────────────────
@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logged out successfully.", "success")
    return redirect(url_for("auth.login"))