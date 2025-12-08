from flask import (
    Flask, render_template, request, jsonify,
    abort, session
)
import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime

# ============================================================
# KONFIGURASI FLASK
# ============================================================
app = Flask(__name__, template_folder="Template", static_folder="Static")

# SECRET KEY untuk session login (ganti dengan yang lebih aman di production)
app.secret_key = "gaw-secret-key-yang-wajib-diganti"


# ============================================================
# FUNGSI KONEKSI DATABASE
# ============================================================
def get_db():
    """
    PRIORITAS:
    1) Kalau ENV DATABASE_URL ada -> pakai itu (Supabase / server lain).
    2) Kalau tidak ada -> pakai PostgreSQL lokal.

    Setelah connect, kita SET search_path ke metadata_poshujan,public
    supaya tabel tanpa schema (users, pos_hujan, dll) mengarah ke schema itu.
    """
    db_url = os.getenv("DATABASE_URL")

    if db_url:
        # Koneksi via connection string (Supabase)
        conn = psycopg2.connect(db_url)
    else:
        # Fallback: koneksi ke PostgreSQL lokal
        conn = psycopg2.connect(
            host="localhost",
            database="db_curah_hujan",
            user="postgres",
            password="123456",
            port=5432,
        )

    # Set search_path di setiap koneksi
    cur = conn.cursor()
    cur.execute("SET search_path TO metadata_poshujan, public;")
    cur.close()

    return conn


# ============================================================
# HELPER: CEK LOGIN UNTUK API (UPLOAD)
# ============================================================
def ensure_logged_in_api():
    if "user_id" not in session:
        abort(403)


# ============================================================
# HALAMAN UTAMA (PETA)
# ============================================================
@app.route("/")
def index():
    return render_template("index.html")


# ============================================================
# HALAMAN UPLOAD (FORM HTML)
# ============================================================
@app.route("/upload", methods=["GET"])
def upload():
    return render_template("upload.html")


# ============================================================
# HALAMAN LIHAT DAFTAR POS
# ============================================================
@app.route("/lihat", methods=["GET"])
def lihat():
    return render_template("lihat.html")


# ============================================================
# LOGIN BACKEND (API)
# ============================================================
@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()

    if not username or not password:
        return jsonify({
            "status": "error",
            "message": "Username dan password wajib diisi."
        }), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, username, password
            FROM users
            WHERE username = %s
        """, (username,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        return jsonify({
            "status": "error",
            "message": "Username atau password salah."
        }), 401

    user_id, db_user, db_pass = row

    if password != db_pass:
        return jsonify({
            "status": "error",
            "message": "Username atau password salah."
        }), 401

    # Simpan ke session
    session["user_id"] = user_id
    session["username"] = db_user

    return jsonify({
        "status": "success",
        "message": "Login berhasil.",
        "username": db_user
    })


@app.route("/api/session", methods=["GET"])
def api_session():
    if "user_id" in session:
        return jsonify({
            "logged_in": True,
            "username": session.get("username")
        })
    else:
        return jsonify({"logged_in": False})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({
        "status": "success",
        "message": "Logout berhasil."
    })


# ============================================================
# HALAMAN DETAIL POS HUJAN
# ============================================================
@app.route("/pos/<int:id_poshujan>")
def detail_pos(id_poshujan):
    conn = get_db()
    cur = conn.cursor()

    # METADATA POS
    cur.execute("""
        SELECT
            p.id_poshujan,
            p.kode_pos,
            p.nama_pos,
            p.balai,
            p.kecamatan,
            p.lintang_dd,
            p.bujur_dd,
            p.elevasi_m,
            prov.nama_provinsi,
            kab.nama_kabupaten
        FROM pos_hujan p
        LEFT JOIN provinsi  prov ON p.id_provinsi  = prov.id_provinsi
        LEFT JOIN kabupaten kab  ON p.id_kabupaten = kab.id_kabupaten
        WHERE p.id_poshujan = %s
    """, (id_poshujan,))
    meta_row = cur.fetchone()

    if not meta_row:
        cur.close()
        conn.close()
        abort(404, description="Pos hujan tidak ditemukan")

    meta = {
        "id_poshujan": meta_row[0],
        "kode_pos":   meta_row[1],
        "nama_pos":   meta_row[2],
        "balai":      meta_row[3],
        "kecamatan":  meta_row[4],
        "lintang":    meta_row[5],
        "bujur":      meta_row[6],
        "elevasi":    meta_row[7],
        "provinsi":   meta_row[8],
        "kabupaten":  meta_row[9],
    }

    # DATA CH HARIAN
    cur.execute("""
        SELECT tanggal, ch_mm
        FROM curah_hujan
        WHERE id_poshujan = %s
          AND ch_mm IS NOT NULL
        ORDER BY tanggal
    """, (id_poshujan,))
    rows_harian = cur.fetchall()

    harian_labels = [r[0].strftime("%Y-%m-%d") for r in rows_harian]
    harian_values = [float(r[1]) for r in rows_harian if r[1] is not None]

    # DATA CH BULANAN (AGREGASI)
    cur.execute("""
        SELECT date_trunc('month', tanggal)::date AS bulan,
               SUM(ch_mm) AS total_ch
        FROM curah_hujan
        WHERE id_poshujan = %s
          AND ch_mm IS NOT NULL
        GROUP BY bulan
        ORDER BY bulan
    """, (id_poshujan,))
    rows_bulanan = cur.fetchall()

    bulanan_labels = [r[0].strftime("%Y-%m") for r in rows_bulanan]
    bulanan_values = [float(r[1]) for r in rows_bulanan if r[1] is not None]

    cur.close()
    conn.close()

    return render_template(
        "detail_pos.html",
        meta=meta,
        rows_harian=rows_harian,
        harian_labels=harian_labels,
        harian_values=harian_values,
        bulanan_labels=bulanan_labels,
        bulanan_values=bulanan_values
    )


# ============================================================
# API POS HUJAN UNTUK TABEL & PETA
# ============================================================
@app.route("/api/pos_hujan")
def api_pos_hujan():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        WITH latest AS (
            SELECT
                id_poshujan,
                MAX(tanggal) AS tanggal_terkini
            FROM curah_hujan
            WHERE ch_mm IS NOT NULL
            GROUP BY id_poshujan
        ),
        latest_ch AS (
            SELECT c.id_poshujan, c.tanggal, c.ch_mm
            FROM curah_hujan c
            JOIN latest l
              ON c.id_poshujan = l.id_poshujan
             AND c.tanggal = l.tanggal_terkini
        )
        SELECT 
            p.id_poshujan,
            p.kode_pos,
            p.nama_pos,
            p.lintang_dd,
            p.bujur_dd,
            k.nama_kabupaten,
            p.kecamatan,
            lc.tanggal AS tanggal_terkini,
            lc.ch_mm   AS ch_terkini
        FROM pos_hujan p
        LEFT JOIN kabupaten k ON p.id_kabupaten = k.id_kabupaten
        LEFT JOIN latest_ch lc ON lc.id_poshujan = p.id_poshujan
        ORDER BY k.nama_kabupaten NULLS LAST, p.nama_pos;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    hasil = []
    for r in rows:
        tanggal_terkini = r[7].strftime("%Y-%m-%d") if r[7] is not None else None
        ch_terkini = float(r[8]) if r[8] is not None else None

        hasil.append({
            "id_poshujan":       r[0],
            "kode_pos":          r[1],
            "nama":              r[2],
            "lat":               r[3],
            "lng":               r[4],
            "kabupaten":         r[5],
            "kecamatan":         r[6],
            "tanggal_terkini":   tanggal_terkini,
            "ch_terkini":        ch_terkini,
        })

    return jsonify(hasil)


# ============================================================
# API JSON CURAH HUJAN PER POS
# ============================================================
@app.route("/api/curah_hujan")
def api_curah_hujan():
    nama_pos = request.args.get("nama_pos", "").strip()
    mode = request.args.get("mode", "harian").lower()

    if not nama_pos:
        return jsonify({"status": "error", "message": "nama_pos wajib ada"}), 400

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id_poshujan
        FROM pos_hujan
        WHERE LOWER(nama_pos) = LOWER(%s)
    """, (nama_pos,))
    res = cur.fetchone()
    if not res:
        cur.close()
        conn.close()
        return jsonify({"status": "error", "message": "Pos tidak ditemukan"}), 404

    id_pos = res[0]

    if mode == "bulanan":
        cur.execute("""
            SELECT date_trunc('month', tanggal)::date AS bulan,
                   SUM(ch_mm) AS total_ch
            FROM curah_hujan
            WHERE id_poshujan = %s
              AND ch_mm IS NOT NULL
            GROUP BY bulan
            ORDER BY bulan
        """, (id_pos,))
        rows = cur.fetchall()
        data = [{"bulan": r[0].strftime("%Y-%m"), "ch": float(r[1])} for r in rows]
        cur.close()
        conn.close()
        return jsonify({
            "status": "success",
            "mode": "bulanan",
            "nama_pos": nama_pos,
            "data": data
        })

    cur.execute("""
        SELECT tanggal, ch_mm
        FROM curah_hujan
        WHERE id_poshujan = %s
          AND ch_mm IS NOT NULL
        ORDER BY tanggal
    """, (id_pos,))
    rows = cur.fetchall()
    data = [{"tanggal": r[0].strftime("%Y-%m-%d"), "ch": float(r[1])} for r in rows]

    cur.close()
    conn.close()
    return jsonify({
        "status": "success",
        "mode": "harian",
        "nama_pos": nama_pos,
        "data": data
    })


# ============================================================
# UPLOAD METADATA POS HUJAN (EXCEL / CSV)
# ============================================================
@app.route("/upload_metadata", methods=["POST"])
def upload_metadata():
    ensure_logged_in_api()

    if "file" not in request.files:
        return jsonify({"status": "error", "message": "File tidak ditemukan di request!"})

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"status": "error", "message": "Tidak ada file yang dipilih!"})

    filename = file.filename.lower()

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Gagal membaca file (Excel/CSV): {e}"})

    required_cols = [
        "Pos Hujan", "ID", "Balai", "Provinsi",
        "Kabupaten", "Kecamatan", "Lintang", "Bujur", "Elevasi"
    ]
    for col in required_cols:
        if col not in df.columns:
            return jsonify({
                "status": "error",
                "message": f"Kolom wajib '{col}' tidak ditemukan di file!"
            })

    df = df.fillna("")

    conn = get_db()
    cur = conn.cursor()

    try:
        for _, row in df.iterrows():
            nama_prov = str(row["Provinsi"]).strip()
            nama_kab  = str(row["Kabupaten"]).strip()

            # PROVINSI
            if nama_prov != "":
                cur.execute("""
                    INSERT INTO provinsi (nama_provinsi)
                    VALUES (%s)
                    ON CONFLICT (nama_provinsi) DO NOTHING;
                """, (nama_prov,))

                cur.execute("""
                    SELECT id_provinsi
                    FROM provinsi
                    WHERE nama_provinsi = %s
                """, (nama_prov,))
                res = cur.fetchone()
                id_prov = res[0] if res else None
            else:
                id_prov = None

            # KABUPATEN
            if nama_kab != "" and id_prov is not None:
                cur.execute("""
                    INSERT INTO kabupaten (id_provinsi, nama_kabupaten)
                    VALUES (%s, %s)
                    ON CONFLICT (id_provinsi, nama_kabupaten) DO NOTHING;
                """, (id_prov, nama_kab))

                cur.execute("""
                    SELECT id_kabupaten
                    FROM kabupaten
                    WHERE id_provinsi = %s AND nama_kabupaten = %s
                """, (id_prov, nama_kab))
                res = cur.fetchone()
                id_kab = res[0] if res else None
            else:
                id_kab = None

            # POS HUJAN
            kode_pos = str(row["ID"]).strip()
            nama_pos = str(row["Pos Hujan"]).strip()
            balai    = str(row["Balai"]).strip() or None
            kec      = str(row["Kecamatan"]).strip() or None

            try:
                lintang = float(row["Lintang"])
                bujur   = float(row["Bujur"])
            except Exception:
                print("Baris dilewati (Lintang/Bujur tidak valid):", nama_pos)
                continue

            elev = None
            elev_str = str(row["Elevasi"]).strip()
            if elev_str != "":
                try:
                    elev = int(float(elev_str))
                except Exception:
                    elev = None

            cur.execute("""
                INSERT INTO pos_hujan (
                    kode_pos, nama_pos, balai, kecamatan,
                    id_provinsi, id_kabupaten,
                    lintang_dd, bujur_dd, elevasi_m, geom
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                )
                ON CONFLICT (kode_pos)
                DO UPDATE SET
                    nama_pos    = EXCLUDED.nama_pos,
                    balai       = EXCLUDED.balai,
                    kecamatan   = EXCLUDED.kecamatan,
                    id_provinsi = EXCLUDED.id_provinsi,
                    id_kabupaten= EXCLUDED.id_kabupaten,
                    lintang_dd  = EXCLUDED.lintang_dd,
                    bujur_dd    = EXCLUDED.bujur_dd,
                    elevasi_m   = EXCLUDED.elevasi_m,
                    geom        = EXCLUDED.geom;
            """, (
                kode_pos, nama_pos, balai, kec,
                id_prov, id_kab,
                lintang, bujur, elev,
                bujur, lintang
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({"status": "error", "message": f"Error saat simpan metadata: {e}"})

    cur.close()
    conn.close()
    return jsonify({"status": "success", "message": "Metadata pos hujan berhasil disimpan ke database."})


# ============================================================
# UPLOAD DATA CURAH HUJAN (EXCEL / CSV) — versi cepat (batch)
# ============================================================
@app.route("/upload_curah_hujan", methods=["POST"])
def upload_curah_hujan():
    # pastikan login dulu
    ensure_logged_in_api()

    if "file" not in request.files:
        return jsonify({"status": "error", "message": "File tidak ditemukan di request!"})

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"status": "error", "message": "Tidak ada file yang dipilih!"})

    filename = file.filename.lower()

    # --- BACA EXCEL / CSV ----------------------------------------------------
    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Gagal membaca file (Excel/CSV): {e}"})

    # Normalisasi nama kolom
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    col_pos = next((c for c in ["pos_hujan", "pos", "stasiun"] if c in df.columns), None)
    col_tgl = next((c for c in ["tanggal", "tgl", "date"] if c in df.columns), None)
    col_ch  = next((c for c in ["curah_hujan", "ch", "ch_mm", "hujan"] if c in df.columns), None)

    if not col_pos or not col_tgl or not col_ch:
        return jsonify({
            "status": "error",
            "message": "Kolom wajib (Pos Hujan, Tanggal, Curah Hujan) tidak ditemukan di file."
        })

    df_use = df[[col_pos, col_tgl, col_ch]].copy()
    df_use.columns = ["pos_hujan", "tanggal", "curah_hujan"]

    df_use["tanggal"] = pd.to_datetime(df_use["tanggal"], errors="coerce").dt.date
    df_use["curah_hujan"] = pd.to_numeric(df_use["curah_hujan"], errors="coerce")

    # Handle kode 8888 / 9999 / negatif
    df_use.loc[df_use["curah_hujan"].isin([8888, 9999]), "curah_hujan"] = pd.NA
    df_use.loc[df_use["curah_hujan"] < 0, "curah_hujan"] = pd.NA

    conn = get_db()
    cur = conn.cursor()

    gagal_pos = []
    rows_insert = []

    try:
        # 1) Ambil semua mapping nama_pos -> id_poshujan sekali saja
        cur.execute("SELECT LOWER(nama_pos), id_poshujan FROM pos_hujan;")
        mapping = {row[0]: row[1] for row in cur.fetchall()}

        # 2) Siapkan data yang akan diinsert (di-memory dulu)
        for _, row in df_use.iterrows():
            if pd.isna(row["tanggal"]) or pd.isna(row["curah_hujan"]):
                continue

            nama_pos_asli = str(row["pos_hujan"]).strip()
            nama_pos = nama_pos_asli.lower()
            tanggal  = row["tanggal"]
            ch       = float(row["curah_hujan"])

            id_pos = mapping.get(nama_pos)
            if not id_pos:
                gagal_pos.append(nama_pos_asli)
                continue

            rows_insert.append(
                (id_pos, tanggal, ch, "Upload Excel/CSV")
            )

        # 3) Batch insert ke curah_hujan (ON CONFLICT DO UPDATE)
        if rows_insert:
            execute_values(cur, """
                INSERT INTO curah_hujan (id_poshujan, tanggal, ch_mm, sumber_data)
                VALUES %s
                ON CONFLICT (id_poshujan, tanggal)
                DO UPDATE SET
                    ch_mm       = EXCLUDED.ch_mm,
                    sumber_data = EXCLUDED.sumber_data,
                    created_at  = NOW();
            """, rows_insert, page_size=500)

        conn.commit()

    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({"status": "error", "message": f"Error saat simpan curah hujan: {e}"})

    cur.close()
    conn.close()

    if gagal_pos:
        gagal_unik = sorted(set(gagal_pos))
        return jsonify({
            "status": "success",
            "message": "Sebagian besar data curah hujan tersimpan. Namun beberapa Pos Hujan tidak ditemukan di metadata.",
            "pos_tidak_ditemukan": gagal_unik
        })

    return jsonify({"status": "success", "message": "Semua data curah hujan berhasil disimpan."})


# ============================================================
# DEBUG: CEK DB & VERSI POSTGRES
# ============================================================
@app.route("/debug_db")
def debug_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT current_database(), version();")
    db_name, ver = cur.fetchone()
    cur.close()
    conn.close()
    return f"DB: {db_name}<br>Versi: {ver}"


# ============================================================
# RUN SERVER
# ============================================================
if __name__ == "__main__":
    app.run(debug=True)
