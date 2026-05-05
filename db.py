import sqlite3

DB = "database.db"


def get_conn():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute(
        """CREATE TABLE IF NOT EXISTS news (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        content TEXT,
        hindi_title TEXT,
        hindi_content TEXT,
        source TEXT DEFAULT 'Google News',
        category TEXT DEFAULT 'General',
        image_url TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        views INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        tags TEXT DEFAULT '',
        submitted_by TEXT DEFAULT 'admin'
    )"""
    )

    for col, definition in [
        ("views", "INTEGER DEFAULT 0"),
        ("tags", "TEXT DEFAULT ''"),
        ("submitted_by", "TEXT DEFAULT 'admin'"),
        ("top_pick", "INTEGER DEFAULT 0"),
    ]:
        try:
            c.execute(f"ALTER TABLE news ADD COLUMN {col} {definition}")
        except Exception:
            pass

    c.execute(
        """CREATE TABLE IF NOT EXISTS country_views (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        news_id INTEGER,
        country TEXT DEFAULT 'Unknown',
        city TEXT DEFAULT 'Unknown',
        ip TEXT DEFAULT '',
        viewed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS staff (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        role TEXT DEFAULT 'writer',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        news_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        message TEXT NOT NULL,
        status TEXT DEFAULT 'approved',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS epapers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        edition_date TEXT DEFAULT '',
        file_url TEXT NOT NULL,
        cover_image TEXT DEFAULT '',
        uploaded_by TEXT DEFAULT 'admin',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        slug TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS subcategories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER,
        name TEXT NOT NULL,
        slug TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS tags_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        slug TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS shorts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        media_url TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS featured_sections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        news_id INTEGER DEFAULT 0,
        position INTEGER DEFAULT 0,
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS ad_spaces (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        location TEXT DEFAULT '',
        image_url TEXT DEFAULT '',
        target_url TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS users_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        full_name TEXT NOT NULL,
        email TEXT DEFAULT '',
        phone TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS comment_flags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comment_id INTEGER DEFAULT 0,
        reason TEXT NOT NULL,
        status TEXT DEFAULT 'open',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        audience TEXT DEFAULT 'all',
        status TEXT DEFAULT 'draft',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        slug TEXT UNIQUE NOT NULL,
        content TEXT DEFAULT '',
        status TEXT DEFAULT 'published',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS roles_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        permissions TEXT DEFAULT '',
        status TEXT DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS system_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        setting_key TEXT UNIQUE NOT NULL,
        setting_value TEXT DEFAULT '',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )"""
    )

    c.execute(
        "INSERT OR IGNORE INTO staff (username, password, role) VALUES ('editor1','editor123','editor')"
    )
    c.execute(
        "INSERT OR IGNORE INTO staff (username, password, role) VALUES ('writer1','writer123','writer')"
    )
    c.execute(
        "INSERT OR IGNORE INTO roles_master (name, permissions, status) VALUES ('admin','all','active')"
    )
    c.execute(
        "INSERT OR IGNORE INTO roles_master (name, permissions, status) VALUES ('editor','news,comments','active')"
    )
    c.execute(
        "INSERT OR IGNORE INTO roles_master (name, permissions, status) VALUES ('writer','submit_news','active')"
    )
    c.execute(
        "INSERT OR IGNORE INTO pages (title, slug, content, status) VALUES ('About Us','about-us','About page content','published')"
    )
    c.execute(
        "INSERT OR IGNORE INTO pages (title, slug, content, status) VALUES ('Contact Us','contact-us','Contact page content','published')"
    )
    c.execute(
        "INSERT OR IGNORE INTO system_settings (setting_key, setting_value) VALUES ('site_name','India News')"
    )
    c.execute(
        "INSERT OR IGNORE INTO system_settings (setting_key, setting_value) VALUES ('default_language','hi')"
    )
    conn.commit()
    conn.close()


def _rows_to_dicts(rows):
    return [dict(row) for row in rows]


def get_all_news(role=None, username=None):
    conn = get_conn()
    c = conn.cursor()
    if role and role != "admin" and username:
        c.execute(
            """SELECT id,title,content,hindi_title,hindi_content,source,category,
                     image_url,status,views,created_at,tags,submitted_by,top_pick
                     FROM news WHERE submitted_by=? ORDER BY created_at DESC""",
            (username,),
        )
    else:
        c.execute(
            """SELECT id,title,content,hindi_title,hindi_content,source,category,
                     image_url,status,views,created_at,tags,submitted_by,top_pick
                     FROM news ORDER BY created_at DESC"""
        )
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def get_news_by_id(news_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT id,title,content,hindi_title,hindi_content,source,category,
                  image_url,status,views,created_at,tags,submitted_by,top_pick
           FROM news WHERE id=?""",
        (news_id,),
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def get_approved_news():
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT id,title,content,hindi_title,hindi_content,source,category,
                 image_url,status,views,created_at,tags,submitted_by,top_pick
                 FROM news WHERE status='approved' ORDER BY created_at DESC"""
    )
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def insert_news(
    title,
    content,
    hindi_title,
    hindi_content,
    source,
    category,
    image_url="",
    tags="",
    submitted_by="admin",
):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """INSERT INTO news (title,content,hindi_title,hindi_content,source,category,
               image_url,status,views,tags,submitted_by)
               VALUES (?,?,?,?,?,?,?,'pending',0,?,?)""",
        (
            title,
            content,
            hindi_title,
            hindi_content,
            source,
            category,
            image_url,
            tags,
            submitted_by,
        ),
    )
    conn.commit()
    conn.close()


def update_status(news_id, status):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE news SET status=? WHERE id=?", (status, news_id))
    conn.commit()
    conn.close()


def update_news(news_id, title, content, hindi_title, hindi_content, image_url, tags=""):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """UPDATE news SET title=?, content=?, hindi_title=?, hindi_content=?,
               image_url=?, tags=? WHERE id=?""",
        (title, content, hindi_title, hindi_content, image_url, tags, news_id),
    )
    conn.commit()
    conn.close()


def count_top_picks():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) AS count FROM news WHERE top_pick=1")
    row = c.fetchone()
    conn.close()
    return row["count"] if row else 0


def set_top_pick(news_id, enabled):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE news SET top_pick=? WHERE id=?", (1 if enabled else 0, news_id))
    conn.commit()
    conn.close()


def get_top_picks(limit=5):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT id,title,content,hindi_title,hindi_content,source,category,
                  image_url,status,views,created_at,tags,submitted_by,top_pick
           FROM news
           WHERE status='approved' AND top_pick=1
           ORDER BY created_at DESC
           LIMIT ?""",
        (limit,),
    )
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def delete_news(news_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM news WHERE id=?", (news_id,))
    c.execute("DELETE FROM country_views WHERE news_id=?", (news_id,))
    c.execute("DELETE FROM comments WHERE news_id=?", (news_id,))
    conn.commit()
    conn.close()


def increment_views(news_id, country="Unknown", city="Unknown", ip=""):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE news SET views = views + 1 WHERE id=?", (news_id,))
    c.execute(
        "INSERT INTO country_views (news_id, country, city, ip) VALUES (?,?,?,?)",
        (news_id, country, city, ip),
    )
    conn.commit()
    conn.close()


def get_stats(news_items=None):
    if news_items is not None:
        stats = {
            "pending": 0,
            "approved": 0,
            "rejected": 0,
            "total": len(news_items),
            "total_views": sum(int(item.get("views") or 0) for item in news_items),
        }
        for item in news_items:
            status = item.get("status", "pending")
            stats[status] = stats.get(status, 0) + 1
        return stats

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT status, COUNT(*) AS count FROM news GROUP BY status")
    rows = c.fetchall()
    stats = {"pending": 0, "approved": 0, "rejected": 0, "total": 0, "total_views": 0}
    for row in rows:
        stats[row["status"]] = row["count"]
        stats["total"] += row["count"]
    c.execute("SELECT COALESCE(SUM(views),0) AS total_views FROM news")
    stats["total_views"] = c.fetchone()["total_views"]
    conn.close()
    return stats


def get_category_stats(role=None, username=None):
    conn = get_conn()
    c = conn.cursor()
    if role and role != "admin" and username:
        c.execute(
            """SELECT category, COUNT(*) AS count
               FROM news
               WHERE submitted_by=?
               GROUP BY category
               ORDER BY count DESC""",
            (username,),
        )
    else:
        c.execute(
            """SELECT category, COUNT(*) AS count
               FROM news
               WHERE status='approved'
               GROUP BY category
               ORDER BY count DESC"""
        )
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def get_country_stats():
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT country, COUNT(*) as count FROM country_views GROUP BY country ORDER BY count DESC LIMIT 10"
    )
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def get_city_stats():
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT city, COUNT(*) as count
           FROM country_views
           WHERE city != 'Unknown'
           GROUP BY city
           ORDER BY count DESC LIMIT 10"""
    )
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def get_staff(username, password):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT id,username,role,status FROM staff WHERE username=? AND password=?",
        (username, password),
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_staff():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id,username,role,status,created_at FROM staff ORDER BY id")
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def create_staff(username, password, role):
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO staff (username,password,role) VALUES (?,?,?)",
            (username, password, role),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def update_staff(staff_id, username, password, role, status):
    conn = get_conn()
    c = conn.cursor()
    if password:
        c.execute(
            "UPDATE staff SET username=?,password=?,role=?,status=? WHERE id=?",
            (username, password, role, status, staff_id),
        )
    else:
        c.execute(
            "UPDATE staff SET username=?,role=?,status=? WHERE id=?",
            (username, role, status, staff_id),
        )
    conn.commit()
    conn.close()


def delete_staff(staff_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM staff WHERE id=?", (staff_id,))
    conn.commit()
    conn.close()


def add_comment(news_id, name, message, status="approved"):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "INSERT INTO comments (news_id,name,message,status) VALUES (?,?,?,?)",
        (news_id, name, message, status),
    )
    conn.commit()
    conn.close()


def get_comments(news_id=None, status=None):
    conn = get_conn()
    c = conn.cursor()
    query = "SELECT id,news_id,name,message,status,created_at FROM comments"
    params = []
    where = []
    if news_id is not None:
        where.append("news_id=?")
        params.append(news_id)
    if status:
        where.append("status=?")
        params.append(status)
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY created_at DESC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def update_comment_status(comment_id, status):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE comments SET status=? WHERE id=?", (status, comment_id))
    conn.commit()
    conn.close()


def delete_comment(comment_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM comments WHERE id=?", (comment_id,))
    conn.commit()
    conn.close()


def insert_epaper(title, edition_date, file_url, cover_image="", uploaded_by="admin"):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """INSERT INTO epapers (title,edition_date,file_url,cover_image,uploaded_by)
           VALUES (?,?,?,?,?)""",
        (title, edition_date, file_url, cover_image, uploaded_by),
    )
    conn.commit()
    conn.close()


def get_epapers():
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        """SELECT id,title,edition_date,file_url,cover_image,uploaded_by,created_at
           FROM epapers ORDER BY COALESCE(edition_date, created_at) DESC, created_at DESC"""
    )
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def delete_epaper(epaper_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM epapers WHERE id=?", (epaper_id,))
    conn.commit()
    conn.close()


MODULE_SCHEMA = {
    "category": {
        "table": "categories",
        "fields": ["name", "slug"],
        "status_field": "status",
    },
    "subcategory": {
        "table": "subcategories",
        "fields": ["category_id", "name", "slug"],
        "status_field": "status",
    },
    "tag": {
        "table": "tags_master",
        "fields": ["name", "slug"],
        "status_field": "status",
    },
    "short": {
        "table": "shorts",
        "fields": ["title", "media_url"],
        "status_field": "status",
    },
    "featured_section": {
        "table": "featured_sections",
        "fields": ["title", "news_id", "position"],
        "status_field": "status",
    },
    "ad_spaces": {
        "table": "ad_spaces",
        "fields": ["name", "location", "image_url", "target_url"],
        "status_field": "status",
    },
    "user": {
        "table": "users_master",
        "fields": ["full_name", "email", "phone"],
        "status_field": "status",
    },
    "comment_flag": {
        "table": "comment_flags",
        "fields": ["comment_id", "reason"],
        "status_field": "status",
    },
    "send_notification": {
        "table": "notifications",
        "fields": ["title", "message", "audience"],
        "status_field": "status",
    },
    "pages": {
        "table": "pages",
        "fields": ["title", "slug", "content"],
        "status_field": "status",
    },
    "roles": {
        "table": "roles_master",
        "fields": ["name", "permissions"],
        "status_field": "status",
    },
    "system_setting": {
        "table": "system_settings",
        "fields": ["setting_key", "setting_value"],
        "status_field": "",
    },
}


def get_module_config(module):
    return MODULE_SCHEMA.get(module)


def get_module_items(module):
    cfg = get_module_config(module)
    if not cfg:
        return []
    conn = get_conn()
    c = conn.cursor()
    c.execute(f"SELECT * FROM {cfg['table']} ORDER BY id DESC")
    rows = c.fetchall()
    conn.close()
    return _rows_to_dicts(rows)


def create_module_item(module, data):
    cfg = get_module_config(module)
    if not cfg:
        return False
    fields = list(cfg["fields"])
    if cfg["status_field"]:
        fields.append(cfg["status_field"])
    values = []
    for field in fields:
        if field == cfg["status_field"]:
            values.append(data.get(field, "active"))
        else:
            values.append(data.get(field, ""))
    placeholders = ",".join("?" for _ in fields)
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute(
            f"INSERT INTO {cfg['table']} ({','.join(fields)}) VALUES ({placeholders})",
            tuple(values),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def delete_module_item(module, item_id):
    cfg = get_module_config(module)
    if not cfg:
        return False
    conn = get_conn()
    c = conn.cursor()
    c.execute(f"DELETE FROM {cfg['table']} WHERE id=?", (item_id,))
    conn.commit()
    conn.close()
    return True


def toggle_module_item_status(module, item_id):
    cfg = get_module_config(module)
    if not cfg or not cfg["status_field"]:
        return False
    conn = get_conn()
    c = conn.cursor()
    c.execute(f"SELECT {cfg['status_field']} AS status FROM {cfg['table']} WHERE id=?", (item_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return False
    current = (row["status"] or "").lower()
    new_status = "inactive" if current in ("active", "open", "published", "draft") else "active"
    c.execute(f"UPDATE {cfg['table']} SET {cfg['status_field']}=? WHERE id=?", (new_status, item_id))
    conn.commit()
    conn.close()
    return True
