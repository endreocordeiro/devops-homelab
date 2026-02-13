import os
import time
import psycopg2
from flask import Flask, jsonify, request

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("POSTGRES_DB", "appdb")
DB_USER = os.getenv("POSTGRES_USER", "devops")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "devops123")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def wait_for_db():
    while True:
        try:
            conn = get_connection()
            conn.close()
            print("Database connection successful!")
            break
        except Exception:
            print("Waiting for database...")
            time.sleep(2)


def create_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Table ensured.")


wait_for_db()
create_table()


@app.route("/health")
def health():
    try:
        conn = get_connection()
        conn.close()
        return jsonify(status="ok", database="connected")
    except:
        return jsonify(status="error", database="disconnected"), 500


@app.route("/messages", methods=["POST"])
def create_message():
    data = request.get_json()
    content = data.get("content")

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO messages (content) VALUES (%s) RETURNING id;", (content,))
    message_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    return jsonify(id=message_id, content=content)


@app.route("/messages", methods=["GET"])
def list_messages():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, content, created_at FROM messages;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    messages = [
        {"id": row[0], "content": row[1], "created_at": row[2]}
        for row in rows
    ]

    return jsonify(messages)


@app.route("/")
def home():
    return jsonify(message="DevOps Homelab API running")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)