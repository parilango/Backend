from flask import Flask, jsonify, request
import os
import pymysql
from pymysql.err import OperationalError
import logging
from flask_cors import CORS
from pymysql.cursors import DictCursor

from datetime import datetime, date, timezone
from email.utils import format_datetime

# IMPORTANT: tests expect "application" variable name
application = Flask(__name__)
CORS(application)
logging.basicConfig(level=logging.INFO)


# Endpoint: Health Check
@application.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200


# Endpoint: Data Insertion
@application.route('/events', methods=['POST'])
def create_event():
    try:
        payload = request.get_json()
        required_fields = ["title", "date"]
        if not payload or not all(field in payload for field in required_fields):
            return jsonify({"error": "Missing required fields: 'title' and 'date'"}), 400

        insert_data_into_db(payload)
        return jsonify({"message": "Event created successfully"}), 201

    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during event creation")
        return jsonify({"error": "During event creation", "detail": str(e)}), 500


# Endpoint: Data Retrieval
@application.route('/data', methods=['GET'])
def get_data():
    try:
        data = fetch_data_from_db()
        return jsonify({"data": data}), 200

    except NotImplementedError as nie:
        return jsonify({"error": str(nie)}), 501
    except Exception as e:
        logging.exception("Error occurred during data retrieval")
        return jsonify({"error": "During data retrieval", "detail": str(e)}), 500


def get_db_connection():
    """
    Requires EB environment variables:
      DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
    """
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        msg = f"Missing environment variables: {', '.join(missing)}"
        logging.error(msg)
        raise EnvironmentError(msg)

    try:
        return pymysql.connect(
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"),
            db=os.environ.get("DB_NAME"),
            cursorclass=DictCursor,     # <-- makes fetchall() return dicts
            autocommit=True
        )
    except OperationalError as e:
        raise ConnectionError(f"Failed to connect to the database: {e}")


def create_db_table():
    """
    Creates table if missing.
    """
    try:
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        title VARCHAR(255) NOT NULL,
                        description TEXT,
                        image_url VARCHAR(255),
                        date DATE NOT NULL,
                        location VARCHAR(255)
                    )
                """)
        logging.info("Events table created or already exists")
    except Exception as e:
        logging.exception("Failed to create or verify the events table")
        raise RuntimeError(f"Table creation failed: {str(e)}")


def _to_http_gmt(d):
    """
    Convert a MySQL DATE/datetime to: 'Mon, 01 Feb 2026 00:00:00 GMT'
    """
    if isinstance(d, date) and not isinstance(d, datetime):
        dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        return format_datetime(dt, usegmt=True)
    if isinstance(d, datetime):
        return format_datetime(d.astimezone(timezone.utc), usegmt=True)
    return d


def insert_data_into_db(payload):
    """
    Insert title, description, image_url, date, location into events.
    """
    create_db_table()

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO events (title, description, image_url, date, location)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    payload.get("title"),
                    payload.get("description"),
                    payload.get("image_url"),
                    payload.get("date"),      # 'YYYY-MM-DD'
                    payload.get("location"),
                )
            )
        logging.info("Inserted event successfully")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_data_from_db():
    """
    Fetch all rows ordered by date ascending.
    Return list of event dicts with date formatted as GMT string.
    """
    create_db_table()

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, title, description, image_url, date, location
                FROM events
                ORDER BY date ASC
            """)
            rows = cursor.fetchall() or []

        for r in rows:
            r["date"] = _to_http_gmt(r.get("date"))

        return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    application.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
