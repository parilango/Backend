from datetime import datetime, date, timezone
from email.utils import format_datetime

def insert_data_into_db(payload):
    """
    Insert a new event row into the events table.
    Expected payload keys:
      title (required)
      date (required, 'YYYY-MM-DD')
      description (optional)
      image_url (optional)
      location (optional)
    """
    create_db_table()

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            insert_sql = """
                INSERT INTO events (title, description, image_url, date, location)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(
                insert_sql,
                (
                    payload.get("title"),
                    payload.get("description"),
                    payload.get("image_url"),
                    payload.get("date"),      # stored as DATE in MySQL
                    payload.get("location"),
                )
            )
        connection.commit()
        logging.info("Inserted event into DB successfully")
    except Exception as e:
        logging.exception("Failed to insert event into DB")
        raise RuntimeError(f"Insert failed: {str(e)}")
    finally:
        try:
            connection.close()
        except Exception:
            pass


def fetch_data_from_db():
    """
    Fetch all rows from the events table ordered by date ascending.
    Returns a list of dict-like rows.

    The autograder FAQ mentions expected date format like:
      'Mon, 01 Feb 2026 00:00:00 GMT'
    So we convert DATE values into that HTTP-date format.
    """
    create_db_table()

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            select_sql = """
                SELECT id, title, description, image_url, date, location
                FROM events
                ORDER BY date ASC
            """
            cursor.execute(select_sql)
            rows = cursor.fetchall()

        # Convert rows to list of dicts (PyMySQL default cursor returns tuples unless configured)
        # If your cursor returns tuples, we map using cursor.description
        if rows and not isinstance(rows[0], dict):
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in rows]
        elif not rows:
            rows = []

        # Convert date to 'Mon, 01 Feb 2026 00:00:00 GMT'
        for r in rows:
            d = r.get("date")
            if isinstance(d, date) and not isinstance(d, datetime):
                dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                r["date"] = format_datetime(dt, usegmt=True)
            elif isinstance(d, datetime):
                r["date"] = format_datetime(d.astimezone(timezone.utc), usegmt=True)

        return rows

    except Exception as e:
        logging.exception("Failed to fetch events from DB")
        raise RuntimeError(f"Fetch failed: {str(e)}")
    finally:
        try:
            connection.close()
        except Exception:
            pass
