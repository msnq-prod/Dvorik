"""Lightweight Flask admin UI for the local SQLite DB.

Run with:
  python -m admin_ui
or
  python admin_ui/server.py
"""

import os
from .server import create_app  # re-export factory

def main():
    app = create_app()
    host = os.getenv("ADMIN_HOST", "127.0.0.1")
    try:
        port = int(os.getenv("ADMIN_PORT", "8000"))
    except Exception:
        port = 8000
    debug = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes")
    app.run(host=host, port=port, debug=debug)

if __name__ == "__main__":
    main()
