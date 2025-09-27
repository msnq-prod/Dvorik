import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture()
def app_modules(tmp_path, monkeypatch):
    cfg = tmp_path / "config.json"
    cfg.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CONFIG_PATH", str(cfg))
    db_path = tmp_path / "merge.sqlite3"
    monkeypatch.setenv("DB_PATH", str(db_path))
    repo_root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(repo_root))
    for mod in [m for m in list(sys.modules) if m == "app" or m.startswith("app.")]:
        sys.modules.pop(mod, None)
    db_module = importlib.import_module("app.db")
    merge_module = importlib.import_module("app.services.product_merge")
    imports_module = importlib.import_module("app.services.imports")
    constants_module = importlib.import_module("app.constants")
    db_module.init_db()
    return {
        "db": db_module,
        "merge": merge_module,
        "imports": imports_module,
        "constants": constants_module,
    }


def test_apply_merge_creates_alias_and_can_undo(app_modules):
    const = app_modules["constants"]
    db = app_modules["db"]
    merge = app_modules["merge"]
    conn = db.db()
    try:
        with conn:
            cur = conn.execute(
                "INSERT INTO product(article, name, brand_country, local_name) VALUES (?,?,?,?)",
                ("SKU-A", "Маршмеллоу A", "RU", "A"),
            )
            pid_a = cur.lastrowid
            cur = conn.execute(
                "INSERT INTO product(article, name, brand_country, local_name) VALUES (?,?,?,?)",
                ("SKU-B", "Маршмеллоу B", "BY", "B"),
            )
            pid_b = cur.lastrowid
            conn.execute(
                "INSERT INTO stock(product_id, location_code, qty_pack, name, local_name) VALUES (?,?,?,?,?)",
                (pid_a, const.HUB_LOCATION_CODE, 2, "Маршмеллоу A", "A"),
            )
            conn.execute(
                "INSERT INTO stock(product_id, location_code, qty_pack, name, local_name) VALUES (?,?,?,?,?)",
                (pid_b, "SKL-1", 3, "Маршмеллоу B", "B"),
            )
        result = merge.apply_merge(
            conn,
            pid_a,
            pid_b,
            field_modes={
                "article": "b",
                "name": "merge",
                "brand_country": "a",
                "local_name": "merge",
                "photo": "a",
            },
            stock_mode="merge",
        )
        assert result["ok"] is True
        log_id = result["log_id"]

        row = conn.execute("SELECT article, name, brand_country, local_name FROM product WHERE id=?", (pid_a,)).fetchone()
        assert row["article"] == "SKU-B"
        assert row["brand_country"] == "RU"
        assert "Маршмеллоу A" in row["name"] and "Маршмеллоу B" in row["name"]

        alias_rows = conn.execute(
            "SELECT alias_article, product_id FROM product_article_alias ORDER BY alias_article",
        ).fetchall()
        assert {(r["alias_article"], r["product_id"]) for r in alias_rows} == {
            ("SKU-A", pid_a),
            ("SKU-B", pid_a),
        }

        archived_row = conn.execute("SELECT archived, article FROM product WHERE id=?", (pid_b,)).fetchone()
        assert archived_row["archived"] == 1
        assert archived_row["article"] == "SKU-B"

        stock_rows = conn.execute(
            "SELECT location_code, qty_pack FROM stock WHERE product_id=? ORDER BY location_code",
            (pid_a,),
        ).fetchall()
        assert {(r["location_code"], float(r["qty_pack"])) for r in stock_rows} == {
            (const.HUB_LOCATION_CODE, 2.0),
            ("SKL-1", 3.0),
        }

        rule_row = conn.execute(
            "SELECT result_id, active FROM product_merge_rule WHERE merge_log_id=?",
            (log_id,),
        ).fetchone()
        assert rule_row and rule_row["result_id"] == pid_a and rule_row["active"] == 1

        undo = merge.undo_merge(conn, log_id)
        assert undo["ok"] is True
        restored = conn.execute(
            "SELECT article, archived FROM product WHERE id IN (?,?) ORDER BY id",
            (pid_a, pid_b),
        ).fetchall()
        assert restored[0]["article"] == "SKU-A" and restored[0]["archived"] == 0
        assert restored[1]["article"] == "SKU-B" and restored[1]["archived"] == 0
        remaining_aliases = conn.execute("SELECT COUNT(*) AS c FROM product_article_alias").fetchone()["c"]
        assert remaining_aliases == 0
        rule_after = conn.execute(
            "SELECT active FROM product_merge_rule WHERE merge_log_id=?",
            (log_id,),
        ).fetchone()
        assert rule_after and rule_after["active"] == 0
    finally:
        conn.close()


def test_import_uses_aliases(app_modules):
    const = app_modules["constants"]
    db = app_modules["db"]
    merge = app_modules["merge"]
    imports = app_modules["imports"]
    conn = db.db()
    try:
        with conn:
            cur = conn.execute(
                "INSERT INTO product(article, name, brand_country, local_name) VALUES (?,?,?,?)",
                ("CANON-1", "Карамель", "RU", "Карамель"),
            )
            pid = cur.lastrowid
            conn.execute(
                "INSERT INTO stock(product_id, location_code, qty_pack, name, local_name) VALUES (?,?,?,?,?)",
                (pid, const.HUB_LOCATION_CODE, 1, "Карамель", "Карамель"),
            )
            conn.execute(
                "INSERT INTO product_article_alias(product_id, alias_article) VALUES (?,?)",
                (pid, "ALIAS-1"),
            )

        stats = imports._import_article_rows([("ALIAS-1", "Карамель", 5.0)], err_prefix="Row", start_index=1)
        assert stats["imported"] == 1
        total_qty = conn.execute(
            "SELECT SUM(qty_pack) AS total FROM stock WHERE product_id=?",
            (pid,),
        ).fetchone()["total"]
        assert pytest.approx(total_qty, rel=1e-5) == 6.0

        norm = merge.normalize_name("Сливочная карамель")
        with conn:
            conn.execute(
                "INSERT INTO product_name_alias(product_id, alias_name, normalized_name) VALUES (?,?,?)",
                (pid, "Сливочная карамель", norm),
            )
        stats2 = imports._import_article_rows([("NEW-ALIAS2", "Сливочная карамель", 2.0)], err_prefix="Row", start_index=1)
        assert stats2["imported"] == 1
        total_qty = conn.execute(
            "SELECT SUM(qty_pack) AS total FROM stock WHERE product_id=?",
            (pid,),
        ).fetchone()["total"]
        assert pytest.approx(total_qty, rel=1e-5) == 8.0
    finally:
        conn.close()


def test_merge_reassigns_existing_aliases(app_modules):
    const = app_modules["constants"]
    db = app_modules["db"]
    merge = app_modules["merge"]
    imports = app_modules["imports"]
    article_a = "CANON-A1"
    article_b = "CANON-B1"
    article_c = "CANON-C1"
    conn = db.db()
    try:
        with conn:
            cur = conn.execute(
                "INSERT INTO product(article, name, brand_country, local_name) VALUES (?,?,?,?)",
                (article_a, "Карточка A", "RU", "A"),
            )
            pid_a = int(cur.lastrowid)
            cur = conn.execute(
                "INSERT INTO product(article, name, brand_country, local_name) VALUES (?,?,?,?)",
                (article_b, "Карточка B", "RU", "B"),
            )
            pid_b = int(cur.lastrowid)
            cur = conn.execute(
                "INSERT INTO product(article, name, brand_country, local_name) VALUES (?,?,?,?)",
                (article_c, "Карточка C", "RU", "C"),
            )
            pid_c = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO stock(product_id, location_code, qty_pack, name, local_name) VALUES (?,?,?,?,?)",
                (pid_a, const.HUB_LOCATION_CODE, 1.0, "Карточка A", "A"),
            )
            conn.execute(
                "INSERT INTO stock(product_id, location_code, qty_pack, name, local_name) VALUES (?,?,?,?,?)",
                (pid_b, const.HUB_LOCATION_CODE, 2.0, "Карточка B", "B"),
            )
            conn.execute(
                "INSERT INTO stock(product_id, location_code, qty_pack, name, local_name) VALUES (?,?,?,?,?)",
                (pid_c, const.HUB_LOCATION_CODE, 3.0, "Карточка C", "C"),
            )

        first_merge = merge.apply_merge(
            conn,
            pid_b,
            pid_c,
            field_modes={
                "article": "a",
                "name": "merge",
                "brand_country": "a",
                "local_name": "merge",
                "photo": "a",
            },
            stock_mode="merge",
        )
        assert first_merge["ok"] is True

        alias_before = conn.execute(
            "SELECT product_id FROM product_article_alias WHERE alias_article=?",
            (article_c,),
        ).fetchone()
        assert alias_before and alias_before["product_id"] == pid_b

        second_merge = merge.apply_merge(
            conn,
            pid_a,
            pid_b,
            field_modes={
                "article": "a",
                "name": "merge",
                "brand_country": "a",
                "local_name": "merge",
                "photo": "a",
            },
            stock_mode="merge",
        )
        assert second_merge["ok"] is True
        log_id = second_merge["log_id"]

        alias_after = conn.execute(
            "SELECT product_id FROM product_article_alias WHERE alias_article=?",
            (article_c,),
        ).fetchone()
        assert alias_after and alias_after["product_id"] == pid_a
        norm_c = merge.normalize_name("Карточка C")
        name_alias_after = conn.execute(
            "SELECT product_id FROM product_name_alias WHERE normalized_name=?",
            (norm_c,),
        ).fetchone()
        assert name_alias_after and name_alias_after["product_id"] == pid_a

        stats = imports._import_article_rows(
            [(article_c, "Карточка C", 4.0)],
            err_prefix="Row",
            start_index=1,
        )
        assert stats["imported"] == 1
        total_a = conn.execute(
            "SELECT SUM(qty_pack) AS total FROM stock WHERE product_id=?",
            (pid_a,),
        ).fetchone()["total"]
        assert pytest.approx(total_a or 0.0, rel=1e-5) == 10.0

        undo = merge.undo_merge(conn, log_id)
        assert undo["ok"] is True

        alias_restored = conn.execute(
            "SELECT product_id FROM product_article_alias WHERE alias_article=?",
            (article_c,),
        ).fetchone()
        assert alias_restored and alias_restored["product_id"] == pid_b
        name_alias_restored = conn.execute(
            "SELECT product_id FROM product_name_alias WHERE normalized_name=?",
            (norm_c,),
        ).fetchone()
        assert name_alias_restored and name_alias_restored["product_id"] == pid_b

        total_a_after_undo = conn.execute(
            "SELECT SUM(qty_pack) AS total FROM stock WHERE product_id=?",
            (pid_a,),
        ).fetchone()["total"]
        assert pytest.approx(total_a_after_undo or 0.0, rel=1e-5) == 1.0
        total_b_after_undo = conn.execute(
            "SELECT SUM(qty_pack) AS total FROM stock WHERE product_id=?",
            (pid_b,),
        ).fetchone()["total"]
        assert pytest.approx(total_b_after_undo or 0.0, rel=1e-5) == 5.0
    finally:
        conn.close()
