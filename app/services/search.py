from __future__ import annotations

import math
import re
import sqlite3
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from app.services import product_merge as merge_svc
from app.services import products_display as display_svc


def _normalize_match_name(name: Optional[str], phrases: Sequence[str]) -> str:
    cleaned = display_svc.strip_display_exceptions(name, phrases)
    base = cleaned if cleaned else (name or "").strip()
    if not base:
        return ""
    normalized = merge_svc.normalize_name(base)
    normalized = normalized.replace("ё", "е").replace("Ё", "е")
    normalized = re.sub(r"[^0-9a-zа-я]+", " ", normalized)
    normalized = re.sub(r"\s{2,}", " ", normalized).strip()
    return normalized


def _tokenize_match_name(text: str) -> List[str]:
    if not text:
        return []
    tokens = re.split(r"[^0-9a-zа-я]+", text)
    result: List[str] = []
    seen: Set[str] = set()
    for token in tokens:
        token = token.strip()
        if len(token) < 2:
            continue
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _similarity_ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    ratio = SequenceMatcher(None, left, right).ratio()
    if " " in left or " " in right:
        compact_left = left.replace(" ", "")
        compact_right = right.replace(" ", "")
        compact_ratio = SequenceMatcher(None, compact_left, compact_right).ratio()
        return max(ratio, compact_ratio)
    return ratio


def cards_search(
    conn: sqlite3.Connection,
    q: str,
    limit: int,
    *,
    without_local: bool = False,
    hide_empty: bool = False,
    only_empty: bool = False,
    location_codes: Optional[Sequence[str]] = None,
) -> List[sqlite3.Row]:
    try:
        limit_val = int(limit)
    except (TypeError, ValueError):
        limit_val = 60
    limit = max(limit_val, 1)

    without_local = bool(without_local)
    hide_empty = bool(hide_empty)
    only_empty = bool(only_empty)
    normalized_codes: List[str] = []
    if location_codes:
        seen_codes: Set[str] = set()
        for raw_code in location_codes:
            code = (raw_code or "").strip()
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            normalized_codes.append(code)

    agg_params: List[Any] = list(normalized_codes)
    if normalized_codes:
        placeholders = ",".join(["?"] * len(normalized_codes))
        filtered_case = (
            f"SUM(CASE WHEN s.location_code IN ({placeholders}) THEN s.qty_pack ELSE 0 END)"
        )
    else:
        filtered_case = "SUM(s.qty_pack)"

    totals_subquery = f"""
        SELECT s.product_id,
               SUM(s.qty_pack) AS total_all,
               {filtered_case} AS total_filtered
        FROM stock s
        GROUP BY s.product_id
    """
    EPS = 0.000001
    card_select = (
        "p.id, p.article, p.name, p.local_name, p.photo_path, p.brand_country, "
        "p.manufacturer_id, m.name AS manufacturer_name, m.country AS manufacturer_country"
    )

    def apply_common_filters(conditions: List[str], params: List[Any]) -> None:
        if without_local:
            conditions.append("(NULLIF(TRIM(p.local_name), '') IS NULL)")
        if only_empty:
            conditions.append("ABS(COALESCE(t.total_filtered,0)) <= ?")
            params.append(EPS)
        elif hide_empty:
            conditions.append("COALESCE(t.total_filtered,0) > ?")
            params.append(EPS)

    def execute_query(query: str, params: Sequence[Any]) -> List[sqlite3.Row]:
        query_params: List[Any] = list(agg_params)
        query_params.extend(params)
        query_params.append(limit)
        return conn.execute(query, query_params).fetchall()

    rows: List[sqlite3.Row] = []
    try:
        if q:
            has_fts = (
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='product_fts'"
                ).fetchone()
                is not None
            )
            if has_fts:
                match = (q.replace(" ", "* ") + "*").strip()
                conditions = ["product_fts MATCH ?", "p.archived=0"]
                params_list: List[Any] = [match]
                apply_common_filters(conditions, params_list)
                where_sql = " AND ".join(conditions) if conditions else "1=1"
                query = f"""
                    SELECT {card_select}
                    FROM product_fts f
                    JOIN product p ON p.id=f.rowid
                    LEFT JOIN ({totals_subquery}) t ON t.product_id=p.id
                    LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
                    WHERE {where_sql}
                    ORDER BY (COALESCE(t.total_filtered,0) > 0) DESC,
                             (COALESCE(t.total_all,0) > 0) DESC,
                             p.id DESC
                    LIMIT ?
                """
                rows = execute_query(query, params_list)
                if not rows:
                    like = f"%{q}%"
                    conditions = [
                        "(p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)",
                        "p.archived=0",
                    ]
                    params_list = [like, like, like]
                    apply_common_filters(conditions, params_list)
                    where_sql = " AND ".join(conditions)
                    query = f"""
                        SELECT {card_select}
                        FROM product p
                        LEFT JOIN ({totals_subquery}) t ON t.product_id=p.id
                        LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
                        WHERE {where_sql}
                        ORDER BY (COALESCE(t.total_filtered,0) > 0) DESC,
                                 (COALESCE(t.total_all,0) > 0) DESC,
                                 p.id DESC
                        LIMIT ?
                    """
                    rows = execute_query(query, params_list)
            else:
                like = f"%{q}%"
                conditions = [
                    "p.archived=0",
                    "(p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ?)",
                ]
                params_list = [like, like, like]
                apply_common_filters(conditions, params_list)
                where_sql = " AND ".join(conditions) if conditions else "1=1"
                query = f"""
                    SELECT {card_select},
                           COALESCE(t.total_all,0) AS total
                    FROM product p
                    LEFT JOIN ({totals_subquery}) t ON t.product_id=p.id
                    LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
                    WHERE {where_sql}
                    ORDER BY (COALESCE(t.total_filtered,0) > 0) DESC,
                             (COALESCE(t.total_all,0) > 0) DESC,
                             p.id DESC
                    LIMIT ?
                """
                rows = execute_query(query, params_list)
        else:
            conditions = ["p.archived=0"]
            params_list: List[Any] = []
            apply_common_filters(conditions, params_list)
            where_sql = " AND ".join(conditions) if conditions else "1=1"
            query = f"""
                SELECT {card_select},
                       COALESCE(t.total_all,0) AS total
                FROM product p
                LEFT JOIN ({totals_subquery}) t ON t.product_id=p.id
                LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
                WHERE {where_sql}
                ORDER BY (COALESCE(t.total_filtered,0) > 0) DESC,
                         (COALESCE(t.total_all,0) > 0) DESC,
                         p.id DESC
                LIMIT ?
            """
            rows = execute_query(query, params_list)
    except Exception:
        like_raw = f"%{q}%"
        sq = (q or "").replace("Ё", "Е").replace("ё", "е").strip().lower()
        like_simpl = f"%{sq}%"
        conditions = [
            "p.archived=0",
            "(p.article LIKE ? OR p.name LIKE ? OR COALESCE(p.local_name,'') LIKE ? "
            "OR REPLACE(LOWER(p.name),'ё','е') LIKE ? "
            "OR REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?)",
        ]
        params_list = [like_raw, like_raw, like_raw, like_simpl, like_simpl]
        apply_common_filters(conditions, params_list)
        where_sql = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT {card_select},
                   COALESCE(t.total_all,0) AS total
            FROM product p
            LEFT JOIN ({totals_subquery}) t ON t.product_id=p.id
            LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
            WHERE {where_sql}
            ORDER BY (COALESCE(t.total_filtered,0) > 0) DESC,
                     (COALESCE(t.total_all,0) > 0) DESC,
                     p.id DESC
            LIMIT ?
        """
        rows = execute_query(query, params_list)

    return rows


def find_similar_cards(
    conn: sqlite3.Connection,
    product_id: int,
    *,
    limit: int = 30,
    threshold: float = 0.7,
) -> Tuple[List[sqlite3.Row], Dict[int, Dict[str, Any]]]:
    try:
        limit_val = int(limit)
    except (TypeError, ValueError):
        limit_val = 30
    limit = max(1, min(limit_val, 200))
    try:
        threshold_val = float(threshold)
    except (TypeError, ValueError):
        threshold_val = 0.7
    threshold = max(0.0, min(threshold_val, 1.0))

    base_row = conn.execute(
        "SELECT id, article, name, local_name FROM product WHERE id=?",
        (product_id,),
    ).fetchone()
    if not base_row:
        return [], {}

    phrase_rows = conn.execute(
        "SELECT phrase FROM display_name_exception ORDER BY lower(phrase)"
    ).fetchall()
    phrases = [row["phrase"] for row in phrase_rows if row["phrase"]]

    base_variants_raw = [base_row["local_name"], base_row["name"]]
    base_variants: List[str] = []
    seen_base: Set[str] = set()
    for variant in base_variants_raw:
        normalized = _normalize_match_name(variant, phrases)
        if not normalized or normalized in seen_base:
            continue
        seen_base.add(normalized)
        base_variants.append(normalized)
    if not base_variants:
        return [], {}

    base_tokens: List[str] = []
    seen_tokens: Set[str] = set()
    for variant in base_variants:
        for token in _tokenize_match_name(variant):
            if token in seen_tokens:
                continue
            seen_tokens.add(token)
            base_tokens.append(token)

    card_select = (
        "p.id, p.article, p.name, p.local_name, p.photo_path, p.brand_country, "
        "p.manufacturer_id, m.name AS manufacturer_name, m.country AS manufacturer_country"
    )

    candidate_rows: List[sqlite3.Row] = []
    candidate_ids: Set[int] = set()
    max_candidates = max(limit * 6, 120)

    def add_candidates(rows: Sequence[sqlite3.Row]) -> bool:
        for row in rows:
            pid = int(row["id"])
            if pid == product_id or pid in candidate_ids:
                continue
            candidate_ids.add(pid)
            candidate_rows.append(row)
            if len(candidate_ids) >= max_candidates:
                return True
        return False

    token_limit = max(limit * 5, 60)
    if base_tokens:
        token_clauses: List[str] = []
        params: List[Any] = [product_id]
        for token in base_tokens[:5]:
            like = f"%{token}%"
            token_clauses.append(
                "REPLACE(LOWER(COALESCE(p.local_name,'')),'ё','е') LIKE ?"
            )
            params.append(like)
            token_clauses.append(
                "REPLACE(LOWER(COALESCE(p.name,'')),'ё','е') LIKE ?"
            )
            params.append(like)
        where_sql = " AND ".join([
            "p.archived=0",
            "p.id<>?",
            "(" + " OR ".join(token_clauses) + ")",
        ])
        rows = conn.execute(
            f"""
            SELECT {card_select}
            FROM product p
            LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
            WHERE {where_sql}
            LIMIT ?
            """,
            params + [token_limit],
        ).fetchall()
        if add_candidates(rows):
            candidate_rows = candidate_rows[:max_candidates]
    else:
        rows = conn.execute(
            """
            SELECT p.id, p.article, p.name, p.local_name, p.photo_path,
                   p.brand_country, p.manufacturer_id,
                   m.name AS manufacturer_name,
                   m.country AS manufacturer_country
            FROM product p
            LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
            WHERE p.archived=0 AND p.id<>?
            ORDER BY p.id DESC
            LIMIT ?
            """,
            (product_id, token_limit),
        ).fetchall()
        add_candidates(rows)

    if base_tokens and len(candidate_ids) < max_candidates:
        alias_limit = max(limit * 3, 45)
        for token in base_tokens[:5]:
            like = f"%{token}%"
            alias_rows = conn.execute(
                """
                SELECT p.id, p.article, p.name, p.local_name, p.photo_path,
                       p.brand_country, p.manufacturer_id,
                       m.name AS manufacturer_name,
                       m.country AS manufacturer_country
                FROM product_name_alias a
                JOIN product p ON p.id=a.product_id
                LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
                WHERE p.archived=0 AND p.id<>?
                  AND REPLACE(a.normalized_name,'ё','е') LIKE ?
                LIMIT ?
                """,
                (product_id, like, alias_limit),
            ).fetchall()
            if add_candidates(alias_rows):
                break

    scored: List[Tuple[float, sqlite3.Row]] = []
    for row in candidate_rows:
        variants_raw = [row["local_name"], row["name"]]
        candidate_variants: List[str] = []
        seen_variant: Set[str] = set()
        for raw in variants_raw:
            normalized = _normalize_match_name(raw, phrases)
            if not normalized or normalized in seen_variant:
                continue
            seen_variant.add(normalized)
            candidate_variants.append(normalized)
        if not candidate_variants:
            continue
        best_score = 0.0
        for base_text in base_variants:
            for cand_text in candidate_variants:
                score = _similarity_ratio(base_text, cand_text)
                if score > best_score:
                    best_score = score
        if best_score >= threshold:
            scored.append((best_score, row))

    scored.sort(key=lambda item: (item[0], int(item[1]["id"])), reverse=True)
    top = scored[:limit]
    extras = {int(row["id"]): {"match_score": round(score, 4)} for score, row in top}
    top_rows = [row for _, row in top]
    return top_rows, extras


def find_similar_groups(
    conn: sqlite3.Connection,
    *,
    limit: int = 20,
    threshold: float = 0.7,
    use_exceptions: bool = True,
) -> List[Dict[str, Any]]:
    try:
        limit_val = int(limit)
    except (TypeError, ValueError):
        limit_val = 20
    limit = max(1, min(limit_val, 200))
    try:
        threshold_val = float(threshold)
    except (TypeError, ValueError):
        threshold_val = 0.7
    threshold = max(0.0, min(threshold_val, 1.0))

    if use_exceptions:
        phrase_rows = conn.execute(
            "SELECT phrase FROM display_name_exception ORDER BY lower(phrase)"
        ).fetchall()
        phrases: Sequence[str] = [row["phrase"] for row in phrase_rows if row["phrase"]]
    else:
        phrases = []

    product_rows = conn.execute(
        """
        SELECT p.id,
               p.article,
               p.name,
               p.local_name,
               p.photo_path,
               p.brand_country,
               p.manufacturer_id,
               m.name AS manufacturer_name,
               m.country AS manufacturer_country
        FROM product p
        LEFT JOIN manufacturer m ON m.id=p.manufacturer_id
        WHERE p.archived=0
        """
    ).fetchall()
    if not product_rows:
        return []

    alias_rows = conn.execute(
        "SELECT product_id, normalized_name FROM product_name_alias"
    ).fetchall()
    alias_map: Dict[int, List[str]] = {}
    for alias_row in alias_rows:
        pid = int(alias_row["product_id"])
        raw_alias = alias_row["normalized_name"]
        normalized_alias = _normalize_match_name(raw_alias, phrases)
        if not normalized_alias:
            continue
        alias_map.setdefault(pid, []).append(normalized_alias)

    candidates: List[Dict[str, Any]] = []
    token_map: Dict[str, List[int]] = {}
    for row in product_rows:
        variants_raw = [row["local_name"], row["name"]]
        variants_raw.extend(alias_map.get(int(row["id"]), []))
        variants: List[str] = []
        seen_variants: Set[str] = set()
        for variant in variants_raw:
            normalized = _normalize_match_name(variant, phrases)
            if not normalized or normalized in seen_variants:
                continue
            seen_variants.add(normalized)
            variants.append(normalized)
        if not variants:
            continue
        tokens: Set[str] = set()
        for variant in variants:
            for token in _tokenize_match_name(variant):
                tokens.add(token)
        if not tokens:
            continue
        idx = len(candidates)
        candidates.append({
            "row": row,
            "variants": variants,
            "tokens": tokens,
        })
        for token in tokens:
            token_map.setdefault(token, []).append(idx)

    if not candidates:
        return []

    MAX_TOKEN_NEIGHBORS = 400
    adjacency: Dict[int, Set[int]] = {i: set() for i in range(len(candidates))}
    edge_scores: Dict[Tuple[int, int], float] = {}

    for idx, cand in enumerate(candidates):
        possible: Set[int] = set()
        for token in cand["tokens"]:
            neighbors = token_map.get(token, [])
            if len(neighbors) > MAX_TOKEN_NEIGHBORS:
                continue
            for other_idx in neighbors:
                if other_idx <= idx:
                    continue
                possible.add(other_idx)
        if not possible:
            continue
        for other_idx in possible:
            other = candidates[other_idx]
            best_score = 0.0
            for left in cand["variants"]:
                for right in other["variants"]:
                    score = _similarity_ratio(left, right)
                    if score > best_score:
                        best_score = score
                        if best_score >= 0.9999:
                            break
                if best_score >= 0.9999:
                    break
            if best_score < threshold:
                continue
            adjacency[idx].add(other_idx)
            adjacency[other_idx].add(idx)
            edge_scores[(idx, other_idx)] = best_score

    visited: Set[int] = set()
    groups_idx: List[List[int]] = []
    for idx in range(len(candidates)):
        if idx in visited:
            continue
        if not adjacency.get(idx):
            continue
        stack = [idx]
        component: List[int] = []
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    stack.append(neighbor)
        if len(component) >= 2:
            groups_idx.append(component)

    if not groups_idx:
        return []

    groups: List[Dict[str, Any]] = []
    for component in groups_idx:
        per_idx_best: Dict[int, float] = {}
        group_best = 0.0
        for idx in component:
            best = 0.0
            for neighbor in component:
                if neighbor == idx:
                    continue
                key = (min(idx, neighbor), max(idx, neighbor))
                score = edge_scores.get(key, 0.0)
                if score > best:
                    best = score
                if score > group_best:
                    group_best = score
            per_idx_best[idx] = best
        sorted_component = sorted(
            component,
            key=lambda i: (-per_idx_best.get(i, 0.0), int(candidates[i]["row"]["id"])),
        )
        rows = [candidates[i]["row"] for i in sorted_component]
        extras: Dict[int, Dict[str, Any]] = {}
        for i in sorted_component:
            pid = int(candidates[i]["row"]["id"])
            best_score = per_idx_best.get(i, 0.0)
            if best_score > 0:
                extras[pid] = {"match_score": round(best_score, 4)}
        group_id = min(int(candidates[i]["row"]["id"]) for i in sorted_component)
        groups.append(
            {
                "group_id": group_id,
                "size": len(sorted_component),
                "score": round(group_best, 4),
                "rows": rows,
                "extras": extras,
            }
        )

    groups.sort(key=lambda g: (g["score"], g["size"], -g["group_id"]), reverse=True)
    return groups[:limit]


__all__ = [
    "cards_search",
    "find_similar_cards",
    "find_similar_groups",
]
