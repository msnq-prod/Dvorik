# Schema Mapping Notes

This document captures the one-to-one mapping between the legacy SQLite schema defined in `dvorik/db/migrations.py` and the new PostgreSQL structure expressed via Laravel migrations.

## Core Conversions

| SQLite Table | Laravel Table | Key Type Adjustments |
| --- | --- | --- |
| `manufacturer` | `manufacturer` | `INTEGER PRIMARY KEY AUTOINCREMENT` → `BIGSERIAL`; timestamp stored as text replaced with `timestamp with time zone` via `timestampTz` and `useCurrent()`. |
| `supplier` | `supplier` | Same as above; nullable `contact` kept as string. |
| `product` | `product` | `REAL` price/vat mapped to `decimal(12,2)`/`decimal(5,2)`; boolean flags stored as integers now true booleans; timestamps stored as text replaced with `timestampTz`. |
| `location` | `location` | Primary key stays textual; timestamp upgraded to `timestampTz`. |
| `stock` | `stock` | `REAL` quantities mapped to `decimal(15,3)`; composite primary key preserved; timestamp upgraded to `timestampTz`. |
| `supplier_sku` | `supplier_sku` | Boolean `active` column now real boolean; numeric quantities use `decimal(15,3)`; timestamps upgraded. |
| `display_name_exception` | `display_name_exception` | Timestamp stored as text → `timestampTz`. |
| `import_log` | `import_log` | `REAL` counts mapped to unsigned integer; JSON/text fields preserved; timestamps upgraded. |
| `product_merge_log` | `product_merge_log` | JSON payload stored in JSON columns; timestamps upgraded. |
| `product_article_alias` | `product_article_alias` | Unique alias maintained; timestamps upgraded. |
| `product_name_alias` | `product_name_alias` | Normalised name unique index preserved; timestamps upgraded. |
| `product_merge_rule` | `product_merge_rule` | Boolean `active` flag stored as integer now boolean; JSON fields mapped to JSON; timestamps upgraded. |
| `schedule_day` | `schedule_day` | Date primary key preserved; integer flag converted to boolean. |
| `schedule_assignment` | `schedule_assignment` | `created_at` stored via `timestampTz`. |
| `schedule_transfer_request` | `schedule_transfer_request` | Status check enforced via Laravel enum; timestamps upgraded. |
| `schedule_anchor` | `schedule_anchor` | Date stored with native date column. |
| `user_role` | `user_role` | Role check converted to Laravel enum; timestamps upgraded. |
| `user_notify` | `user_notify` | Enum columns replace CHECK constraints; timestamps upgraded. |
| `event_log` | `event_log` | Numeric delta stored as `decimal(15,3)`; timestamps upgraded. |
| `registration_request` | `registration_request` | Enum for status; timestamps upgraded. |
| `query_registry` | `query_registry` | Primary key string preserved; timestamp upgraded. |
| `ui_widget` | `ui_widget` | JSON/text fields maintained; uniqueness on `(module, name)` kept. |
| `ui_widget_instance` | `ui_widget_instance` | Boolean `enabled` stored as boolean; JSON config preserved. |
| `ui_menu` | `ui_menu` | Self-referencing foreign key maintained; visibility flag stored as boolean. |
| `scheduled_job` | `scheduled_job` | Enum for schedule type; timestamps upgraded. |
| `audit_log` | `audit_log` | Timestamp upgraded; payload stored as JSON. |

## Full-Text Search Replacement

The legacy `product_fts` virtual table built with SQLite FTS5 is replaced by a physical PostgreSQL table backed by a `tsvector` column:

- Table `product_fts` stores a `product_id` primary key linked to `product` and a `search_vector` `tsvector` column.
- A PostgreSQL trigger function (`product_fts_refresh`) materialises combined `article`, `name`, and `local_name` text into the vector using the `simple` dictionary.
- Three triggers (`product_fts_ai`, `product_fts_au`, `product_fts_ad`) refresh or remove the search document on insert/update/delete.
- A GIN index (`product_fts_search_vector_gin`) accelerates search queries that replace the prior `fts5` index.

## Boolean Casting During Import

The Laravel artisan command `legacy:ingest-sqlite` casts integer-backed boolean columns (`is_new`, `archived`, `active`, `visible`, `enabled`, etc.) into actual PostgreSQL booleans during ingestion to match the new schema expectations.
