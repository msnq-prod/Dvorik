<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class QueryRegistrySeeder extends Seeder
{
    public function run(): void
    {
        $queries = [
            'metrics.low_stock' => [
                'sql' => <<<'SQL'
                    SELECT
                        p.id AS product_id,
                        p.name AS product_name,
                        p.unit AS product_unit,
                        l.code AS location_code,
                        l.title AS location_title,
                        s.qty_pack::numeric AS qty_pack,
                        COALESCE(s.reserved_pack, 0)::numeric AS reserved_pack
                    FROM stock AS s
                    JOIN product AS p ON p.id = s.product_id
                    JOIN location AS l ON l.code = s.location_code
                    WHERE s.qty_pack <= COALESCE(:threshold, 0)
                    ORDER BY s.qty_pack ASC, p.name ASC
                    SQL,
                'description' => 'Returns products with stock below the configured threshold ordered by lowest quantity.',
            ],
            'metrics.schedule_snapshot' => [
                'sql' => <<<'SQL'
                    WITH upcoming AS (
                        SELECT
                            d.date,
                            d.is_open,
                            COALESCE(d.notes, '') AS notes,
                            COALESCE(string_agg(sa.tg_id::text, ', ' ORDER BY sa.tg_id), '—') AS assignees
                        FROM schedule_day AS d
                        LEFT JOIN schedule_assignment AS sa ON sa.date = d.date
                        WHERE d.date BETWEEN CURRENT_DATE AND (
                            CURRENT_DATE + (GREATEST(COALESCE(:days, 7), 1) - 1) * INTERVAL '1 day'
                        )
                        GROUP BY d.date, d.is_open, d.notes
                    )
                    SELECT
                        to_char(upcoming.date, 'YYYY-MM-DD') AS date,
                        CASE WHEN upcoming.is_open THEN 'Open' ELSE 'Closed' END AS status,
                        upcoming.assignees
                    FROM upcoming
                    ORDER BY upcoming.date ASC
                    SQL,
                'description' => 'Summarises upcoming schedule assignments for the configured horizon.',
            ],
            'metrics.stock_by_location' => [
                'sql' => <<<'SQL'
                    SELECT
                        l.code AS location_code,
                        l.title AS location_title,
                        p.id AS product_id,
                        p.name AS product_name,
                        s.qty_pack::numeric AS qty_pack
                    FROM stock AS s
                    JOIN product AS p ON p.id = s.product_id
                    JOIN location AS l ON l.code = s.location_code
                    WHERE (:location_code IS NULL OR l.code = :location_code)
                    ORDER BY l.code ASC, p.name ASC
                    SQL,
                'description' => 'Lists product stock quantities grouped by location, optionally filtered by location code.',
            ],
        ];

        foreach ($queries as $key => $definition) {
            DB::table('query_registry')->updateOrInsert(
                ['key' => $key],
                [
                    'sql' => trim($definition['sql']),
                    'description' => $definition['description'],
                    'updated_at' => now(),
                ]
            );
        }
    }
}
