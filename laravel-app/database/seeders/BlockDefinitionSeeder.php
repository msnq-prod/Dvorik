<?php

namespace Database\Seeders;

use App\Models\BlockDefinition;
use Illuminate\Database\Seeder;

class BlockDefinitionSeeder extends Seeder
{
    public function run(): void
    {
        $legacyMap = config('block-runtime.legacy_entrypoints', []);

        $definitions = [
            [
                'module' => 'builtin',
                'name' => 'low_stock',
                'title' => 'Low stock overview',
                'description' => 'Highlights products that are running low on inventory.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:LowStockWidget',
            ],
            [
                'module' => 'builtin',
                'name' => 'schedule_mini',
                'title' => 'Schedule snapshot',
                'description' => 'Shows the current duty schedule summary.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:ScheduleMiniWidget',
            ],
            [
                'module' => 'builtin',
                'name' => 'stock_by_location',
                'title' => 'Stock by location',
                'description' => 'Displays stock totals grouped by location.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:StockByLocationWidget',
            ],
        ];

        foreach ($definitions as $definition) {
            $mapping = $legacyMap[$definition['entrypoint']] ?? null;

            if ($mapping === null) {
                continue;
            }

            BlockDefinition::query()->updateOrCreate(
                [
                    'module' => $definition['module'],
                    'name' => $definition['name'],
                    'version' => (int) ($mapping['version'] ?? 1),
                ],
                [
                    'title' => $definition['title'],
                    'description' => $definition['description'],
                    'component' => $mapping['component'],
                    'metadata' => [
                        'legacy_entrypoint' => $definition['entrypoint'],
                    ],
                ]
            );
        }
    }
}
