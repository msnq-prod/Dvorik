<?php

namespace Database\Seeders;

use App\Models\BlockDefinition;
use Illuminate\Database\Seeder;

class BlockDefinitionSeeder extends Seeder
{
    public function run(): void
    {
        $legacyMap = config('block-runtime.legacy_entrypoints', []);

        $schemas = [
            'metric' => [
                'type' => 'object',
                'required' => ['data_source', 'columns'],
                'properties' => [
                    'title' => ['type' => 'string'],
                    'description' => ['type' => 'string'],
                    'data_source' => ['type' => 'string'],
                    'limit' => ['type' => 'integer', 'minimum' => 1, 'maximum' => 100],
                    'parameters' => ['type' => 'object'],
                    'columns' => [
                        'type' => 'array',
                        'items' => [
                            'type' => 'object',
                            'required' => ['key'],
                            'properties' => [
                                'key' => ['type' => 'string'],
                                'label' => ['type' => 'string'],
                            ],
                        ],
                    ],
                    'empty_message' => ['type' => 'string'],
                ],
            ],
        ];

        $defaultConfigs = [
            'builtin.low_stock' => [
                'title' => 'Low stock overview',
                'description' => 'Highlights products that are running low on inventory.',
                'data_source' => 'metrics.low_stock',
                'limit' => 10,
                'parameters' => [
                    'threshold' => 5,
                ],
                'columns' => [
                    ['key' => 'product_name', 'label' => 'Product'],
                    ['key' => 'location_title', 'label' => 'Location'],
                    ['key' => 'qty_pack', 'label' => 'Quantity'],
                ],
                'empty_message' => 'Inventory levels look healthy.',
            ],
            'builtin.schedule_mini' => [
                'title' => 'Schedule snapshot',
                'description' => 'Shows the current duty schedule summary.',
                'data_source' => 'metrics.schedule_snapshot',
                'limit' => 14,
                'parameters' => [
                    'days' => 14,
                ],
                'columns' => [
                    ['key' => 'date', 'label' => 'Date'],
                    ['key' => 'status', 'label' => 'Status'],
                    ['key' => 'assignees', 'label' => 'Assignees'],
                ],
                'empty_message' => 'No upcoming assignments have been scheduled.',
            ],
            'builtin.stock_by_location' => [
                'title' => 'Stock by location',
                'description' => 'Displays stock totals grouped by location.',
                'data_source' => 'metrics.stock_by_location',
                'limit' => 15,
                'parameters' => [
                    'location_code' => null,
                ],
                'columns' => [
                    ['key' => 'location_title', 'label' => 'Location'],
                    ['key' => 'product_name', 'label' => 'Product'],
                    ['key' => 'qty_pack', 'label' => 'Quantity'],
                ],
                'empty_message' => 'No stock was found for the selected locations.',
            ],
        ];

        $definitions = [
            [
                'module' => 'builtin',
                'name' => 'low_stock',
                'title' => 'Low stock overview',
                'description' => 'Highlights products that are running low on inventory.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:LowStockWidget',
                'schema_key' => 'metric',
            ],
            [
                'module' => 'builtin',
                'name' => 'schedule_mini',
                'title' => 'Schedule snapshot',
                'description' => 'Shows the current duty schedule summary.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:ScheduleMiniWidget',
                'schema_key' => 'metric',
            ],
            [
                'module' => 'builtin',
                'name' => 'stock_by_location',
                'title' => 'Stock by location',
                'description' => 'Displays stock totals grouped by location.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:StockByLocationWidget',
                'schema_key' => 'metric',
            ],
        ];

        foreach ($definitions as $definition) {
            $mapping = $legacyMap[$definition['entrypoint']] ?? null;

            if ($mapping === null) {
                continue;
            }

            $defaultConfig = $defaultConfigs[$definition['module'].'.'.$definition['name']] ?? null;

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
                    'config_schema' => $schemas[$definition['schema_key']] ?? null,
                    'metadata' => array_filter([
                        'legacy_entrypoint' => $definition['entrypoint'],
                        'default_config' => $defaultConfig,
                    ]),
                ]
            );
        }
    }
}
