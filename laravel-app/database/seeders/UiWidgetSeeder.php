<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class UiWidgetSeeder extends Seeder
{
    public function run(): void
    {
        $widgets = [
            [
                'module' => 'builtin',
                'name' => 'low_stock',
                'title' => 'Low stock overview',
                'description' => 'Highlights products that are running low on inventory.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:LowStockWidget',
                'config_schema' => null,
            ],
            [
                'module' => 'builtin',
                'name' => 'schedule_mini',
                'title' => 'Schedule snapshot',
                'description' => 'Shows the current duty schedule summary.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:ScheduleMiniWidget',
                'config_schema' => null,
            ],
            [
                'module' => 'builtin',
                'name' => 'stock_by_location',
                'title' => 'Stock by location',
                'description' => 'Displays stock totals grouped by location.',
                'entrypoint' => 'dvorik.admin.widgets.builtin:StockByLocationWidget',
                'config_schema' => null,
            ],
        ];

        foreach ($widgets as $widget) {
            DB::table('ui_widget')->updateOrInsert(
                [
                    'module' => $widget['module'],
                    'name' => $widget['name'],
                ],
                [
                    'title' => $widget['title'],
                    'description' => $widget['description'],
                    'entrypoint' => $widget['entrypoint'],
                    'config_schema' => $widget['config_schema'],
                ],
            );
        }
    }
}
