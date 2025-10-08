<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class UiWidgetInstanceSeeder extends Seeder
{
    public function run(): void
    {
        $defaultOrder = [
            'builtin.low_stock',
            'builtin.schedule_mini',
            'builtin.stock_by_location',
        ];

        if (DB::table('ui_widget_instance')->exists()) {
            return;
        }

        foreach ($defaultOrder as $position => $key) {
            [$module, $name] = explode('.', $key, 2);

            $widgetId = DB::table('ui_widget')
                ->where('module', $module)
                ->where('name', $name)
                ->value('id');

            if ($widgetId === null) {
                continue;
            }

            DB::table('ui_widget_instance')->updateOrInsert(
                [
                    'zone' => 'home.main',
                    'position' => $position,
                ],
                [
                    'widget_id' => $widgetId,
                    'config_json' => null,
                    'enabled' => true,
                ],
            );
        }
    }
}
