<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class UiMenuSeeder extends Seeder
{
    public function run(): void
    {
        $entries = [
            ['slug' => 'dashboard', 'title' => 'Dashboard', 'url' => '/', 'icon' => null, 'position' => 0],
            ['slug' => 'supply', 'title' => 'Supply', 'url' => '/supply', 'icon' => null, 'position' => 1],
            ['slug' => 'tables', 'title' => 'Tables', 'url' => '/tables', 'icon' => null, 'position' => 2],
            ['slug' => 'superadmin', 'title' => 'Superadmin', 'url' => '/superadmin', 'icon' => null, 'position' => 3],
        ];

        foreach ($entries as $entry) {
            DB::table('ui_menu')->updateOrInsert(
                ['slug' => $entry['slug']],
                [
                    'title' => $entry['title'],
                    'url' => $entry['url'],
                    'icon' => $entry['icon'],
                    'parent_id' => null,
                    'position' => $entry['position'],
                    'target' => null,
                    'required_role' => null,
                    'visible' => true,
                ]
            );
        }
    }
}
