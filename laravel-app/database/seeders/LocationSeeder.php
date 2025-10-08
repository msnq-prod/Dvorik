<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Date;

class LocationSeeder extends Seeder
{
    public function run(): void
    {
        DB::table('location')->updateOrInsert(
            ['code' => 'SKL-0'],
            [
                'kind' => 'HUB',
                'title' => 'Центральный склад',
                'created_at' => Date::now(),
            ]
        );
    }
}
