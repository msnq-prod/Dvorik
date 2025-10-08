<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Date;

class SupplierSeeder extends Seeder
{
    public function run(): void
    {
        DB::table('supplier')->updateOrInsert(
            ['name' => '__default__'],
            [
                'contact' => null,
                'created_at' => Date::now(),
            ]
        );
    }
}
