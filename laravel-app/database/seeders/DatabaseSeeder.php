<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;

class DatabaseSeeder extends Seeder
{
    public function run(): void
    {
        $this->call([
            LocationSeeder::class,
            SupplierSeeder::class,
            UiMenuSeeder::class,
            UiWidgetSeeder::class,
            UiWidgetInstanceSeeder::class,
            RbacSeeder::class,
            SuperAdminSeeder::class,
        ]);
    }
}
