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
            QueryRegistrySeeder::class,
            BlockDefinitionSeeder::class,
            BlockInstanceSeeder::class,
            RbacSeeder::class,
            SuperAdminSeeder::class,
        ]);
    }
}
