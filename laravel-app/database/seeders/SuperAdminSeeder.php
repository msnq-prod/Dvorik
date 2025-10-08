<?php

namespace Database\Seeders;

use App\Models\User;
use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\Hash;
use Spatie\Permission\Models\Role;

class SuperAdminSeeder extends Seeder
{
    public function run(): void
    {
        $guard = config('permission.defaults.guard', 'filament');

        Role::firstOrCreate([
            'name' => 'superadmin',
            'guard_name' => $guard,
        ]);

        $user = User::updateOrCreate(
            ['email' => config('app.super_admin_email', 'admin@example.com')],
            [
                'name' => config('app.super_admin_name', 'Super Admin'),
                'password' => Hash::make(config('app.super_admin_password', 'password')),
                'is_super_admin' => true,
                'email_verified_at' => now(),
            ]
        );

        $user->assignRole('superadmin');
    }
}
