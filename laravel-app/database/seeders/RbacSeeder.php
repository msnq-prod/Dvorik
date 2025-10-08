<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Spatie\Permission\Models\Permission;
use Spatie\Permission\Models\Role;
use Spatie\Permission\PermissionRegistrar;

class RbacSeeder extends Seeder
{
    public function run(): void
    {
        /** @var \Spatie\Permission\PermissionRegistrar $registrar */
        $registrar = app(PermissionRegistrar::class);
        $registrar->forgetCachedPermissions();

        $guard = config('permission.defaults.guard', 'filament');

        $permissions = [
            'widgets.manage',
            'queries.manage',
            'imports.manage',
            'audit.view',
        ];

        foreach ($permissions as $permission) {
            Permission::firstOrCreate([
                'name' => $permission,
                'guard_name' => $guard,
            ]);
        }

        $roles = [
            'superadmin' => $permissions,
            'admin' => [
                'imports.manage',
                'audit.view',
            ],
            'seller' => [],
        ];

        $legacyRoles = Schema::hasTable('user_role')
            ? DB::table('user_role')->select('role')->distinct()->pluck('role')->all()
            : [];

        foreach ($legacyRoles as $legacyRole) {
            if (! array_key_exists($legacyRole, $roles)) {
                $roles[$legacyRole] = [];
            }
        }

        foreach ($roles as $roleName => $rolePermissions) {
            $role = Role::firstOrCreate([
                'name' => $roleName,
                'guard_name' => $guard,
            ]);

            $role->syncPermissions($rolePermissions);
        }

        $registrar->forgetCachedPermissions();
    }
}
