# RBAC Migration Plan

This project now relies on [spatie/laravel-permission](https://spatie.be/docs/laravel-permission) for managing panel roles.

## Package & Database Setup

- `composer.json` includes the package and the default guard is configured for the Filament panel.
- Published configuration lives in `config/permission.php` with the guard fixed to `filament` so every permission check uses the panel session.
- Published migrations create the canonical `roles`, `permissions`, and pivot tables. They set a default guard name of `filament` on every record.

Run the migrations after pulling these changes:

```bash
php artisan migrate
```

## Role & Permission Mapping

`Database\Seeders\RbacSeeder` provisions the full permission matrix based on the legacy `user_role` table:

| Permission        | Purpose                                              | Granted to                 |
| ----------------- | ---------------------------------------------------- | -------------------------- |
| `widgets.manage`  | Manage widget catalog and dashboard placements.      | `superadmin`               |
| `queries.manage`  | Manage SQL registry/table explorer entries.          | `superadmin`               |
| `imports.manage`  | Run, review, and revert inventory imports.           | `superadmin`, `admin`      |
| `audit.view`      | Review the administrative audit log.                 | `superadmin`, `admin`      |

The seeder also introspects `user_role.role` values. Any unexpected legacy role name is created in the new `roles` table (with no permissions) so that manual review can take place instead of silently dropping access.

`SuperAdminSeeder` assigns the `superadmin` role to the bootstrap account while keeping the hard `is_super_admin` override in place.

Seed roles and permissions with:

```bash
php artisan db:seed --class=Database\\Seeders\\RbacSeeder
php artisan db:seed --class=Database\\Seeders\\SuperAdminSeeder
```

The full `DatabaseSeeder` already calls both, so a plain `php artisan db:seed` after migrating is sufficient for fresh installs.

## Policy Overview

Policies now protect the Laravel replacements for the Flask administration screens:

- `UiWidgetPolicy` → widget catalog & placements (requires `widgets.manage`).
- `QueryRegistryPolicy` → SQL registry / saved table queries (requires `queries.manage`).
- `ImportLogPolicy` → supply imports (requires `imports.manage`).
- `AuditLogPolicy` → audit log reader (requires `audit.view`).

The policies are registered in `AuthServiceProvider` and still inherit the blanket access that super admins receive through `Gate::before`.

## Filament Guard & Navigation

The Filament panel authenticates against the dedicated `filament` guard. A custom middleware (`EnsureFilamentUserHasAccess`) now runs on every panel request to ensure the signed-in user holds one of the panel roles (`superadmin`, `admin`, or `seller`).

Navigation items and groups can call the `requiresPermission()` macro added in `FilamentServiceProvider`. The provider also filters the resolved navigation tree at runtime, so entries tagged with a permission disappear automatically for users who lack the capability.

## Assigning Roles To Users

1. Import the legacy SQLite data using `php artisan legacy:ingest-sqlite ...`.
2. Confirm that `user_role` rows exist for each Telegram account. The values (`admin`, `seller`, etc.) now have corresponding rows in the new `roles` table.
3. Link Laravel users to roles. Typical workflows:
   - Match by email/username and call `$user->assignRole('admin')` in a tinker session.
   - Use an upcoming Filament resource for managing accounts (recommended once the panel is available).
4. Re-run `php artisan permission:cache-reset` (or any seed command) after bulk changes to flush cached permissions.

Document each manual assignment in your migration checklist so the bot and panel stay in sync.
