# Laravel Setup Notes

## Overview

- Added a Laravel 11 skeleton under `laravel-app/` configured for PHP 8.3.
- Composer requirements include Filament v3 and Livewire v3 for the upcoming admin interface.
- Default guard configuration now exposes a dedicated `filament` guard for panel authentication.

## Environment Configuration

The `.env.example` file sets sensible defaults for local development:

- PostgreSQL connection (`dvorik_admin` database, `postgres` user).
- Redis-backed cache, queue, and session drivers.
- Storage disks including S3-compatible configuration and a public URL helper.
- Telegram bot token, webhook secret, and super-admin chat ID placeholders required by later tasks.
- Super admin bootstrap credentials (`SUPERADMIN_*`) consumed by the seeder.

## Authentication & Authorization

- `config/auth.php` registers a `filament` guard that reuses the standard `users` provider.
- `app/Models/User` implements the `FilamentUser` contract and introduces an `is_super_admin` flag.
- Policy overrides grant super admins blanket access through `AuthServiceProvider`.

## Database & Seeding

- Core Laravel migrations include an `is_super_admin` boolean on `users`.
- `Database\Seeders\SuperAdminSeeder` provisions a default super admin using the credentials above.
- `DatabaseSeeder` wires the seeder so `php artisan db:seed` creates the account automatically.

## Next Steps

1. Run `composer install` followed by `npm install` inside `laravel-app/`.
2. Copy `.env.example` to `.env` and adjust secrets.
3. Execute database migrations and seeds:
   ```bash
   php artisan migrate --seed
   ```
4. Configure a queue worker (`php artisan queue:work`) once Redis is available.
