# Laravel Setup Notes

## Launch Notes

- The Laravel skeleton under `laravel-app/` is generated for PHP 8.3 (for example: `mise exec php@8.3 -- laravel new laravel-app`).
- Composer requirements include Filament v3 and Livewire v3 for the upcoming admin interface.
- Default guard configuration now exposes a dedicated `filament` guard for panel authentication and is surfaced via the `AUTH_GUARD` environment variable.
- Initial commits are limited to the Laravel tree to keep the PHP history isolated from the existing Python sources.

## Environment Configuration

The `.env.example` file sets sensible defaults for local development:

- PostgreSQL connection (`dvorik_admin` database, `postgres` user).
- Redis-backed cache, queue, and session drivers.
- Storage disks including S3-compatible configuration and a public URL helper.
- Telegram bot token, webhook secret, and super-admin chat ID placeholders required by later tasks.
- Super admin bootstrap credentials (`SUPERADMIN_*`) consumed by the seeder.
- `AUTH_GUARD` defaults the application to Filament authentication out of the box.

## Authentication & Authorization

- `config/auth.php` registers a `filament` guard that reuses the standard `users` provider.
- `app/Models/User` implements the `FilamentUser` contract and introduces an `is_super_admin` flag.
- Policy overrides grant super admins blanket access through `AuthServiceProvider`.

## Database & Seeding

- Core Laravel migrations include an `is_super_admin` boolean on `users`.
- `Database\Seeders\SuperAdminSeeder` provisions a default super admin using the credentials above and guarantees the `superadmin` role exists.
- `DatabaseSeeder` wires the seeder so `php artisan db:seed` creates the account automatically.

## Next Steps

1. Run `composer install` followed by `npm install` inside `laravel-app/`.
2. Copy `.env.example` to `.env` and adjust secrets.
3. Execute database migrations and seeds:
   ```bash
   php artisan migrate --seed
   ```
4. Configure a queue worker (`php artisan queue:work`) once Redis is available.
