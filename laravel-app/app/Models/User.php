<?php

namespace App\Models;

use Filament\Models\Contracts\FilamentUser;
use Filament\Models\Contracts\HasName as FilamentHasName;
use Filament\Panels\Contracts\Panel;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Foundation\Auth\User as Authenticatable;
use Illuminate\Notifications\Notifiable;
use Laravel\Sanctum\HasApiTokens;
use Spatie\Permission\Traits\HasRoles;

class User extends Authenticatable implements FilamentUser, FilamentHasName
{
    use HasApiTokens, HasFactory, Notifiable, HasRoles;

    protected string $guard_name = 'filament';

    protected $fillable = [
        'name',
        'email',
        'password',
        'is_super_admin',
    ];

    protected $hidden = [
        'password',
        'remember_token',
    ];

    protected $casts = [
        'email_verified_at' => 'datetime',
        'password' => 'hashed',
        'is_super_admin' => 'boolean',
    ];

    public function canAccessPanel(Panel $panel): bool
    {
        if ($this->is_super_admin) {
            return true;
        }

        return $this->hasAnyRole(['superadmin', 'admin', 'seller']);
    }

    public function getFilamentName(): string
    {
        return $this->name ?? $this->email;
    }
}
