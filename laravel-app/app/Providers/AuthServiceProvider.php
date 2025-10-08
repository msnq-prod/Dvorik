<?php

namespace App\Providers;

use App\Models\AuditLog;
use App\Models\ImportLog;
use App\Models\QueryRegistry;
use App\Models\UiWidget;
use App\Policies\AuditLogPolicy;
use App\Policies\ImportLogPolicy;
use App\Policies\QueryRegistryPolicy;
use App\Policies\UiWidgetPolicy;
use Illuminate\Foundation\Support\Providers\AuthServiceProvider as ServiceProvider;
use Illuminate\Support\Facades\Gate;

class AuthServiceProvider extends ServiceProvider
{
    protected $policies = [
        UiWidget::class => UiWidgetPolicy::class,
        QueryRegistry::class => QueryRegistryPolicy::class,
        ImportLog::class => ImportLogPolicy::class,
        AuditLog::class => AuditLogPolicy::class,
    ];

    public function boot(): void
    {
        $this->registerPolicies();

        Gate::before(function ($user, $ability) {
            return $user->is_super_admin ? true : null;
        });
    }
}
