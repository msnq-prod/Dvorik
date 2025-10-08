<?php

namespace App\Policies;

use App\Models\UiWidget;
use App\Models\User;
use Illuminate\Auth\Access\HandlesAuthorization;

class UiWidgetPolicy
{
    use HandlesAuthorization;

    public function viewAny(User $user): bool
    {
        return $user->can('widgets.manage');
    }

    public function view(User $user, UiWidget $widget): bool
    {
        return $user->can('widgets.manage');
    }

    public function create(User $user): bool
    {
        return $user->can('widgets.manage');
    }

    public function update(User $user, UiWidget $widget): bool
    {
        return $user->can('widgets.manage');
    }

    public function delete(User $user, UiWidget $widget): bool
    {
        return $user->can('widgets.manage');
    }
}
