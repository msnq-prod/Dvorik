<?php

namespace App\Policies;

use App\Models\ImportLog;
use App\Models\User;
use Illuminate\Auth\Access\HandlesAuthorization;

class ImportLogPolicy
{
    use HandlesAuthorization;

    public function viewAny(User $user): bool
    {
        return $user->can('imports.manage');
    }

    public function view(User $user, ImportLog $import): bool
    {
        return $user->can('imports.manage');
    }

    public function create(User $user): bool
    {
        return $user->can('imports.manage');
    }

    public function update(User $user, ImportLog $import): bool
    {
        return $user->can('imports.manage');
    }

    public function delete(User $user, ImportLog $import): bool
    {
        return $user->can('imports.manage');
    }

    public function revert(User $user, ImportLog $import): bool
    {
        return $user->can('imports.manage');
    }
}
