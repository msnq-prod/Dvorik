<?php

namespace App\Policies;

use App\Models\QueryRegistry;
use App\Models\User;
use Illuminate\Auth\Access\HandlesAuthorization;

class QueryRegistryPolicy
{
    use HandlesAuthorization;

    public function viewAny(User $user): bool
    {
        return $user->can('queries.manage');
    }

    public function view(User $user, QueryRegistry $query): bool
    {
        return $user->can('queries.manage');
    }

    public function create(User $user): bool
    {
        return $user->can('queries.manage');
    }

    public function update(User $user, QueryRegistry $query): bool
    {
        return $user->can('queries.manage');
    }

    public function delete(User $user, QueryRegistry $query): bool
    {
        return $user->can('queries.manage');
    }
}
