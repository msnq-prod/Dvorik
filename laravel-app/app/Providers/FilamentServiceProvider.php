<?php

namespace App\Providers;

use App\Models\User;
use App\Services\BlockRuntime;
use Filament\Facades\Filament;
use Filament\Navigation\NavigationBuilder;
use Filament\Navigation\NavigationGroup;
use Filament\Navigation\NavigationItem;
use Illuminate\Support\ServiceProvider;

class FilamentServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        //
    }

    public function boot(): void
    {
        NavigationItem::macro('requiresPermission', function (?string $permission) {
            if ($permission === null) {
                return $this;
            }

            /** @var NavigationItem $this */
            $this->visible(fn (): bool => auth()->user()?->can($permission) ?? false);

            return $this;
        });

        NavigationGroup::macro('requiresPermission', function (?string $permission) {
            if ($permission === null) {
                return $this;
            }

            /** @var NavigationGroup $this */
            $this->visible(fn (): bool => auth()->user()?->can($permission) ?? false);

            return $this;
        });

        Filament::navigation(function (NavigationBuilder $builder): NavigationBuilder {
            $user = auth()->user();

            if (! $user instanceof User) {
                return $builder;
            }

            $items = method_exists($builder, 'getItems')
                ? collect($builder->getItems())
                    ->filter(fn (NavigationItem $item): bool => $this->navigationItemVisibleForUser($item, $user))
                    ->all()
                : [];

            $groups = method_exists($builder, 'getGroups')
                ? collect($builder->getGroups())
                    ->map(function (NavigationGroup $group) use ($user): NavigationGroup {
                        $visibleItems = method_exists($group, 'getItems')
                            ? collect($group->getItems())
                                ->filter(fn (NavigationItem $item): bool => $this->navigationItemVisibleForUser($item, $user))
                                ->all()
                            : [];

                        return $group->items($visibleItems);
                    })
                    ->filter(fn (NavigationGroup $group): bool => method_exists($group, 'getItems') ? count($group->getItems()) > 0 : true)
                    ->all()
                : [];

            return $builder
                ->items($items)
                ->groups($groups);
        });

        $renderHooks = config('block-runtime.render_hooks', []);

        foreach ($renderHooks as $zone => $hookConfiguration) {
            $hook = $hookConfiguration['hook'] ?? null;

            if ($hook === null) {
                continue;
            }

            Filament::registerRenderHook($hook, function () use ($zone, $hookConfiguration): string {
                $panelId = Filament::getCurrentPanel()?->getId();
                $expectedPanel = $hookConfiguration['panel'] ?? null;

                if ($expectedPanel !== null && $panelId !== $expectedPanel) {
                    return '';
                }

                $routeName = $hookConfiguration['route'] ?? null;

                if ($routeName !== null && ! request()->routeIs($routeName)) {
                    return '';
                }

                return (string) app(BlockRuntime::class)->renderZone($zone);
            });
        }

        Filament::serving(function (): void {
            app(BlockRuntime::class)->warmDefinitionCache();
        });
    }

    private function navigationItemVisibleForUser(NavigationItem $item, User $user): bool
    {
        $meta = method_exists($item, 'getMeta') ? $item->getMeta() : [];

        $requiredPermission = $meta['required_permission'] ?? null;

        if ($requiredPermission === null) {
            return true;
        }

        return $user->can($requiredPermission);
    }
}
