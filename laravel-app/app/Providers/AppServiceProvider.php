<?php

namespace App\Providers;

use App\Services\BlockRuntime;
use Illuminate\Contracts\Cache\Factory as CacheFactory;
use Illuminate\Support\ServiceProvider;
use Opis\JsonSchema\Validator;

class AppServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->app->singleton(BlockRuntime::class, function ($app): BlockRuntime {
            /** @var CacheFactory $cacheFactory */
            $cacheFactory = $app->make(CacheFactory::class);

            return new BlockRuntime($cacheFactory->store(), new Validator());
        });
    }

    public function boot(): void
    {
        //
    }
}
