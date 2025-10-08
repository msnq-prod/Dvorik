<?php

return [
    'default_panel' => 'admin',

    'panels' => [
        'admin' => [
            'id' => 'admin',
            'path' => env('FILAMENT_PATH', 'admin'),
            'auth' => [
                'guard' => env('FILAMENT_DEFAULT_GUARD', 'filament'),
                'middleware' => [
                    \Filament\Http\Middleware\Authenticate::class,
                    \App\Http\Middleware\EnsureFilamentUserHasAccess::class,
                ],
            ],
        ],
    ],
];
