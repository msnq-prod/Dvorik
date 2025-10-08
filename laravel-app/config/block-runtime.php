<?php

return [
    'zone_cache_ttl' => 60,

    'legacy_entrypoints' => [
        'dvorik.admin.widgets.builtin:LowStockWidget' => [
            'component' => \App\Livewire\Blocks\LegacyWidgetPlaceholder::class,
            'version' => 1,
        ],
        'dvorik.admin.widgets.builtin:ScheduleMiniWidget' => [
            'component' => \App\Livewire\Blocks\LegacyWidgetPlaceholder::class,
            'version' => 1,
        ],
        'dvorik.admin.widgets.builtin:StockByLocationWidget' => [
            'component' => \App\Livewire\Blocks\LegacyWidgetPlaceholder::class,
            'version' => 1,
        ],
    ],

    'legacy_zone_map' => [
        'home.main' => 'admin.dashboard.main',
    ],

    'render_hooks' => [
        'admin.dashboard.main' => [
            'panel' => 'admin',
            'route' => 'filament.admin.pages.dashboard',
            'hook' => 'panels::content.start',
        ],
    ],
];
