<?php

namespace Database\Seeders;

use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use Illuminate\Database\Seeder;

class BlockInstanceSeeder extends Seeder
{
    public function run(): void
    {
        if (BlockInstance::query()->exists()) {
            return;
        }

        $zoneMap = config('block-runtime.legacy_zone_map', []);
        $zone = $zoneMap['home.main'] ?? 'admin.dashboard.main';

        $definitions = BlockDefinition::query()
            ->where('module', 'builtin')
            ->orderBy('name')
            ->get();

        foreach ($definitions as $position => $definition) {
            BlockInstance::query()->updateOrCreate(
                [
                    'zone' => $zone,
                    'position' => $position,
                ],
                [
                    'block_definition_id' => $definition->id,
                    'config' => null,
                    'enabled' => true,
                ]
            );
        }
    }
}
