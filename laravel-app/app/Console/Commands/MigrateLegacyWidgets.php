<?php

namespace App\Console\Commands;

use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use Illuminate\Console\Command;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Throwable;

class MigrateLegacyWidgets extends Command
{
    protected $signature = 'blocks:migrate-legacy {--dry-run : Run the migration without writing data}';

    protected $description = 'Migrate legacy ui_widget tables into the block runtime structure.';

    public function handle(): int
    {
        $dryRun = (bool) $this->option('dry-run');

        $componentMap = config('block-runtime.legacy_entrypoints', []);
        $zoneMap = config('block-runtime.legacy_zone_map', []);

        $legacyWidgets = DB::table('ui_widget')->orderBy('id')->get();
        $legacyInstances = DB::table('ui_widget_instance')->orderBy('id')->get();

        $definitionMap = [];
        $unmappedWidgets = [];

        try {
            DB::beginTransaction();

            foreach ($legacyWidgets as $widget) {
                $entrypoint = $widget->entrypoint;
                $mapping = $componentMap[$entrypoint] ?? null;

                if ($mapping === null) {
                    $unmappedWidgets[] = $entrypoint;
                    $this->warn("No component mapping defined for entrypoint [{$entrypoint}].");
                    continue;
                }

                $version = (int) ($mapping['version'] ?? 1);
                $component = $mapping['component'];

                $attributes = [
                    'module' => $widget->module,
                    'name' => $widget->name,
                    'version' => $version,
                ];

                $payload = [
                    'title' => $widget->title,
                    'description' => $widget->description,
                    'component' => $component,
                    'config_schema' => $widget->config_schema,
                    'metadata' => [
                        'legacy_entrypoint' => $entrypoint,
                    ],
                ];

                if ($dryRun) {
                    $this->line(sprintf('Would migrate definition %s:%s v%s to component %s', $widget->module, $widget->name, $version, $component));
                    $definition = new BlockDefinition($attributes + $payload);
                    $definition->id = $widget->id;
                } else {
                    $definition = BlockDefinition::query()->updateOrCreate($attributes, $payload);
                }

                $definitionMap[$widget->id] = $definition;
            }

            $skippedInstances = [];
            $migratedInstances = 0;

            foreach ($legacyInstances as $instance) {
                $legacyWidgetId = $instance->widget_id;
                $definition = $definitionMap[$legacyWidgetId] ?? null;

                if ($definition === null) {
                    $skippedInstances[] = $instance->id;
                    $this->warn("Skipping instance {$instance->id}; no migrated definition for widget {$legacyWidgetId}.");
                    continue;
                }

                $zone = $zoneMap[$instance->zone] ?? null;

                if ($zone === null) {
                    $skippedInstances[] = $instance->id;
                    $this->warn("Skipping instance {$instance->id}; no zone mapping for {$instance->zone}.");
                    continue;
                }

                $config = $instance->config_json;

                if (is_string($config)) {
                    $decoded = json_decode($config, true);
                    $config = json_last_error() === JSON_ERROR_NONE ? $decoded : null;
                }

                $payload = [
                    'block_definition_id' => $definition->id,
                    'zone' => $zone,
                    'position' => $instance->position,
                    'config' => $config,
                    'enabled' => (bool) $instance->enabled,
                ];

                if ($dryRun) {
                    $this->line(sprintf('Would migrate instance %d -> definition %d (%s) zone %s', $instance->id, $definition->id, $definition->title ?? $definition->name ?? 'n/a', $zone));
                } else {
                    BlockInstance::query()->updateOrCreate(
                        [
                            'zone' => $zone,
                            'position' => $instance->position,
                        ],
                        Arr::except($payload, ['zone', 'position'])
                    );
                }

                $migratedInstances++;
            }

            if ($dryRun) {
                DB::rollBack();
            } else {
                DB::commit();
            }

            if ($unmappedWidgets !== []) {
                Log::warning('Unmapped widget entrypoints encountered during migration.', [
                    'entrypoints' => array_values(array_unique($unmappedWidgets)),
                ]);
            }

            if ($dryRun) {
                $this->info('Dry-run completed. No data was modified.');
            } else {
                $this->info(sprintf('Migrated %d widget definitions and %d instances.', count($definitionMap), $migratedInstances));
            }

            if ($skippedInstances !== []) {
                Log::warning('Some widget instances were skipped during migration.', [
                    'instance_ids' => $skippedInstances,
                ]);
            }
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->error('Migration failed: '.$throwable->getMessage());

            return self::FAILURE;
        }

        return self::SUCCESS;
    }
}
