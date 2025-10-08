<?php

namespace App\Services;

use App\Exceptions\InvalidBlockConfigurationException;
use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use Livewire\Livewire;
use Opis\JsonSchema\ValidationResult;
use Opis\JsonSchema\Validator;
use RuntimeException;
use Throwable;

class BlockRuntime
{
    private const DEFINITIONS_CACHE_KEY = 'block-runtime:definitions';
    private const ZONE_CACHE_PREFIX = 'block-runtime:zone:';

    private Validator $validator;

    public function __construct(
        private readonly CacheRepository $cache,
        ?Validator $validator = null,
    ) {
        $this->validator = $validator ?? new Validator();
    }

    public function warmDefinitionCache(): void
    {
        $this->definitions();
    }

    public function forgetDefinitionCache(): void
    {
        $this->cache->forget(self::DEFINITIONS_CACHE_KEY);
    }

    public function forgetZoneCache(string $zone): void
    {
        $this->cache->forget($this->zoneCacheKey($zone, true));
        $this->cache->forget($this->zoneCacheKey($zone, false));
    }

    public function getDefinition(string $module, string $name, ?int $version = null): ?BlockDefinition
    {
        $key = $this->definitionKey($module, $name);
        $definitions = $this->definitions();

        if (! array_key_exists($key, $definitions)) {
            return null;
        }

        $payload = $definitions[$key];

        if ($version === null) {
            $version = array_key_first($payload);
        }

        if ($version === null || ! array_key_exists($version, $payload)) {
            return null;
        }

        return $this->rehydrateDefinition($payload[$version]);
    }

    public function getDefinitionById(int $id): ?BlockDefinition
    {
        foreach ($this->definitions() as $versions) {
            foreach ($versions as $definition) {
                if ((int) ($definition['id'] ?? 0) === $id) {
                    return $this->rehydrateDefinition($definition);
                }
            }
        }

        return BlockDefinition::find($id);
    }

    /**
     * @return Collection<int, BlockInstance>
     */
    public function getZoneInstances(string $zone, bool $onlyEnabled = true): Collection
    {
        $payload = $this->cache->remember(
            $this->zoneCacheKey($zone, $onlyEnabled),
            now()->addSeconds((int) config('block-runtime.zone_cache_ttl', 60)),
            function () use ($zone, $onlyEnabled): array {
                $query = BlockInstance::query()
                    ->with('definition')
                    ->where('zone', $zone)
                    ->orderBy('position');

                if ($onlyEnabled) {
                    $query->where('enabled', true);
                }

                return $query->get()
                    ->map(fn (BlockInstance $instance): array => [
                        'instance' => $instance->attributesToArray(),
                        'definition' => $instance->definition?->attributesToArray(),
                    ])
                    ->all();
            }
        );

        return collect($payload)
            ->map(function (array $payload): BlockInstance {
                $instance = new BlockInstance();
                $instance->forceFill($payload['instance']);
                $instance->exists = true;

                $definitionAttributes = $payload['definition'] ?? null;

                if ($definitionAttributes !== null) {
                    $instance->setRelation('definition', $this->rehydrateDefinition($definitionAttributes));
                }

                return $instance;
            });
    }

    public function renderZone(string $zone, bool $onlyEnabled = true): string
    {
        return $this->getZoneInstances($zone, $onlyEnabled)
            ->map(fn (BlockInstance $instance): ?string => $this->renderInstance($instance))
            ->filter()
            ->implode('');
    }

    public function renderInstance(BlockInstance $instance): ?string
    {
        $definition = $instance->definition;

        if ($definition === null) {
            $definition = $this->getDefinitionById($instance->block_definition_id);
        }

        if (! $definition instanceof BlockDefinition) {
            Log::warning('Block definition missing for instance.', [
                'instance_id' => $instance->id,
                'block_definition_id' => $instance->block_definition_id,
            ]);

            return null;
        }

        $config = $instance->config ?? [];

        try {
            $this->validateConfig($definition, $config);
        } catch (InvalidBlockConfigurationException $exception) {
            Log::warning('Block configuration validation failed.', [
                'instance_id' => $instance->id,
                'block_definition_id' => $instance->block_definition_id,
                'errors' => $exception->getErrors(),
            ]);

            return null;
        }

        $component = $definition->component;

        if (! class_exists($component)) {
            Log::warning('Block component class is missing.', [
                'component' => $component,
                'definition_id' => $definition->id,
            ]);

            return null;
        }

        try {
            $mount = Livewire::mount($component, [
                'block' => $instance,
                'definition' => $definition,
                'config' => $config,
            ]);

            return method_exists($mount, 'html') ? $mount->html() : (string) $mount;
        } catch (Throwable $throwable) {
            Log::error('Unable to mount block component.', [
                'component' => $component,
                'definition_id' => $definition->id,
                'instance_id' => $instance->id,
                'exception' => $throwable,
            ]);

            return null;
        }
    }

    public function validateConfig(BlockDefinition $definition, array $config): void
    {
        $schema = $definition->config_schema;

        if ($schema === null) {
            return;
        }

        $schemaObject = $this->jsonify($schema);
        $configObject = $this->jsonify($config);

        try {
            $result = $this->validator->validate($configObject, $schemaObject);
        } catch (Throwable $throwable) {
            throw new RuntimeException('Unable to validate block configuration: '.$throwable->getMessage(), 0, $throwable);
        }

        if ($result->isValid()) {
            return;
        }

        throw new InvalidBlockConfigurationException($this->buildErrorMessages($result));
    }

    /**
     * @return array<string, mixed>
     */
    private function buildErrorMessages(ValidationResult $result): array
    {
        $error = $result->error();

        if ($error === null) {
            return [];
        }

        $messages = [];
        $queue = [$error];

        while ($queue !== []) {
            $current = array_shift($queue);
            $dataPointer = $current->dataPointer() ?? '';
            $message = $current->message() ?? 'Validation error.';
            $messages[] = trim(sprintf('%s: %s', $dataPointer === '' ? '$' : $dataPointer, $message));

            foreach ($current->subErrors() ?? [] as $subError) {
                $queue[] = $subError;
            }
        }

        return $messages;
    }

    private function jsonify(array|string|null $value): mixed
    {
        if ($value === null) {
            return null;
        }

        if (is_string($value)) {
            $decoded = json_decode($value);

            if (json_last_error() === JSON_ERROR_NONE) {
                return $decoded;
            }

            throw new RuntimeException('Invalid JSON string encountered while preparing schema or config.');
        }

        $encoded = json_encode($value);

        if ($encoded === false) {
            throw new RuntimeException('Unable to encode value as JSON.');
        }

        $decoded = json_decode($encoded);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new RuntimeException('Unable to decode JSON: '.json_last_error_msg());
        }

        return $decoded;
    }

    /**
     * @return array<string, array<int, array<string, mixed>>>
     */
    private function definitions(): array
    {
        return $this->cache->rememberForever(self::DEFINITIONS_CACHE_KEY, function (): array {
            return BlockDefinition::query()
                ->orderBy('module')
                ->orderBy('name')
                ->orderByDesc('version')
                ->get()
                ->map(fn (BlockDefinition $definition): array => $definition->attributesToArray())
                ->reduce(function (array $carry, array $definition): array {
                    $key = $this->definitionKey($definition['module'], $definition['name']);
                    $version = (int) $definition['version'];

                    if (! isset($carry[$key])) {
                        $carry[$key] = [];
                    }

                    $carry[$key][$version] = $definition;

                    return $carry;
                }, []);
        });
    }

    /**
     * @param array<string, mixed> $attributes
     */
    private function rehydrateDefinition(array $attributes): BlockDefinition
    {
        $definition = new BlockDefinition();
        $definition->forceFill($attributes);
        $definition->exists = true;

        return $definition;
    }

    private function definitionKey(string $module, string $name): string
    {
        return Str::of($module)->lower().':'.Str::of($name)->lower();
    }

    private function zoneCacheKey(string $zone, bool $onlyEnabled): string
    {
        return self::ZONE_CACHE_PREFIX.Str::lower($zone).($onlyEnabled ? ':enabled' : ':all');
    }
}
