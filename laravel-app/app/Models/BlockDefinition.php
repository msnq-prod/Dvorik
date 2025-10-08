<?php

namespace App\Models;

use App\Services\BlockRuntime;
use App\Models\BlockInstance;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Support\Arr;

class BlockDefinition extends Model
{
    use HasFactory;

    protected $guarded = [];

    protected $casts = [
        'config_schema' => 'array',
        'metadata' => 'array',
        'version' => 'integer',
    ];

    /**
     * @return HasMany<BlockInstance>
     */
    public function instances(): HasMany
    {
        return $this->hasMany(BlockInstance::class);
    }

    protected static function booted(): void
    {
        static::saved(function (BlockDefinition $definition): void {
            $runtime = app(BlockRuntime::class);
            $runtime->forgetDefinitionCache();

            $zones = $definition->instances()->pluck('zone')->all();

            foreach ($zones as $zone) {
                $runtime->forgetZoneCache($zone);
            }
        });

        static::deleting(function (BlockDefinition $definition): void {
            $runtime = app(BlockRuntime::class);

            $zones = BlockInstance::query()
                ->where('block_definition_id', $definition->id)
                ->pluck('zone')
                ->all();

            foreach ($zones as $zone) {
                $runtime->forgetZoneCache($zone);
            }

            $runtime->forgetDefinitionCache();
        });

        static::deleted(function (): void {
            app(BlockRuntime::class)->forgetDefinitionCache();
        });
    }

    public function getHandleAttribute(): string
    {
        return Arr::join([$this->module, $this->name, $this->version], ':');
    }
}
