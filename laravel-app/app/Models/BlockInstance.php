<?php

namespace App\Models;

use App\Services\BlockRuntime;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class BlockInstance extends Model
{
    use HasFactory;

    protected $guarded = [];

    protected $casts = [
        'config' => 'array',
        'enabled' => 'boolean',
    ];

    /**
     * @return BelongsTo<BlockDefinition, BlockInstance>
     */
    public function definition(): BelongsTo
    {
        return $this->belongsTo(BlockDefinition::class);
    }

    protected static function booted(): void
    {
        static::saved(function (BlockInstance $instance): void {
            app(BlockRuntime::class)->forgetZoneCache($instance->zone);
        });

        static::deleted(function (BlockInstance $instance): void {
            app(BlockRuntime::class)->forgetZoneCache($instance->zone);
        });
    }
}
