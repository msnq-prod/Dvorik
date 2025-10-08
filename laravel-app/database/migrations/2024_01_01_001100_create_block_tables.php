<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('block_definitions', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('module');
            $table->string('name');
            $table->unsignedInteger('version')->default(1);
            $table->string('title');
            $table->text('description')->nullable();
            $table->string('component');
            $table->json('config_schema')->nullable();
            $table->json('metadata')->nullable();
            $table->timestampsTz();

            $table->unique(['module', 'name', 'version'], 'uidx_block_definitions_identity');
        });

        Schema::create('block_instances', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->foreignId('block_definition_id')->constrained('block_definitions')->cascadeOnDelete();
            $table->string('zone');
            $table->integer('position')->default(0);
            $table->json('config')->nullable();
            $table->boolean('enabled')->default(true);
            $table->timestampsTz();

            $table->unique(['zone', 'position'], 'uidx_block_instances_zone_position');
            $table->index('enabled', 'idx_block_instances_enabled');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('block_instances');
        Schema::dropIfExists('block_definitions');
    }
};
