<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('query_registry', function (Blueprint $table) {
            $table->string('key')->primary();
            $table->text('sql');
            $table->text('description')->nullable();
            $table->timestampTz('updated_at')->useCurrent();
        });

        Schema::create('ui_widget', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('module');
            $table->string('name');
            $table->string('title');
            $table->text('description')->nullable();
            $table->string('entrypoint')->nullable();
            $table->json('config_schema')->nullable();

            $table->unique(['module', 'name']);
        });

        Schema::create('ui_widget_instance', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->foreignId('widget_id')->constrained('ui_widget')->cascadeOnDelete();
            $table->string('zone');
            $table->integer('position')->default(0);
            $table->json('config_json')->nullable();
            $table->boolean('enabled')->default(true);

            $table->unique(['zone', 'position']);
        });

        Schema::create('ui_menu', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('slug')->unique();
            $table->string('title');
            $table->string('url')->nullable();
            $table->string('icon')->nullable();
            $table->foreignId('parent_id')->nullable()->constrained('ui_menu')->cascadeOnDelete();
            $table->integer('position')->default(0);
            $table->string('target')->nullable();
            $table->string('required_role')->nullable();
            $table->boolean('visible')->default(true);

            $table->index('required_role');
        });

        Schema::create('scheduled_job', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('name')->unique();
            $table->enum('schedule_type', ['daily', 'cron']);
            $table->string('schedule_expression')->nullable();
            $table->timestampTz('next_run_at')->nullable();
            $table->timestampTz('last_run_at')->nullable();
            $table->string('task_module');
            $table->string('task_name');
            $table->json('config_json')->nullable();
            $table->boolean('enabled')->default(true);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('scheduled_job');
        Schema::dropIfExists('ui_menu');
        Schema::dropIfExists('ui_widget_instance');
        Schema::dropIfExists('ui_widget');
        Schema::dropIfExists('query_registry');
    }
};
