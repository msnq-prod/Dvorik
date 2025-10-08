<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('user_role', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->unsignedBigInteger('tg_id')->nullable();
            $table->string('username')->nullable();
            $table->string('display_name')->nullable();
            $table->enum('role', ['admin', 'seller']);
            $table->timestampTz('created_at')->useCurrent();

            $table->unique(['username', 'role']);
            $table->unique(['tg_id', 'role']);
        });

        Schema::create('user_notify', function (Blueprint $table) {
            $table->unsignedBigInteger('user_id');
            $table->enum('notif_type', ['zero', 'last', 'to_skl', 'new_type']);
            $table->enum('mode', ['off', 'daily', 'instant'])->default('off');
            $table->timestampTz('updated_at')->useCurrent();

            $table->primary(['user_id', 'notif_type']);
        });

        Schema::create('event_log', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->timestampTz('ts')->useCurrent();
            $table->string('event_type');
            $table->foreignId('product_id')->nullable()->constrained('product')->nullOnDelete();
            $table->string('location_code')->nullable();
            $table->unsignedBigInteger('user_id')->nullable();
            $table->decimal('delta', 15, 3)->nullable();
            $table->json('payload_json')->nullable();

            $table->foreign('location_code')->references('code')->on('location')->nullOnDelete();
            $table->index('ts', 'idx_event_log_ts');
            $table->index('event_type', 'idx_event_log_type');
        });

        Schema::create('registration_request', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->unsignedBigInteger('tg_id');
            $table->string('username')->nullable();
            $table->string('first_name')->nullable();
            $table->string('last_name')->nullable();
            $table->string('requested_role')->default('admin');
            $table->enum('status', ['pending', 'approved', 'declined', 'cancelled'])->default('pending');
            $table->timestampTz('created_at')->useCurrent();
        });

        Schema::create('audit_log', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->timestampTz('created_at')->useCurrent();
            $table->unsignedBigInteger('actor_id')->nullable();
            $table->string('actor_username')->nullable();
            $table->string('action');
            $table->string('entity')->nullable();
            $table->string('entity_id')->nullable();
            $table->json('payload_json')->nullable();

            $table->index('created_at', 'idx_audit_log_created');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('audit_log');
        Schema::dropIfExists('registration_request');
        Schema::dropIfExists('event_log');
        Schema::dropIfExists('user_notify');
        Schema::dropIfExists('user_role');
    }
};
