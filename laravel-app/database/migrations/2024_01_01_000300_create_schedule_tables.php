<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('schedule_day', function (Blueprint $table) {
            $table->date('date')->primary();
            $table->boolean('is_open')->default(true);
            $table->text('notes')->nullable();
        });

        Schema::create('schedule_assignment', function (Blueprint $table) {
            $table->date('date');
            $table->unsignedBigInteger('tg_id');
            $table->string('source')->default('auto');
            $table->timestampTz('created_at')->useCurrent();

            $table->primary(['date', 'tg_id']);
            $table->foreign('date')->references('date')->on('schedule_day')->cascadeOnDelete();
        });

        Schema::create('schedule_transfer_request', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->date('date');
            $table->unsignedBigInteger('from_tg_id');
            $table->unsignedBigInteger('to_tg_id');
            $table->enum('status', ['pending', 'accepted', 'declined', 'cancelled', 'expired'])->default('pending');
            $table->timestampTz('created_at')->useCurrent();
            $table->timestampTz('expires_at')->nullable();

            $table->unique(['date', 'from_tg_id', 'to_tg_id']);
            $table->foreign('date')->references('date')->on('schedule_day')->cascadeOnDelete();
        });

        Schema::create('schedule_anchor', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->date('start_date')->unique();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('schedule_anchor');
        Schema::dropIfExists('schedule_transfer_request');
        Schema::dropIfExists('schedule_assignment');
        Schema::dropIfExists('schedule_day');
    }
};
