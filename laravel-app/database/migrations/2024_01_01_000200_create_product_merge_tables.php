<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('product_merge_log', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->timestampTz('created_at')->useCurrent();
            $table->foreignId('source_a_id')->constrained('product');
            $table->foreignId('source_b_id')->constrained('product');
            $table->foreignId('result_id')->constrained('product');
            $table->json('field_modes');
            $table->string('stock_mode');
            $table->text('summary')->nullable();
            $table->json('changes_json');
            $table->timestampTz('reverted_at')->nullable();
        });

        Schema::create('product_article_alias', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->foreignId('product_id')->constrained('product');
            $table->string('alias_article')->unique();
            $table->foreignId('source_product_id')->nullable()->constrained('product');
            $table->foreignId('merge_log_id')->nullable()->constrained('product_merge_log');
            $table->timestampTz('created_at')->useCurrent();

            $table->index('product_id');
        });

        Schema::create('product_name_alias', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->foreignId('product_id')->constrained('product');
            $table->string('alias_name');
            $table->string('normalized_name')->unique();
            $table->foreignId('source_product_id')->nullable()->constrained('product');
            $table->foreignId('merge_log_id')->nullable()->constrained('product_merge_log');
            $table->timestampTz('created_at')->useCurrent();

            $table->index('product_id');
        });

        Schema::create('product_merge_rule', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->foreignId('result_id')->constrained('product');
            $table->json('field_modes');
            $table->string('stock_mode');
            $table->json('articles_json')->nullable();
            $table->json('names_json')->nullable();
            $table->foreignId('merge_log_id')->nullable()->constrained('product_merge_log');
            $table->boolean('active')->default(true);
            $table->timestampTz('created_at')->useCurrent();
            $table->timestampTz('reverted_at')->nullable();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('product_merge_rule');
        Schema::dropIfExists('product_name_alias');
        Schema::dropIfExists('product_article_alias');
        Schema::dropIfExists('product_merge_log');
    }
};
