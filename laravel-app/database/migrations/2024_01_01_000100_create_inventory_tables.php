<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('manufacturer', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('name')->unique();
            $table->string('country')->nullable();
            $table->timestampTz('created_at')->useCurrent();
        });

        Schema::create('supplier', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('name')->unique();
            $table->string('contact')->nullable();
            $table->timestampTz('created_at')->useCurrent();
        });

        Schema::create('product', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('article')->nullable();
            $table->string('barcode')->nullable();
            $table->string('name');
            $table->string('brand_country')->nullable();
            $table->string('local_name')->nullable();
            $table->text('description')->nullable();
            $table->string('unit')->nullable();
            $table->foreignId('manufacturer_id')->nullable()->constrained('manufacturer')->nullOnDelete();
            $table->decimal('price', 12, 2)->nullable();
            $table->decimal('vat_rate', 5, 2)->nullable();
            $table->boolean('is_new')->default(false);
            $table->boolean('archived')->default(false);
            $table->timestampTz('archived_at')->nullable();
            $table->timestampTz('created_at')->useCurrent();
            $table->timestampTz('updated_at')->nullable();
            $table->timestampTz('last_restock_at')->nullable();
            $table->string('photo_file_id')->nullable();
            $table->string('photo_path')->nullable();

            $table->index('article', 'idx_product_article');
            $table->index('name', 'idx_product_name');
        });

        Schema::create('location', function (Blueprint $table) {
            $table->string('code')->primary();
            $table->string('kind');
            $table->string('title');
            $table->timestampTz('created_at')->useCurrent();
        });

        Schema::create('stock', function (Blueprint $table) {
            $table->foreignId('product_id')->constrained('product')->cascadeOnDelete();
            $table->string('location_code');
            $table->decimal('qty_pack', 15, 3)->default(0);
            $table->string('name')->nullable();
            $table->string('local_name')->nullable();
            $table->decimal('reserved_pack', 15, 3)->default(0);
            $table->timestampTz('updated_at')->useCurrent();

            $table->primary(['product_id', 'location_code']);
            $table->foreign('location_code')->references('code')->on('location')->cascadeOnDelete();
            $table->index('location_code', 'idx_stock_location');
        });

        Schema::create('supplier_sku', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->foreignId('product_id')->constrained('product')->cascadeOnDelete();
            $table->foreignId('supplier_id')->constrained('supplier')->cascadeOnDelete();
            $table->string('code');
            $table->string('barcode')->nullable();
            $table->decimal('pack_qty', 15, 3)->nullable();
            $table->boolean('active')->default(true);
            $table->timestampTz('created_at')->useCurrent();
            $table->timestampTz('updated_at')->nullable();

            $table->unique(['supplier_id', 'code']);
            $table->index('product_id', 'idx_supplier_sku_product');
            $table->index(['supplier_id', 'active'], 'idx_supplier_sku_supplier_active');
        });

        Schema::create('display_name_exception', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('phrase')->unique();
            $table->timestampTz('created_at')->useCurrent();
        });

        Schema::create('import_log', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->string('original_name');
            $table->string('stored_path');
            $table->enum('import_type', ['csv', 'excel']);
            $table->string('source_hash')->unique();
            $table->text('normalized_csv')->nullable();
            $table->string('normalized_hash')->nullable()->unique();
            $table->string('supplier')->nullable();
            $table->string('invoice')->nullable();
            $table->unsignedInteger('items_count')->default(0);
            $table->json('items_json')->nullable();
            $table->timestampTz('reverted_at')->nullable();
            $table->timestampTz('created_at')->useCurrent();

            $table->index('created_at', 'idx_import_log_created');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('import_log');
        Schema::dropIfExists('display_name_exception');
        Schema::dropIfExists('supplier_sku');
        Schema::dropIfExists('stock');
        Schema::dropIfExists('location');
        Schema::dropIfExists('product');
        Schema::dropIfExists('supplier');
        Schema::dropIfExists('manufacturer');
    }
};
