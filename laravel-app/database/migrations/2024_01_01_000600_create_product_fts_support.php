<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('product_fts', function (Blueprint $table) {
            $table->unsignedBigInteger('product_id')->primary();
            $table->foreign('product_id')->references('id')->on('product')->cascadeOnDelete();
        });

        DB::statement("ALTER TABLE product_fts ADD COLUMN search_vector tsvector NOT NULL DEFAULT ''::tsvector");
        DB::statement("CREATE INDEX product_fts_search_vector_gin ON product_fts USING GIN (search_vector)");

        DB::statement(<<<'SQL'
CREATE OR REPLACE FUNCTION product_fts_refresh() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM product_fts WHERE product_id = OLD.id;
        RETURN OLD;
    END IF;

    INSERT INTO product_fts (product_id, search_vector)
    VALUES (
        NEW.id,
        to_tsvector('simple',
            coalesce(NEW.article, '') || ' ' ||
            coalesce(NEW.name, '') || ' ' ||
            coalesce(NEW.local_name, '')
        )
    )
    ON CONFLICT (product_id) DO UPDATE
    SET search_vector = EXCLUDED.search_vector;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
SQL);

        DB::statement("CREATE TRIGGER product_fts_ai AFTER INSERT ON product FOR EACH ROW EXECUTE FUNCTION product_fts_refresh()");
        DB::statement("CREATE TRIGGER product_fts_au AFTER UPDATE ON product FOR EACH ROW EXECUTE FUNCTION product_fts_refresh()");
        DB::statement("CREATE TRIGGER product_fts_ad AFTER DELETE ON product FOR EACH ROW EXECUTE FUNCTION product_fts_refresh()");
    }

    public function down(): void
    {
        DB::statement("DROP TRIGGER IF EXISTS product_fts_ai ON product");
        DB::statement("DROP TRIGGER IF EXISTS product_fts_au ON product");
        DB::statement("DROP TRIGGER IF EXISTS product_fts_ad ON product");
        DB::statement("DROP FUNCTION IF EXISTS product_fts_refresh()");
        DB::statement("DROP INDEX IF EXISTS product_fts_search_vector_gin");

        Schema::dropIfExists('product_fts');
    }
};
