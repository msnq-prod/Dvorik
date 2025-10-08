<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use PDO;
use PDOException;
use RuntimeException;
use Throwable;

class ImportLegacySqlite extends Command
{
    protected $signature = 'legacy:ingest-sqlite {path : Path to the legacy SQLite database}';

    protected $description = 'Ingest data from the legacy SQLite database, validating row counts.';

    /** @var string[] */
    private array $tableOrder = [
        'manufacturer',
        'supplier',
        'product',
        'location',
        'stock',
        'supplier_sku',
        'display_name_exception',
        'import_log',
        'product_merge_log',
        'product_article_alias',
        'product_name_alias',
        'product_merge_rule',
        'schedule_day',
        'schedule_assignment',
        'schedule_transfer_request',
        'schedule_anchor',
        'user_role',
        'user_notify',
        'event_log',
        'registration_request',
        'query_registry',
        'ui_widget',
        'ui_widget_instance',
        'ui_menu',
        'scheduled_job',
        'audit_log',
    ];

    public function handle(): int
    {
        $path = (string) $this->argument('path');

        if (! is_file($path)) {
            $this->error("Legacy database not found at {$path}");
            return self::FAILURE;
        }

        try {
            $sqlite = new PDO('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        } catch (PDOException $exception) {
            $this->error('Unable to open legacy SQLite database: '.$exception->getMessage());
            return self::FAILURE;
        }

        DB::beginTransaction();

        try {

            foreach ($this->tableOrder as $table) {
                $legacyCount = (int) $sqlite->query("SELECT COUNT(*) FROM \"{$table}\"")->fetchColumn();
                $this->info("Importing {$table} ({$legacyCount} rows)...");

                DB::statement(sprintf('TRUNCATE TABLE "%s" RESTART IDENTITY CASCADE', $table));

                $stmt = $sqlite->query("SELECT * FROM \"{$table}\"");
                $stmt->setFetchMode(PDO::FETCH_ASSOC);

                $batch = [];
                $batchSize = 500;

                while (($row = $stmt->fetch()) !== false) {
                    $batch[] = $this->transformRow($table, $row);

                    if (count($batch) >= $batchSize) {
                        $this->insertBatch($table, $batch);
                        $batch = [];
                    }
                }

                if ($batch !== []) {
                    $this->insertBatch($table, $batch);
                }

                $currentCount = (int) DB::table($table)->count();

                if ($currentCount !== $legacyCount) {
                    throw new RuntimeException("Row count mismatch for {$table}: legacy={$legacyCount}, imported={$currentCount}");
                }
            }

            DB::commit();
            $this->info('Legacy SQLite import completed successfully.');
        } catch (Throwable $throwable) {
            DB::rollBack();
            $this->error('Import failed: '.$throwable->getMessage());
            return self::FAILURE;
        } finally {
            $sqlite = null;
        }

        return self::SUCCESS;
    }

    /**
     * @param array<string, mixed> $rows
     */
    private function insertBatch(string $table, array $rows): void
    {
        if ($rows === []) {
            return;
        }

        DB::table($table)->insert($rows);
    }

    /**
     * @param array<string, mixed> $row
     * @return array<string, mixed>
     */
    private function transformRow(string $table, array $row): array
    {
        return match ($table) {
            'product' => $this->castBooleanColumns($row, ['is_new', 'archived']),
            'supplier_sku' => $this->castBooleanColumns($row, ['active']),
            'product_merge_rule' => $this->castBooleanColumns($row, ['active']),
            'schedule_day' => $this->castBooleanColumns($row, ['is_open']),
            'ui_widget_instance' => $this->castBooleanColumns($row, ['enabled']),
            'ui_menu' => $this->castBooleanColumns($row, ['visible']),
            'scheduled_job' => $this->castBooleanColumns($row, ['enabled']),
            default => $row,
        };
    }

    /**
     * @param array<string, mixed> $row
     * @param string[] $columns
     * @return array<string, mixed>
     */
    private function castBooleanColumns(array $row, array $columns): array
    {
        foreach ($columns as $column) {
            if (array_key_exists($column, $row)) {
                $value = $row[$column];
                $row[$column] = $value === null ? null : (bool) ((int) $value);
            }
        }

        return $row;
    }
}
