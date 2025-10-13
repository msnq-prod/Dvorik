<?php

namespace Tests\Feature;

use App\Livewire\Blocks\FormBlock;
use App\Livewire\Blocks\MetricBlock;
use App\Livewire\Blocks\TableBlock;
use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use App\Models\QueryRegistry;
use App\Models\User;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Livewire\Livewire;
use Spatie\Permission\Models\Permission;
use Spatie\Permission\PermissionRegistrar;
use Tests\Fixtures\DummyFormHandler;
use Tests\TestCase;

class BlockRuntimeTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        /** @var PermissionRegistrar $registrar */
        $registrar = app(PermissionRegistrar::class);
        $registrar->forgetCachedPermissions();
    }

    public function test_table_block_renders_rows_and_exports_csv(): void
    {
        $user = User::factory()->create();
        $this->grantPermission($user, 'queries.manage');

        $productId = DB::table('product')->insertGetId([
            'article' => null,
            'barcode' => null,
            'name' => 'Widget Alpha',
            'brand_country' => null,
            'local_name' => null,
            'description' => null,
            'unit' => 'pcs',
            'manufacturer_id' => null,
            'price' => null,
            'vat_rate' => null,
            'is_new' => false,
            'archived' => false,
            'archived_at' => null,
            'updated_at' => now(),
            'last_restock_at' => null,
            'photo_file_id' => null,
            'photo_path' => null,
        ]);

        QueryRegistry::query()->updateOrCreate(
            ['key' => 'test.products'],
            [
                'sql' => <<<'SQL'
                    SELECT name
                    FROM product
                    WHERE :search IS NULL OR name LIKE '%' || :search || '%'
                    ORDER BY name ASC
                SQL,
                'description' => 'Lists products with optional search.',
            ]
        );

        $definition = BlockDefinition::query()->create([
            'module' => 'custom',
            'name' => 'products_table',
            'version' => 1,
            'title' => 'Products',
            'component' => TableBlock::class,
            'config_schema' => null,
        ]);

        $instance = BlockInstance::query()->create([
            'block_definition_id' => $definition->id,
            'zone' => 'test.zone',
            'position' => 0,
            'config' => [
                'title' => 'Products',
                'data_source' => 'test.products',
                'columns' => [
                    ['key' => 'name', 'label' => 'Name'],
                ],
                'search' => [
                    'parameter' => 'search',
                    'label' => 'Search products',
                    'default' => '',
                ],
                'per_page' => 10,
                'export' => [
                    'enabled' => true,
                    'max_rows' => 50,
                    'filename' => 'products.csv',
                ],
            ],
            'enabled' => true,
        ]);

        $instance->setRelation('definition', $definition);

        Livewire::actingAs($user)
            ->test(TableBlock::class, [
                'block' => $instance,
                'definition' => $definition,
                'config' => $instance->config,
            ])
            ->assertSee('Products')
            ->assertSee('Widget Alpha')
            ->call('export')
            ->assertFileDownloaded('products.csv');
    }

    public function test_metric_block_uses_query_registry_data_source(): void
    {
        $user = User::factory()->create();
        $this->grantPermission($user, 'queries.manage');

        $locationCode = 'A1';

        DB::table('location')->insert([
            'code' => $locationCode,
            'kind' => 'WAREHOUSE',
            'title' => 'Main Warehouse',
            'created_at' => now(),
        ]);

        $productId = DB::table('product')->insertGetId([
            'article' => null,
            'barcode' => null,
            'name' => 'Widget Beta',
            'brand_country' => null,
            'local_name' => null,
            'description' => null,
            'unit' => 'pcs',
            'manufacturer_id' => null,
            'price' => null,
            'vat_rate' => null,
            'is_new' => false,
            'archived' => false,
            'archived_at' => null,
            'updated_at' => now(),
            'last_restock_at' => null,
            'photo_file_id' => null,
            'photo_path' => null,
        ]);

        DB::table('stock')->insert([
            'product_id' => $productId,
            'location_code' => $locationCode,
            'qty_pack' => 3,
            'name' => null,
            'local_name' => null,
            'reserved_pack' => 0,
            'updated_at' => now(),
        ]);

        $this->seed(\Database\Seeders\QueryRegistrySeeder::class);

        $definition = BlockDefinition::query()->create([
            'module' => 'custom',
            'name' => 'metric_stock',
            'version' => 1,
            'title' => 'Stock by location',
            'component' => MetricBlock::class,
            'config_schema' => null,
        ]);

        $config = [
            'title' => 'Stock by location',
            'data_source' => 'metrics.stock_by_location',
            'limit' => 10,
            'parameters' => ['location_code' => null],
            'columns' => [
                ['key' => 'location_title', 'label' => 'Location'],
                ['key' => 'product_name', 'label' => 'Product'],
                ['key' => 'qty_pack', 'label' => 'Quantity'],
            ],
        ];

        $instance = BlockInstance::query()->create([
            'block_definition_id' => $definition->id,
            'zone' => 'test.zone',
            'position' => 0,
            'config' => $config,
            'enabled' => true,
        ]);

        $instance->setRelation('definition', $definition);

        Livewire::actingAs($user)
            ->test(MetricBlock::class, [
                'block' => $instance,
                'definition' => $definition,
                'config' => $config,
            ])
            ->assertSee('Stock by location')
            ->assertSee('Main Warehouse')
            ->assertSee('Widget Beta');
    }

    public function test_form_block_invokes_handler_and_logs_audit(): void
    {
        $user = User::factory()->create();
        $this->grantPermission($user, 'widgets.manage');

        $handler = new DummyFormHandler();
        $this->app->instance(DummyFormHandler::class, $handler);

        $definition = BlockDefinition::query()->create([
            'module' => 'custom',
            'name' => 'form_demo',
            'version' => 1,
            'title' => 'Demo form',
            'component' => FormBlock::class,
            'config_schema' => null,
        ]);

        $config = [
            'title' => 'Create note',
            'permission' => 'widgets.manage',
            'handler' => DummyFormHandler::class,
            'success_message' => 'Saved successfully.',
            'fields' => [
                [
                    'name' => 'subject',
                    'label' => 'Subject',
                    'type' => 'text',
                    'rules' => ['required', 'string'],
                ],
            ],
        ];

        $instance = BlockInstance::query()->create([
            'block_definition_id' => $definition->id,
            'zone' => 'test.zone',
            'position' => 0,
            'config' => $config,
            'enabled' => true,
        ]);

        $instance->setRelation('definition', $definition);

        Livewire::actingAs($user)
            ->test(FormBlock::class, [
                'block' => $instance,
                'definition' => $definition,
                'config' => $config,
            ])
            ->set('formData.subject', 'Hello world')
            ->call('submit')
            ->assertSee('Saved successfully.');

        $this->assertNotEmpty($handler->calls);

        $this->assertDatabaseHas('audit_log', [
            'action' => 'form_block.submit',
            'entity' => $definition->handle,
        ]);
    }

    public function test_block_preview_endpoint_renders_block_html(): void
    {
        $user = User::factory()->create();
        $this->grantPermission($user, 'queries.manage');

        QueryRegistry::query()->updateOrCreate(
            ['key' => 'preview.test'],
            [
                'sql' => 'SELECT 1 AS value',
                'description' => 'Preview dataset',
            ]
        );

        $definition = BlockDefinition::query()->create([
            'module' => 'custom',
            'name' => 'preview_table',
            'version' => 1,
            'title' => 'Preview Table',
            'component' => TableBlock::class,
            'config_schema' => null,
        ]);

        $instance = BlockInstance::query()->create([
            'block_definition_id' => $definition->id,
            'zone' => 'test.zone',
            'position' => 0,
            'config' => [
                'data_source' => 'preview.test',
                'columns' => [['key' => 'value', 'label' => 'Value']],
            ],
            'enabled' => true,
        ]);

        $response = $this->actingAs($user)
            ->postJson(route('blocks.preview'), [
                'definition_id' => $definition->id,
                'config' => $instance->config,
            ]);

        $response->assertOk();
        $response->assertJson(['ok' => true]);
        $this->assertStringContainsString('Value', $response->json('html'));
    }

    private function grantPermission(User $user, string $permission): void
    {
        $perm = Permission::query()->firstOrCreate([
            'name' => $permission,
            'guard_name' => 'filament',
        ]);

        $user->givePermissionTo($perm);
    }
}
