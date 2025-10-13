<?php

namespace App\Livewire\Blocks;

use App\DataSources\DataSourceManager;
use App\DataSources\Exceptions\DataSourceExecutionException;
use App\DataSources\Exceptions\DataSourceNotFoundException;
use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use App\Models\QueryRegistry;
use Illuminate\Contracts\View\View;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Gate;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use Illuminate\Validation\ValidationException;
use Livewire\Attributes\Locked;
use Livewire\Component;
use Livewire\WithPagination;
use Symfony\Component\HttpFoundation\StreamedResponse;

class TableBlock extends Component
{
    use WithPagination;

    #[Locked]
    public BlockInstance $block;

    #[Locked]
    public BlockDefinition $definition;

    /** @var array<string, mixed> */
    #[Locked]
    public array $config = [];

    /** @var array<string, mixed> */
    public array $filters = [];

    public string $sortField = '';

    public string $sortDirection = 'asc';

    public ?string $search = null;

    private DataSourceManager $sources;

    /**
     * @var array<string, mixed>
     */
    private array $defaultParameters = [];

    /**
     * @var array<string, array<string, mixed>>
     */
    private array $filterDefinitions = [];

    private ?DataSourceResponse $cachedResponse = null;

    public function boot(DataSourceManager $sources): void
    {
        $this->sources = $sources;
    }

    public function mount(BlockInstance $block, BlockDefinition $definition, array $config = []): void
    {
        $this->block = $block;
        $this->definition = $definition;
        $this->config = $config;

        $this->defaultParameters = Arr::get($config, 'default_parameters', []);
        $this->filterDefinitions = collect(Arr::get($config, 'filters', []))
            ->mapWithKeys(function (array $filter): array {
                $key = (string) ($filter['key'] ?? $filter['parameter'] ?? Str::uuid()->toString());

                return [$key => $filter];
            })
            ->all();

        foreach ($this->filterDefinitions as $key => $filter) {
            $default = Arr::get($filter, 'default');
            $this->filters[$key] = $this->defaultParameters[$filter['parameter'] ?? $key] ?? $default;
        }

        $sortConfig = Arr::get($config, 'sort', []);
        $this->sortField = (string) Arr::get($sortConfig, 'key', '');
        $this->sortDirection = Str::lower((string) Arr::get($sortConfig, 'direction', 'asc')) === 'desc'
            ? 'desc'
            : 'asc';

        $this->search = Arr::get($config, 'search.default');
    }

    public function updatingFilters(): void
    {
        $this->cachedResponse = null;
        $this->resetPage();
    }

    public function updatingSearch(): void
    {
        $this->cachedResponse = null;
        $this->resetPage();
    }

    public function render(): View
    {
        return view('livewire.blocks.table-block', [
            'title' => Arr::get($this->config, 'title'),
            'description' => Arr::get($this->config, 'description'),
            'columns' => $this->columns(),
            'data' => $this->dataSourceResponse()->paginator,
            'exportable' => (bool) Arr::get($this->config, 'export.enabled', false),
            'filterDefinitions' => $this->filterDefinitions,
            'filterState' => $this->filters,
            'config' => $this->config,
        ]);
    }

    public function export(): StreamedResponse
    {
        $exportConfig = Arr::get($this->config, 'export', []);

        if (! Arr::get($exportConfig, 'enabled', false)) {
            abort(404);
        }

        $maxRows = (int) Arr::get($exportConfig, 'max_rows', 1000);
        $filename = (string) Arr::get($exportConfig, 'filename', 'export.csv');

        $response = $this->executeDataSource($maxRows, 1);

        $columns = $response->columns();
        $rows = $response->rows->take($maxRows);

        return response()->streamDownload(function () use ($columns, $rows): void {
            $handle = fopen('php://output', 'wb');

            if ($handle === false) {
                return;
            }

            if ($columns !== []) {
                fputcsv($handle, $columns);
            }

            foreach ($rows as $row) {
                fputcsv($handle, Arr::only($row, $columns));
            }

            fclose($handle);
        }, $filename);
    }

    private function columns(): array
    {
        $columns = Arr::get($this->config, 'columns', []);

        if ($columns !== []) {
            return array_map(function ($column) {
                return [
                    'key' => (string) Arr::get($column, 'key'),
                    'label' => (string) Arr::get($column, 'label', Arr::get($column, 'key', '')),
                ];
            }, $columns);
        }

        return array_map(
            fn (string $column): array => ['key' => $column, 'label' => Str::headline($column)],
            $this->dataSourceResponse()->columns()
        );
    }

    private function dataSourceResponse(): DataSourceResponse
    {
        if ($this->cachedResponse === null) {
            $perPage = (int) Arr::get($this->config, 'per_page', 10);
            $this->cachedResponse = $this->executeDataSource($perPage, $this->getPage());
        }

        return $this->cachedResponse;
    }

    private function executeDataSource(int $perPage, int $page): DataSourceResponse
    {
        $key = (string) Arr::get($this->config, 'data_source');

        if ($key === '') {
            throw ValidationException::withMessages([
                'config.data_source' => __('Table block configuration is missing the data source key.'),
            ]);
        }

        $query = QueryRegistry::query()->find($key);

        if ($query instanceof QueryRegistry) {
            Gate::authorize('view', $query);
        }

        $parameters = $this->resolveParameters();

        try {
            return $this->sources->execute($key, $parameters, $perPage, $page);
        } catch (DataSourceNotFoundException $exception) {
            throw ValidationException::withMessages([
                'config.data_source' => $exception->getMessage(),
            ]);
        } catch (DataSourceExecutionException $exception) {
            Log::warning('Failed to execute table block data source.', [
                'block_id' => $this->block->id,
                'definition_id' => $this->definition->id,
                'errors' => $exception->getErrors(),
            ]);

            throw ValidationException::withMessages([
                'data_source' => $exception->getErrors(),
            ]);
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function resolveParameters(): array
    {
        $parameters = $this->defaultParameters;

        foreach ($this->filters as $key => $value) {
            $definition = $this->filterDefinitions[$key] ?? null;

            if ($definition === null) {
                continue;
            }

            $parameter = (string) Arr::get($definition, 'parameter', $key);
            $parameters[$parameter] = $value;
        }

        if ($this->search !== null && $this->search !== '') {
            $searchKey = (string) Arr::get($this->config, 'search.parameter', 'search');
            $parameters[$searchKey] = $this->search;
        }

        if ($this->sortField !== '') {
            $parameters[Arr::get($this->config, 'sort.parameter', 'sort_key')] = $this->sortField;
            $parameters[Arr::get($this->config, 'sort.direction_parameter', 'sort_direction')] = $this->sortDirection;
        }

        return $parameters;
    }

    protected function getPaginationView(): string
    {
        return 'pagination::tailwind';
    }
}
