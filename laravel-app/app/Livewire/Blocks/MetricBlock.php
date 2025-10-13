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
use Illuminate\Validation\ValidationException;
use Livewire\Attributes\Locked;
use Livewire\Component;

class MetricBlock extends Component
{
    #[Locked]
    public BlockInstance $block;

    #[Locked]
    public BlockDefinition $definition;

    /** @var array<string, mixed> */
    #[Locked]
    public array $config = [];

    private DataSourceManager $sources;

    /**
     * @var array<int, array<string, mixed>>|null
     */
    private ?array $dataset = null;

    public function boot(DataSourceManager $sources): void
    {
        $this->sources = $sources;
    }

    public function mount(BlockInstance $block, BlockDefinition $definition, array $config = []): void
    {
        $this->block = $block;
        $this->definition = $definition;
        $this->config = $config;
    }

    public function render(): View
    {
        return view('livewire.blocks.metric-block', [
            'title' => Arr::get($this->config, 'title', $this->definition->title),
            'description' => Arr::get($this->config, 'description', $this->definition->description),
            'dataset' => $this->dataset(),
            'columns' => Arr::get($this->config, 'columns', []),
            'empty' => Arr::get($this->config, 'empty_message', __('No data available.')),
        ]);
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function dataset(): array
    {
        if ($this->dataset !== null) {
            return $this->dataset;
        }

        $key = (string) Arr::get($this->config, 'data_source');

        if ($key === '') {
            throw ValidationException::withMessages([
                'config.data_source' => __('Metric block configuration is missing the data source key.'),
            ]);
        }

        $query = QueryRegistry::query()->find($key);

        if ($query instanceof QueryRegistry) {
            Gate::authorize('view', $query);
        }

        $parameters = Arr::get($this->config, 'parameters', []);
        $limit = (int) Arr::get($this->config, 'limit', 25);

        try {
            $response = $this->sources->execute($key, $parameters, $limit, 1);
        } catch (DataSourceNotFoundException $exception) {
            throw ValidationException::withMessages([
                'config.data_source' => $exception->getMessage(),
            ]);
        } catch (DataSourceExecutionException $exception) {
            Log::warning('Metric data source execution failed.', [
                'block_id' => $this->block->id,
                'definition_id' => $this->definition->id,
                'errors' => $exception->getErrors(),
            ]);

            throw ValidationException::withMessages([
                'data_source' => $exception->getErrors(),
            ]);
        }

        return $this->dataset = $response->rows->take($limit)->map(fn ($row) => $row)->values()->all();
    }
}
