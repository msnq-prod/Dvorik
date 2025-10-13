<div class="space-y-4" wire:key="table-block-{{ $block->id ?? 'preview' }}">
    @if($title)
        <div>
            <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100">{{ $title }}</h3>
            @if($description)
                <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">{{ $description }}</p>
            @endif
        </div>
    @endif

    <div class="flex flex-wrap gap-2 items-end">
        @foreach($filterDefinitions as $key => $filter)
            <div class="flex flex-col">
                <label for="filter-{{ $key }}" class="text-sm font-medium text-gray-700 dark:text-gray-200">
                    {{ $filter['label'] ?? \Illuminate\Support\Str::headline($filter['parameter'] ?? $key) }}
                </label>
                <input
                    id="filter-{{ $key }}"
                    type="text"
                    class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 dark:bg-gray-800 dark:border-gray-700"
                    wire:model.live="filters.{{ $key }}"
                    >
            </div>
        @endforeach

        @if(isset($config['search']))
            <div class="flex flex-col">
                <label for="table-search" class="text-sm font-medium text-gray-700 dark:text-gray-200">
                    {{ $config['search']['label'] ?? __('Search') }}
                </label>
                <input
                    id="table-search"
                    type="search"
                    class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 dark:bg-gray-800 dark:border-gray-700"
                    wire:model.live="search"
                >
            </div>
        @endif

        @if($exportable)
            <button
                type="button"
                wire:click="export"
                class="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md shadow-sm text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            >
                {{ __('Export CSV') }}
            </button>
        @endif
    </div>

    <div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
        <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead class="bg-gray-50 dark:bg-gray-900/30">
                <tr>
                    @foreach($columns as $column)
                        <th scope="col" class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            {{ $column['label'] }}
                        </th>
                    @endforeach
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200 dark:bg-gray-900 dark:divide-gray-800">
                @forelse($data as $row)
                    <tr class="odd:bg-white even:bg-gray-50 dark:odd:bg-gray-900 dark:even:bg-gray-800">
                        @foreach($columns as $column)
                            <td class="px-4 py-3 text-sm text-gray-900 dark:text-gray-100">
                    {{ $row[$column['key']] ?? '—' }}
                            </td>
                        @endforeach
                    </tr>
                @empty
                    <tr>
                        <td colspan="{{ count($columns) }}" class="px-4 py-6 text-center text-sm text-gray-500 dark:text-gray-400">
                            {{ __('No records found for the current selection.') }}
                        </td>
                    </tr>
                @endforelse
            </tbody>
        </table>
    </div>

    <div>
        {{ $data->links() }}
    </div>
</div>
