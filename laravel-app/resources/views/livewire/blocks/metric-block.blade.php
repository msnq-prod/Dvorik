<div class="rounded-lg border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-800 dark:bg-gray-900" wire:key="metric-block-{{ $block->id ?? 'preview' }}">
    <div class="flex items-center justify-between">
        <div>
            <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100">{{ $title }}</h3>
            @if($description)
                <p class="text-sm text-gray-500 dark:text-gray-400">{{ $description }}</p>
            @endif
        </div>
    </div>

    <div class="mt-4 space-y-3">
        @forelse($dataset as $row)
            <div class="rounded-md border border-gray-100 bg-gray-50 p-3 dark:border-gray-800 dark:bg-gray-800/60">
                @foreach($columns as $column)
                    @php($label = $column['label'] ?? \Illuminate\Support\Str::headline($column['key'] ?? 'value'))
                    @php($key = $column['key'] ?? 'value')
                    <div class="flex items-start justify-between gap-4">
                        <dt class="text-sm font-medium text-gray-600 dark:text-gray-300">{{ $label }}</dt>
                        <dd class="text-sm text-gray-900 dark:text-gray-100 text-right">{{ $row[$key] ?? '—' }}</dd>
                    </div>
                @endforeach
            </div>
        @empty
            <p class="text-sm text-gray-500 dark:text-gray-400">{{ $empty }}</p>
        @endforelse
    </div>
</div>
