<div class="p-4 border border-dashed rounded-lg text-center text-sm text-gray-500 dark:text-gray-400">
    <p class="font-semibold text-gray-700 dark:text-gray-200">{{ $definition->title }}</p>
    <p class="mt-2">{{ $message }}</p>
    <p class="mt-4 text-xs text-gray-400">
        {{ __('Legacy entrypoint: :entry', ['entry' => $definition->metadata['legacy_entrypoint'] ?? $definition->component]) }}
    </p>
</div>
