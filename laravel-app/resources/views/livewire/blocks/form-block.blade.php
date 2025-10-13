<div class="space-y-4" wire:key="form-block-{{ $block->id ?? 'preview' }}">
    @if($title)
        <div>
            <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100">{{ $title }}</h3>
            @if($description)
                <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">{{ $description }}</p>
            @endif
        </div>
    @endif

    <form wire:submit.prevent="submit" class="space-y-4">
        <div class="grid gap-4 sm:grid-cols-2">
            @foreach($fields as $name => $field)
                <div class="flex flex-col gap-1">
                    <label for="field-{{ $name }}" class="text-sm font-medium text-gray-700 dark:text-gray-200">
                        {{ $field['label'] ?? \Illuminate\Support\Str::headline($name) }}
                    </label>

                    @php($type = $field['type'] ?? 'text')

                    @switch($type)
                        @case('textarea')
                            <textarea
                                id="field-{{ $name }}"
                                wire:model.defer="formData.{{ $name }}"
                                class="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 dark:bg-gray-800 dark:border-gray-700"
                                rows="3"
                            ></textarea>
                            @break

                        @case('select')
                            <select
                                id="field-{{ $name }}"
                                wire:model.defer="formData.{{ $name }}"
                                class="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 dark:bg-gray-800 dark:border-gray-700"
                            >
                                @foreach($field['options'] ?? [] as $optionValue => $optionLabel)
                                    <option value="{{ $optionValue }}">{{ $optionLabel }}</option>
                                @endforeach
                            </select>
                            @break

                        @default
                            <input
                                id="field-{{ $name }}"
                                type="{{ $type }}"
                                wire:model.defer="formData.{{ $name }}"
                                class="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 dark:bg-gray-800 dark:border-gray-700"
                            />
                            @break
                    @endswitch

                    @error('formData.'.$name)
                        <p class="text-sm text-red-600">{{ $message }}</p>
                    @enderror
                </div>
            @endforeach
        </div>

        <div class="flex items-center gap-3">
            <button
                type="submit"
                class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            >
                {{ $submitLabel }}
            </button>

            <div class="text-sm">
                @if($successMessage)
                    <span class="text-green-600">{{ $successMessage }}</span>
                @elseif($errorMessage)
                    <span class="text-red-600">{{ $errorMessage }}</span>
                @endif
            </div>
        </div>
    </form>
</div>
