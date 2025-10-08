<?php

namespace App\Livewire\Blocks;

use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use Illuminate\Contracts\View\View;
use Livewire\Component;

class LegacyWidgetPlaceholder extends Component
{
    public BlockInstance $block;

    public BlockDefinition $definition;

    /** @var array<string, mixed> */
    public array $config = [];

    public string $message;

    public function mount(BlockInstance $block, BlockDefinition $definition, array $config = []): void
    {
        $this->block = $block;
        $this->definition = $definition;
        $this->config = $config;
        $this->message = __('This block requires a custom frontend implementation.');
    }

    public function render(): View
    {
        return view('livewire.blocks.legacy-widget-placeholder');
    }
}
