<?php

namespace App\Livewire\Blocks;

use App\Models\AuditLog;
use App\Models\BlockDefinition;
use App\Models\BlockInstance;
use Illuminate\Contracts\View\View;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Gate;
use Illuminate\Validation\ValidationException;
use Livewire\Attributes\Locked;
use Livewire\Component;

class FormBlock extends Component
{
    #[Locked]
    public BlockInstance $block;

    #[Locked]
    public BlockDefinition $definition;

    /** @var array<string, mixed> */
    #[Locked]
    public array $config = [];

    /** @var array<string, mixed> */
    public array $formData = [];

    /**
     * @var array<string, mixed>
     */
    private array $fieldDefinitions = [];

    public bool $submitted = false;

    public ?string $successMessage = null;

    public ?string $errorMessage = null;

    public function mount(BlockInstance $block, BlockDefinition $definition, array $config = []): void
    {
        $this->block = $block;
        $this->definition = $definition;
        $this->config = $config;

        $this->fieldDefinitions = collect(Arr::get($config, 'fields', []))
            ->mapWithKeys(function (array $field): array {
                $key = (string) Arr::get($field, 'name');

                return [$key => $field];
            })
            ->all();

        foreach ($this->fieldDefinitions as $name => $definition) {
            $this->formData[$name] = Arr::get($definition, 'default');
        }
    }

    public function render(): View
    {
        return view('livewire.blocks.form-block', [
            'title' => Arr::get($this->config, 'title'),
            'description' => Arr::get($this->config, 'description'),
            'fields' => $this->fieldDefinitions,
            'submitLabel' => Arr::get($this->config, 'submit_label', __('Submit')),
            'submitted' => $this->submitted,
            'successMessage' => $this->successMessage,
            'errorMessage' => $this->errorMessage,
        ]);
    }

    public function submit(): void
    {
        $this->authorizeAction();

        $validated = $this->validate($this->buildValidationRules());

        $handler = (string) Arr::get($this->config, 'handler');

        if ($handler === '') {
            throw ValidationException::withMessages([
                'config.handler' => __('Form block is missing the handler configuration.'),
            ]);
        }

        [$class, $method] = $this->parseHandler($handler);

        try {
            $service = app($class);
            $result = $service->{$method}($validated['formData']);
        } catch (ValidationException $exception) {
            throw $exception;
        } catch (\Throwable $throwable) {
            report($throwable);
            $this->submitted = true;
            $this->successMessage = null;
            $this->errorMessage = __('An unexpected error occurred while processing the form.');

            $this->logAudit($validated['formData'], false, $throwable->getMessage());

            return;
        }

        $this->submitted = true;
        $this->errorMessage = null;
        $this->successMessage = Arr::get($this->config, 'success_message', __('Changes have been saved.'));

        $this->logAudit($validated['formData'], true);

        if ((bool) Arr::get($this->config, 'reset_on_success', true)) {
            foreach ($this->fieldDefinitions as $name => $definition) {
                $this->formData[$name] = Arr::get($definition, 'default');
            }
        }

        $this->dispatch('form-block:submitted', result: $result, blockId: $this->block->id);
    }

    private function authorizeAction(): void
    {
        $permission = Arr::get($this->config, 'permission');

        if (is_string($permission) && $permission !== '') {
            Gate::authorize($permission);

            return;
        }

        $policy = Arr::get($this->config, 'policy');

        if (is_array($policy)) {
            $ability = (string) Arr::get($policy, 'ability');
            $modelClass = Arr::get($policy, 'model');

            if ($ability !== '' && is_string($modelClass) && class_exists($modelClass)) {
                $model = null;

                if (Arr::get($policy, 'with_record', false)) {
                    $recordId = Arr::get($policy, 'record_id');
                    $model = $recordId !== null ? $modelClass::find($recordId) : new $modelClass();
                }

                Gate::authorize($ability, $model ?? $modelClass);
            }
        }
    }

    /**
     * @return array<string, string|array<int, string>>
     */
    private function buildValidationRules(): array
    {
        $rules = [];

        foreach ($this->fieldDefinitions as $name => $definition) {
            $rules['formData.'.$name] = Arr::get($definition, 'rules', []);
        }

        return $rules;
    }

    /**
     * @return array{0: class-string, 1: string}
     */
    private function parseHandler(string $handler): array
    {
        if (str_contains($handler, '@')) {
            [$class, $method] = explode('@', $handler, 2);

            return [$class, $method];
        }

        return [$handler, '__invoke'];
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function logAudit(array $payload, bool $success, ?string $error = null): void
    {
        if (! Arr::get($this->config, 'audit.enabled', true)) {
            return;
        }

        $user = auth()->user();

        AuditLog::query()->create([
            'actor_id' => $user?->getAuthIdentifier(),
            'actor_username' => $user?->email,
            'action' => Arr::get($this->config, 'audit.action', 'form_block.submit'),
            'entity' => $this->definition->handle,
            'entity_id' => (string) $this->block->id,
            'payload_json' => [
                'payload' => $payload,
                'success' => $success,
                'error' => $error,
            ],
        ]);
    }
}
