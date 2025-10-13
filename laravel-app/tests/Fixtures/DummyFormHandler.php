<?php

namespace Tests\Fixtures;

class DummyFormHandler
{
    /**
     * @var array<int, array<string, mixed>>
     */
    public array $calls = [];

    /**
     * @param array<string, mixed> $payload
     */
    public function __invoke(array $payload): array
    {
        $this->calls[] = $payload;

        return [
            'ok' => true,
        ];
    }
}
