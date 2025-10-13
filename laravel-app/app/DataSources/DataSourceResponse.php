<?php

namespace App\DataSources;

use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Collection;

class DataSourceResponse implements Arrayable
{
    /**
     * @param Collection<int, array<string, mixed>> $rows
     */
    public function __construct(
        public readonly Collection $rows,
        public readonly int $total,
        public readonly LengthAwarePaginator $paginator,
    ) {
    }

    /**
     * @return string[]
     */
    public function columns(): array
    {
        $first = $this->rows->first();

        if (! is_array($first)) {
            return [];
        }

        return array_keys($first);
    }

    /**
     * @return array{rows: array<int, array<string, mixed>>, total: int, current_page: int, per_page: int}
     */
    public function toArray(): array
    {
        return [
            'rows' => $this->rows->values()->all(),
            'total' => $this->total,
            'current_page' => $this->paginator->currentPage(),
            'per_page' => $this->paginator->perPage(),
        ];
    }
}
