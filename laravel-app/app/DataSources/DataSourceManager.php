<?php

namespace App\DataSources;

use App\DataSources\Exceptions\DataSourceExecutionException;
use App\DataSources\Exceptions\DataSourceNotFoundException;
use App\Models\QueryRegistry;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Collection;
use Illuminate\Support\Str;
use PDO;
use Throwable;

class DataSourceManager
{
    private const PAGINATION_LIMIT_MAX = 500;

    public function __construct(private readonly ConnectionInterface $connection)
    {
    }

    /**
     * @param array<string, mixed> $parameters
     */
    public function execute(string $key, array $parameters = [], ?int $perPage = null, ?int $page = null): DataSourceResponse
    {
        $query = QueryRegistry::query()->find($key);

        if (! $query instanceof QueryRegistry) {
            throw new DataSourceNotFoundException($key);
        }

        $sql = trim((string) $query->sql);

        if ($sql === '') {
            return $this->emptyResponse($perPage ?? 15, $page ?? 1);
        }

        $page = max($page ?? 1, 1);
        $perPage = $this->sanitizePerPage($perPage);

        try {
            $total = $this->count($sql, $parameters);
            $rows = $this->select($sql, $parameters, $perPage, $page);
        } catch (Throwable $throwable) {
            throw new DataSourceExecutionException(
                sprintf('Failed to execute data source [%s]: %s', $key, $throwable->getMessage()),
                [$throwable->getMessage()]
            );
        }

        $paginator = new LengthAwarePaginator(
            $rows->values(),
            $total,
            $perPage,
            $page,
            [
                'pageName' => $this->buildPageName($key),
            ]
        );

        return new DataSourceResponse($rows, $total, $paginator);
    }

    /**
     * @param array<string, mixed> $parameters
     */
    private function count(string $sql, array $parameters): int
    {
        $countSql = sprintf('SELECT COUNT(*) AS aggregate FROM (%s) AS source', $sql);
        $result = $this->connection->selectOne($countSql, $parameters);

        if ($result === null) {
            return 0;
        }

        if (is_object($result)) {
            return (int) ($result->aggregate ?? 0);
        }

        if (is_array($result)) {
            return (int) ($result['aggregate'] ?? 0);
        }

        return (int) $result;
    }

    /**
     * @param array<string, mixed> $parameters
     * @return Collection<int, array<string, mixed>>
     */
    private function select(string $sql, array $parameters, int $perPage, int $page): Collection
    {
        $offset = max($page - 1, 0) * $perPage;
        $limitPlaceholder = '__limit';
        $offsetPlaceholder = '__offset';

        while (array_key_exists($limitPlaceholder, $parameters)) {
            $limitPlaceholder = '_'.$limitPlaceholder;
        }

        while (array_key_exists($offsetPlaceholder, $parameters)) {
            $offsetPlaceholder = '_'.$offsetPlaceholder;
        }

        $pagedSql = sprintf(
            'SELECT * FROM (%s) AS source LIMIT :%s OFFSET :%s',
            $sql,
            $limitPlaceholder,
            $offsetPlaceholder
        );

        $bindings = $parameters + [
            $limitPlaceholder => $perPage,
            $offsetPlaceholder => $offset,
        ];

        $this->connection->getPdo()?->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

        $rows = $this->connection->select($pagedSql, $bindings);

        return collect($rows)
            ->map(function (mixed $row): array {
                if (is_object($row)) {
                    return (array) $row;
                }

                if (is_array($row)) {
                    return $row;
                }

                return [];
            });
    }

    private function sanitizePerPage(?int $perPage): int
    {
        $perPage ??= 15;

        $perPage = max($perPage, 1);

        return min($perPage, self::PAGINATION_LIMIT_MAX);
    }

    private function buildPageName(string $key): string
    {
        return 'page_'.Str::slug($key, '_');
    }

    private function emptyResponse(int $perPage, int $page): DataSourceResponse
    {
        $paginator = new LengthAwarePaginator(collect(), 0, $perPage, $page);

        return new DataSourceResponse(collect(), 0, $paginator);
    }
}
