<?php

namespace App\DataSources\Exceptions;

use RuntimeException;

class DataSourceExecutionException extends RuntimeException
{
    /**
     * @param string[] $errors
     */
    public function __construct(string $message, private readonly array $errors = [])
    {
        parent::__construct($message);
    }

    /**
     * @return string[]
     */
    public function getErrors(): array
    {
        return $this->errors;
    }
}
