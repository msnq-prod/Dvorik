<?php

namespace App\Exceptions;

use RuntimeException;

class InvalidBlockConfigurationException extends RuntimeException
{
    /**
     * @param string[] $errors
     */
    public function __construct(private readonly array $errors)
    {
        parent::__construct('Block configuration failed JSON schema validation.');
    }

    /**
     * @return string[]
     */
    public function getErrors(): array
    {
        return $this->errors;
    }
}
