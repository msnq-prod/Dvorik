<?php

namespace App\DataSources\Exceptions;

use RuntimeException;

class DataSourceNotFoundException extends RuntimeException
{
    public function __construct(string $key)
    {
        parent::__construct(sprintf('Data source [%s] could not be located.', $key));
    }
}
