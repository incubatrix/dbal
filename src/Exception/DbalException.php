<?php

namespace Incubatrix\DbalManager\Exception;

use Exception;
use Throwable;

class DbalException extends Exception
{
    public function __construct(Throwable $e)
    {
        parent::__construct($e->getMessage());
    }
}
