<?php

declare(strict_types=1);

namespace Incubatrix\Dbal\Exception;

use Throwable;

final class DbalConstraintException extends DbalException
{
    public function __construct(
        private readonly string $constraintName,
        Throwable $previousException,
    ) {
        parent::__construct($previousException);
    }

    public function getConstraintName(): string
    {
        return $this->constraintName;
    }
}
