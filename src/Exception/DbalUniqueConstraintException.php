<?php

namespace Incubatrix\DbalManager\Exception;

use Throwable;

final class DbalUniqueConstraintException extends DbalException
{
    public function __construct(
        private readonly string $uniqueConstraintName,
        private readonly array $notUniqueValues,
        Throwable $previousException,
    ) {
        parent::__construct($previousException);
    }

    public function getUniqueConstraintName(): string
    {
        return $this->uniqueConstraintName;
    }

    public function getNotUniqueValues(): array
    {
        return $this->notUniqueValues;
    }
}
