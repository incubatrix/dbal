<?php

declare(strict_types=1);

namespace Incubatrix\DbalManager;

use Doctrine\DBAL\Result;

trait ManagerTrait
{
    public function __construct(
        protected DbalManager $manager,
    ) {
    }

    public function insert(array $params): void
    {
        $this->manager->insert(static::TABLE, $params);
    }

    public function update(array $params, array $where = []): int
    {
        return $this->manager->update(static::TABLE, $params, $where);
    }

    public function updateBulk(array $paramsList, ?array $whereFields = null): int
    {
        return $this->manager->updateBulk(static::TABLE, $paramsList, $whereFields);
    }

    public function upsert(array $params, array $replaceFields): int
    {
        return $this->manager->upsert(static::TABLE, $params, $replaceFields);
    }

    public function upsertBulk(array $paramsList, array $replaceFields = []): int
    {
        return $this->manager->upsertBulk(static::TABLE, $paramsList, $replaceFields);
    }

    public function executeQuery(string $sql, array $params = [], array $types = []): Result
    {
        return $this->manager->executeQuery($sql, $params, $types);
    }
}
