<?php

declare(strict_types=1);

namespace Incubatrix\DbalManager;

use Doctrine\DBAL\Result;
use Incubatrix\DbalManager\Exception\DbalException;

trait ManagerTrait
{
    public function __construct(
        protected DbalManager $manager,
    ) {
    }

    /**
     * @throws DbalException
     */
    public function insert(array $params): void
    {
        $this->manager->insert(static::TABLE, $params);
    }

    /**
     * @throws DbalException
     */
    public function update(array $params, array $where = []): int
    {
        return $this->manager->update(static::TABLE, $params, $where);
    }

    /**
     * @throws DbalException
     */
    public function updateBulk(array $paramsList, ?array $whereFields = null): int
    {
        return $this->manager->updateBulk(static::TABLE, $paramsList, $whereFields);
    }

    /**
     * @throws DbalException
     */
    public function upsert(array $params, array $replaceFields): int
    {
        return $this->manager->upsert(static::TABLE, $params, $replaceFields);
    }

    /**
     * @throws DbalException
     */
    public function upsertBulk(array $paramsList, array $replaceFields = []): int
    {
        return $this->manager->upsertBulk(static::TABLE, $paramsList, $replaceFields);
    }

    /**
     * @throws DbalException
     */
    public function executeQuery(string $sql, array $params = [], array $types = []): Result
    {
        return $this->manager->executeQuery($sql, $params, $types);
    }
}
