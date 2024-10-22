<?php

declare(strict_types=1);

namespace Incubatrix\DbalManager;

use function array_flip;
use function array_keys;
use function array_map;
use function array_pad;
use function count;
use function implode;
use function is_array;
use function sprintf;
use function str_replace;

class MysqlDriver
{
    public const UPSERT_INCREMENT = 'increment';
    public const UPSERT_DECREMENT = 'decrement';
    public const UPSERT_CONDITION = 'condition';

    public function getInsertBulkSql(string $tableName, array $paramsList, bool $isIgnore = false): string
    {
        $fields = implode(', ', array_keys($paramsList[0]));

        $sql = $isIgnore ? 'INSERT IGNORE' : 'INSERT';
        $sql = sprintf('%s INTO `%s` (%s) VALUES ', $sql, str_replace('`', '', $tableName), $fields);

        $sqlValuesList = [];

        foreach ($paramsList as $params) {
            $values = $this->getValues($params);
            $sqlValuesList[] = sprintf('(%s)', implode(', ', $values));
        }

        $sql .= ' ' . implode(', ', $sqlValuesList);

        return $sql;
    }

    public function getUpdateSql(string $tableName, array $params, array $where = []): string
    {
        if (!$where) {
            $where = ['id' => $params['id']];
        }

        unset($params['id']);

        $subSqlSetList = [];

        foreach ($params as $field => $data) {
            $subSqlSetList[$field] = sprintf('%s = %s', $field, '?');
        }

        $subSqlWhereList = [];

        foreach ($where as $whereField => $whereData) {
            $subSqlWhereList[$whereField] = sprintf('%s = %s', $whereField, '?');
        }

        $sql = sprintf('UPDATE `%s`', $tableName);

        return sprintf('%s SET %s WHERE %s', $sql, implode(', ', $subSqlSetList), implode(' AND ', $subSqlWhereList));
    }

    public function getUpdateBulkSql(string $tableName, array $paramsList, array $whereFields): string
    {
        $whereFieldListFlipped = array_flip($whereFields);
        $subSqlWhenList = [];
        $whereList = [];

        foreach ($paramsList as $params) {
            $whereParts = [];

            foreach ($params as $field => $data) {
                if (!isset($whereFieldListFlipped[$field])) {
                    continue;
                }

                $whereParts[] = sprintf('%s=%s', $field, '?');
            }

            foreach ($params as $field => $data) {
                if (isset($whereFieldListFlipped[$field])) {
                    continue;
                }

                $where = sprintf('(%s)', implode(' AND ', $whereParts));
                $whereKey = array_map(static fn ($item) => $params[$item], $whereFields);
                $whereKey = implode('-', $whereKey);
                $whereList[$whereKey] = $where;

                $subSqlWhenList[$field][] = sprintf('WHEN %s THEN %s', $where, '?');
            }
        }

        $subSqlCaseList = [];

        foreach ($subSqlWhenList as $field => $data) {
            $subSqlCaseList[] = sprintf('%s = CASE %s ELSE %s END', $field, implode(' ', $data), $field);
        }

        $sql = sprintf('UPDATE `%s`', $tableName);

        return sprintf('%s SET %s WHERE %s', $sql, implode(', ', $subSqlCaseList), implode(' OR ', $whereList));
    }

    public function getUpsertBulkSql(string $tableName, array $paramsList, array $replaceFields): string
    {
        $insertSql = $this->getInsertBulkSql($tableName, $paramsList);

        $sql = $insertSql . ' ON DUPLICATE KEY UPDATE ';

        $sqlReplaceList = [];

        foreach ($replaceFields as $replaceField) {
            if (!is_array($replaceField)) {
                $sqlReplaceList[] = sprintf('%s = VALUES(%s)', $replaceField, $replaceField);

                continue;
            }

            [$field, $replaceType] = $replaceField;
            $condition = $replaceField[2] ?? '';

            if ($replaceType === self::UPSERT_INCREMENT) {
                $sqlReplaceList[] = sprintf('%s = %s + VALUES(%s)', $field, $field, $field);
            } elseif ($replaceType === self::UPSERT_DECREMENT) {
                $sqlReplaceList[] = sprintf('%s = %s - VALUES(%s)', $field, $field, $field);
            } elseif ($replaceType === self::UPSERT_CONDITION) {
                $sqlReplaceList[] = sprintf('%s = %s', $field, $condition);
            }
        }

        $sql .= implode(', ', $sqlReplaceList);

        return $sql;
    }

    public function getDeleteSql(string $tableName, string $id): string
    {
        return sprintf('DELETE FROM `%s` WHERE id = %s', $tableName, $id);
    }

    public function getDeleteBulkSql(string $tableName, array $idList): string
    {
        $idList = $this->getValues($idList);

        return sprintf('DELETE FROM `%s` WHERE id IN (%s)', $tableName, implode(',', $idList));
    }

    private function getValues(array $params): array
    {
        $values = [];

        return array_pad($values, count($params), '?');
    }
}
