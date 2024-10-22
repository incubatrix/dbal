<?php

declare(strict_types=1);

namespace Incubatrix\DbalManager;

use Incubatrix\DbalManager\Exception\DbalConstraintException;
use Incubatrix\DbalManager\Exception\DbalException;
use Incubatrix\DbalManager\Exception\DbalUniqueConstraintException;
//use App\Utils\Generator\IdGenerator;
use DateTimeInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Result;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpFoundation\RequestStack;
use Throwable;

use function array_diff_key;
use function array_flip;
use function array_intersect_key;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_merge;
use function array_values;
use function debug_backtrace;
use function explode;
use function get_class;
use function implode;
use function in_array;
use function is_array;
use function is_bool;
use function json_encode;
use function preg_match;
use function reset;
use function sprintf;
use function str_contains;

class DbalManager implements EventSubscriberInterface
{
    private const ID_NAME = 'id';
    private const CREATED_AT_NAME = 'createdAt';
    private const UPDATED_AT_NAME = 'updatedAt';

    private ?Command $command = null;

    public function __construct(
        private readonly Connection $connection,
        private readonly MysqlDriver $driver,
        private readonly RequestStack $requestStack,
    ) {
    }

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    /**
     * @throws DbalException
     */
    public function insert(string $tableName, array $params, bool $isIgnore = false): int
    {
        return $this->insertBulk($tableName, [$params], $isIgnore);
    }

    /**
     * @throws DbalException
     */
    public function insertBulk(
        string $tableName,
        array $paramsList,
        bool $isIgnore = false,
        bool $isPrepareInsert = true,
    ): int {
        if (empty($paramsList)) {
            return 0;
        }

        if ($isPrepareInsert) {
            $paramsList = $this->prepareInsertParamsList($paramsList);
        }

        $sql = $this->driver->getInsertBulkSql($tableName, $paramsList, $isIgnore);

        [$params, $types] = $this->expandListParameters($paramsList);

        return $this->executeSql($sql, $params, $types);
    }

    /**
     * @throws DbalException
     */
    public function update(string $tableName, array $params, array $where = []): int
    {
        $params = $this->prepareUpdateParams($params);

        $sql = $this->driver->getUpdateSql($tableName, $params, $where);

        $idFieldName = self::ID_NAME;

        if (!$where) {
            $where[$idFieldName] = $params[$idFieldName];
        }

        unset($params[$idFieldName]);

        $params = array_merge(array_values($params), array_values($where));

        [$params, $types] = $this->expandListParameters([$params]);

        return $this->executeSql($sql, $params, $types);
    }

    /**
     * @throws DbalException
     */
    public function updateBulk(string $tableName, array $paramsList, ?array $whereFields = null): int
    {
        if (empty($paramsList)) {
            return 0;
        }

        if ($whereFields === null) {
            $whereFields = [self::ID_NAME];
        }

        $paramsList = $this->prepareUpdateParamsList($paramsList);

        $sql = $this->driver->getUpdateBulkSql($tableName, $paramsList, $whereFields);

        [$params, $types] = $this->expandListParameters($paramsList, $whereFields);

        return $this->executeSql($sql, $params, $types);
    }

    /**
     * @throws DbalException
     */
    public function upsert(string $tableName, array $params, array $replaceFields = []): int
    {
        return $this->upsertBulk($tableName, [$params], $replaceFields);
    }

    /**
     * @throws DbalException
     */
    public function upsertBulk(string $tableName, array $paramsList, array $replaceFields = []): int
    {
        if (empty($paramsList)) {
            return 0;
        }

        $paramsList = $this->prepareInsertParamsList($paramsList);
        $replaceFields = $this->updateReplaceFields($replaceFields);

        $sql = $this->driver->getUpsertBulkSql($tableName, $paramsList, $replaceFields);

        [$params, $types] = $this->expandListParameters($paramsList);

        return $this->executeSql($sql, $params, $types);
    }

    /**
     * @throws DbalException
     */
    public function delete(string $tableName, string $id): int
    {
        $sql = $this->driver->getDeleteSql($tableName, $id);

        $idFieldName = self::ID_NAME;
        $params = [$idFieldName => $id];

        [$params, $types] = $this->expandListParameters([$params]);

        return $this->executeSql($sql, $params, $types);
    }

    /**
     * @throws DbalException
     */
    public function deleteBulk(string $tableName, array $idList): int
    {
        if (empty($idList)) {
            return 0;
        }

        $sql = $this->driver->getDeleteBulkSql($tableName, $idList);

        $idFieldName = self::ID_NAME;
        $paramsList = array_map(static fn ($id) => [$idFieldName => $id], $idList);

        [$params, $types] = $this->expandListParameters($paramsList);

        return $this->executeSql($sql, $params, $types);
    }

    private function prepareInsertParamsList(array $paramsList): array
    {
        $date = date('Y-m-d H:i:s');

        foreach ($paramsList as &$params) {
            $params = $this->prepareId($params);
            $params = $this->prepareCreatedAt($params, $date);
            $params = $this->prepareUpdatedAt($params, $date);
        }

        return $paramsList;
    }

    private function prepareUpdateParams(array $params): array
    {
        $result = $this->prepareUpdateParamsList([$params]);

        return reset($result);
    }

    private function prepareUpdateParamsList(array $paramsList): array
    {
        $date = date('Y-m-d H:i:s');

        foreach ($paramsList as &$params) {
            $params = $this->prepareUpdatedAt($params, $date);
        }

        return $paramsList;
    }

    private function updateReplaceFields(array $replaceFields): array
    {
        $updatedAtFieldName = self::UPDATED_AT_NAME;

        if (!in_array($updatedAtFieldName, $replaceFields, true)) {
            $replaceFields[] = $updatedAtFieldName;
        }

        return $replaceFields;
    }

    private function prepareId(array $params): array
    {
        $idFieldName = self::ID_NAME;

        if (empty($params[$idFieldName])) {
            $params[$idFieldName] = [$this->generateUniqueId()];
        }

        return $params;
    }

    private function prepareCreatedAt(array $params, string $date): array
    {
        $createdAtFieldName = self::CREATED_AT_NAME;

        if (empty($params[$createdAtFieldName])) {
            $params[$createdAtFieldName] = [$date];
        }

        return $params;
    }

    private function prepareUpdatedAt(array $params, string $date): array
    {
        $updatedAtFieldName = self::UPDATED_AT_NAME;

        $params[$updatedAtFieldName] = [$date];

        return $params;
    }

    private function generateUniqueId(): string
    {
        return ''; //IdGenerator::id(); //TODO: ????
    }

    private function expandListParameters(array $paramsList, array $whereUpdateBulkFieldList = null): array
    {
        $mergedParams = [];
        $mergedTypes = [];

        if ($whereUpdateBulkFieldList) {
            $whereFieldFlip = array_flip($whereUpdateBulkFieldList);
            $wherePartValueList = [];
            $wherePartTypeList = [];

            $setsList = array_map(static fn ($params) => array_diff_key($params, $whereFieldFlip), $paramsList);
            $fieldList = array_keys(array_merge(...$setsList));

            foreach ($fieldList as $field) {
                foreach ($paramsList as $params) {
                    if (!array_key_exists($field, $params)) {
                        continue;
                    }

                    $whereValueList = [];
                    $whereTypesList = [];

                    $whereFieldValueMap = array_intersect_key($params, $whereFieldFlip);

                    foreach ($whereFieldValueMap as $value) {
                        if (is_array($value)) {
                            $parameterValue = $value[0];
                            $parameterType = $value[1] ?? null;
                        } else {
                            $parameterValue = $value;
                            $parameterType = null;
                        }

                        $mergedParams[] = $parameterValue;
                        $mergedTypes[] = $parameterType;

                        $whereValueList[] = $parameterValue;
                        $whereTypesList[] = $parameterType;
                    }

                    if (is_array($params[$field])) {
                        $parameterValue = $params[$field][0];
                        $parameterType = $params[$field][1] ?? null;
                    } else {
                        $parameterValue = $params[$field];
                        $parameterType = null;
                    }

                    $mergedParams[] = $parameterValue;
                    $mergedTypes[] = $parameterType;

                    $wherePartKey = implode('-', $whereValueList);
                    $wherePartValueList[$wherePartKey] = $whereValueList;
                    $wherePartTypeList[$wherePartKey] = $whereTypesList;
                }
            }

            $mergedParams = array_merge($mergedParams, ...array_values($wherePartValueList));
            $mergedTypes = array_merge($mergedTypes, ...array_values($wherePartTypeList));
        } else {
            foreach ($paramsList as $params) {
                foreach ($params as $value) {
                    $parameterType = ParameterType::STRING;

                    if (is_array($value)) {
                        if (empty($value[0])) {
                            try {
                                $parameterValue = json_encode($value, JSON_THROW_ON_ERROR);
                            } catch (Throwable) {
                                $parameterValue = '{"json":"encode error"}';
                            }
                        } else {
                            $parameterValue = $value[0];
                        }
                    } else {
                        if (is_bool($value)) {
                            $value = (int) $value;
                        }

                        if ($value instanceof DateTimeInterface) {
                            $value = $value->format('Y-m-d H:i:s.u');
                        }

                        $parameterValue = $value;
                    }

                    $mergedParams[] = $parameterValue;
                    $mergedTypes[] = $parameterType;
                }
            }
        }

        return [$mergedParams, $mergedTypes];
    }

    /**
     * @throws DbalException
     */
    private function executeSql(string $sql, array $params, array $types): int
    {
        $sql = $this->addSqlComment($sql);

        try {
            return $this->connection->executeStatement($sql, $params, $types);
        } catch (UniqueConstraintViolationException $e) {
            $message = $e->getMessage();

            preg_match('/Duplicate entry \'(.*?)\' for key \'.*?\.(.*?)\'/', $message, $matches);
            $notUniqueValues = explode('-', $matches[1]);
            $uniqueConstraintName = $matches[2];

            throw new DbalUniqueConstraintException($uniqueConstraintName, $notUniqueValues, $e);
        } catch (DriverException $e) {
            $message = $e->getMessage();

            preg_match('/Check constraint \'(.*?)\' is violated/', $message, $matches);
            $constraintName = $matches[1] ?? null;

            if ($constraintName) {
                throw new DbalConstraintException($constraintName, $e);
            }

            throw new DbalException($e);
        } catch (Throwable $e) {
            throw new DbalException($e);
        }
    }

    /**
     * @throws DbalException
     */
    public function fetchDto(
        string $dtoClass,
        string $sql,
        array $params = [],
        array $types = [],
    ): ?object {
        $stmt = $this->executeQuery($sql, $params, $types);

        return $this->deserialize($dtoClass, $stmt);
    }

    /**
     * @throws DbalException
     */
    public function fetchDtoList(
        string $dtoClass,
        string $sql,
        array $params = [],
        array $types = [],
    ): array {
        $stmt = $this->executeQuery($sql, $params, $types);

        return $this->deserializeList($dtoClass, $stmt);
    }

    /**
     * @throws DbalException
     */
    private function deserialize(string $dtoClass, Result $stmt): ?object
    {
        try {
            $result = $stmt->fetchAssociative();

            if (!$result) {
                return null;
            }

            return new $dtoClass($result);
        } catch (Throwable $e) {
            throw new DbalException($e);
        }
    }

    /**
     * @throws DbalException
     */
    private function deserializeList(string $dtoClass, Result $stmt): array
    {
        try {
            $dtoList = [];

            while ($result = $stmt->fetchAssociative()) {
                $dtoList[] = new $dtoClass($result);
            }

            return $dtoList;
        } catch (Throwable $e) {
            throw new DbalException($e);
        }
    }

    /**
     * @throws DbalException
     */
    public function executeQuery(string $sql, array $params = [], array $types = []): Result
    {
        try {
            $sql = $this->addSqlComment($sql);

            return $this->connection->executeQuery($sql, $params, $types);
        } catch (Throwable $e) {
            throw new DbalException($e);
        }
    }

    private function addSqlComment(string $sql): string
    {
        $additionalSql = [
            'entryPointController' => $this->getEntryPointClass(),
            'applicationCaller' => $this->getApplicationCaller(),
        ];

        try {
            return sprintf('/* %s */ %s', json_encode($additionalSql, JSON_THROW_ON_ERROR), $sql);
        } catch (Throwable) {
            return $sql;
        }
    }

    private function getEntryPointClass(): string
    {
        $mainRequest = $this->requestStack->getMainRequest();
        $requestController = $mainRequest?->attributes->get('_controller');

        if (is_array($requestController)) {
            return implode('::', $requestController);
        }

        if ($requestController) {
            return $requestController;
        }

        if ($this->command) {
            return get_class($this->command);
        }

        return '';
    }

    private function getApplicationCaller(): string
    {
        $backtrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);

        foreach ($backtrace as $key => $item) {
            $filePath = $item['file'] ?? null;

            if (str_contains($filePath, 'DbalManager')) {
                continue;
            }

            if (!str_contains($filePath, '/vendor/')) {
                $applicationCaller = $backtrace[$key + 1] ?? null;

                $class = $applicationCaller['class'] ?? null;
                $function = $applicationCaller['function'] ?? null;

                return $class && $function ? sprintf('%s::%s', $class, $function) : '';
            }
        }

        return '';
    }

    public function onConsoleCommand(ConsoleCommandEvent $event): void
    {
        $command = $event->getCommand();

        if (!$command instanceof Command) {
            return;
        }

        $this->command = $command;
    }

    public static function getSubscribedEvents(): array
    {
        return [
            ConsoleEvents::COMMAND => ['onConsoleCommand', 0],
        ];
    }
}
