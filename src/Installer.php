<?php

declare(strict_types=1);

namespace Incubatrix\DbalManager;

class Installer
{
    public static function copyConfig()
    {
        $source = __DIR__ . '/src/Resources/config/dbal_manager.yaml';
        $destination = __DIR__ . '/../../config/packages/dbal_manager.yaml';

        if (!file_exists($source)) {
            throw new \Exception("Source config file does not exist: " . $source);
        }

        if (!file_exists($destination)) {
            if (!copy($source, $destination)) {
                throw new \Exception("Failed to copy config file to destination: " . $destination);
            }
        } else {
            echo "Config file already exists in destination, skipping copy.";
        }
    }
}
