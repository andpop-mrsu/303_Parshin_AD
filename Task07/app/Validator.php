<?php declare(strict_types=1);

class Validator
{
    public function validateParameter($parameter): array
    {
        if ($parameter === null || $parameter === '') {
            return [true, ''];
        }

        if (!is_numeric($parameter)) {
            return [false, 'Doctor\'s ID must be a number'];
        }

        if ($parameter < 1) {
            return [false, 'Doctor\'s ID must be a number greater than zero'];
        }

        return [true, ''];
    }
}
