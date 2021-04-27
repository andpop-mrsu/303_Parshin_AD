<?php

require_once 'app/repositories/ReceptionsRepository.php';
require_once 'app/repositories/Repository.php';
require_once 'app/Validator.php';
require_once 'app/Output.php';

const DB_NAME = 'hospital.db';

$pdo = preparePDO(DB_NAME);
$receptionsRepository = new ReceptionsRepository($pdo);
$output = new Output();
$validator = new Validator();

$parameter = findDoctorIdFromArgv();

[$isSuccess, $message] = $validator->validateParameter($parameter);

if (!$isSuccess) {
    $output->echoError($message);
    die();
}

$doctorId = $parameter === null | $parameter === '' ? null : (int)$parameter;

$receptions = $doctorId === null ?
    $receptionsRepository->findAll() :
    $receptionsRepository->findByDoctorId($doctorId);

$output->echoReceptions($receptions, [
    'ID',
    'First name',
    'Last name',
    'Patronymic',
    'Service name',
    'Status',
    'Ended at',
    'Price',
]);

function preparePDO(string $dbName): PDO
{
    return new PDO(
        'sqlite:' . realpath($dbName),
        '',
        '',
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
        ]
    );
}

function findDoctorIdFromArgv(): ?string
{
    return $GLOBALS['argv'][1] ?? null;
}

