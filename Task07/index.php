<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Hospital</title>
    <link rel="stylesheet" href="styles/style.css">
</head>
<body>
<?php

require_once 'app/repositories/DoctorsRepository.php';
require_once 'app/repositories/ReceptionsRepository.php';
require_once 'app/repositories/Repository.php';
require_once 'app/Validator.php';

const DB_NAME = 'hospital.db';

$pdo = preparePDO(DB_NAME);
$doctorsRepository = new DoctorsRepository($pdo);
$receptionsRepository = new ReceptionsRepository($pdo);
$validator = new Validator();

$doctorIds = array_map(function (DoctorId $model) {
    return $model->id;
}, $doctorsRepository->findAllIds());

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

function findDoctorIdFromPost(): ?string
{
    return $_POST['doctorId'] ?? null;
}
?>
<div class="navbar">
    <h1 class="title">Clinic</h1>
</div>
<h1 class="form-title">Enter doctor's id</h1>
<form action="" method="POST">
    <label>
        <select class="id-selector" name="doctorId">
            <option value=<?= null ?>></option>
            <?php foreach($doctorIds as $id): ?>
                <option value=<?= $id ?>>
                    <?= $id ?>
                </option>
            <?php endforeach; ?>
        </select>
    </label>
    <button type="submit" class="search-button">Search</button>
</form>
<?php
$parameter = findDoctorIdFromPost();
[$isSuccess, $message] = $validator->validateParameter($parameter);
?>

<?php if (!$isSuccess): ?>
    <?= $message ?>
<?php endif; ?>

<?php
$doctorId = $parameter === null | $parameter === '' ? null : (int)$parameter;

$receptions = $doctorId === null || $doctorId === '' ?
    $receptionsRepository->findAll() :
    $receptionsRepository->findByDoctorId($doctorId);
?>
<div class="doctor-cards">
    <?php if ($isSuccess): ?>
        <?php foreach($receptions as $reception): ?>
            <div class="doctor-card">
                <span class="card-field">ID: <?= $reception->id ?></span>
                <span class="card-field">First name: <?= $reception->firstName ?></span>
                <span class="card-field">Last name: <?= $reception->lastName ?></span>
                <span class="card-field">Patronymic: <?= $reception->patronymic ?></span>
                <span class="card-field">Service name: <?= $reception->serviceName ?></span>
                <span class="card-field">Service status: <?= $reception->status ?></span>
                <span class="card-field">Ended at: <?= $reception->endedAt ?? 'Not ended yet' ?></span>
                <span class="card-field">Price: <?= $reception->price . 'RUB' ?></span>
            </div>
        <?php endforeach; ?>
    <?php endif; ?>
</div>
<?php if ($isSuccess && count($receptions) < 1): ?>
    No receptions for this doctor yet
<?php endif; ?>

</body>
</html>