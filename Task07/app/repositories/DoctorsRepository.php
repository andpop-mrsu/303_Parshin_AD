<?php

require_once 'app/repositories/Repository.php';
require_once 'app/dto/DoctorId.php';

class DoctorsRepository extends Repository
{
    public function findAllIds(): array
    {
        return $this->getConnection()
            ->query(
                '
select d.id as id   
from doctors as d'
            )
            ->fetchAll(PDO::FETCH_CLASS, DoctorId::class);
    }
}
