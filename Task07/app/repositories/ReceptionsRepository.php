<?php

require_once 'app/repositories/Repository.php';
require_once 'app/dto/Reception.php';

class ReceptionsRepository extends Repository
{
    public function findByDoctorId(int $doctorId): array
    {
        $statement = $this->getConnection()
            ->prepare(
                '
select d.id         as id,
       d.first_name as firstName,
       d.last_name  as lastName,
       d.patronymic as patronymic,
       r.ended_at   as endedAt,
       rs.title     as serviceName,
       s.title      as status,
       s.price      as price

from receptions as r
         join doctors as d on r.doctor_id = d.id
         join services as s on r.service_id = s.id
         join reception_statuses rs on r.status_id = rs.id
where d.id = ?'
            );

        $statement->execute([$doctorId]);

        return $statement->fetchAll(PDO::FETCH_CLASS, Reception::class);
    }

    public function findAll(): array
    {
        return $this->getConnection()
            ->query(
                '
select d.id         as id,
       d.first_name as firstName,
       d.last_name  as lastName,
       d.patronymic as patronymic,
       r.ended_at   as endedAt,
       rs.title     as serviceName,
       s.title      as status,
       s.price      as price
from receptions as r
         join doctors as d on r.doctor_id = d.id
         join services as s on r.service_id = s.id
         join reception_statuses rs on r.status_id = rs.id'
            )
            ->fetchAll(PDO::FETCH_CLASS, Reception::class);
    }
}
