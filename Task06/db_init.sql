---- Создание
-- Специальность
CREATE TABLE IF NOT EXISTS specialties --
(
    id    INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

-- Категория оказываемых услуг
CREATE TABLE IF NOT EXISTS categories --
(
    id    INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

-- Статус доктора
CREATE TABLE IF NOT EXISTS employee_statuses --
(
    id    INTEGER PRIMARY KEY,
    title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS doctors
(
    id                  INTEGER PRIMARY KEY,
    first_name          TEXT    NOT NULL,
    last_name           TEXT    NOT NULL,
    patronymic          TEXT,
    date_of_birth       TEXT    NOT NULL,
    speciality_id       INTEGER NOT NULL,
    earning_in_percents INTEGER NOT NULL,
    employee_status_id  INTEGER NOT NULL,
    FOREIGN KEY (speciality_id) REFERENCES specialties (id),
    FOREIGN KEY (employee_status_id) REFERENCES employee_statuses (id)
);

CREATE TABLE IF NOT EXISTS clients
(
    id            INTEGER PRIMARY KEY,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    patronymic    TEXT,
    date_of_birth TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS services
(
    id                  INTEGER PRIMARY KEY,
    title               TEXT    NOT NULL,
    price               DECIMAL NOT NULL,
    duration_in_minutes INTEGER NOT NULL,
    category_id         INTEGER NOT NULL,
    FOREIGN KEY (category_id) REFERENCES categories (id)
);

CREATE TABLE IF NOT EXISTS reception_statuses
(
    id    INTEGER PRIMARY KEY,
    title TEXT
);

CREATE TABLE IF NOT EXISTS receptions
(
    id           INTEGER PRIMARY KEY,
    doctor_id    INTEGER NOT NULL,
    client_id    INTEGER NOT NULL,
    service_id   INTEGER NOT NULL,
    scheduled_at TEXT,
    started_at   TEXT,
    ended_at     TEXT,
    cancelled_at TEXT,
    is_completed INTEGER,
    status_id    INTEGER NOT NULL,
    FOREIGN KEY (doctor_id) REFERENCES doctors (id),
    FOREIGN KEY (client_id) REFERENCES clients (id),
    FOREIGN KEY (status_id) REFERENCES reception_statuses (id),
    FOREIGN KEY (service_id) REFERENCES services (id)
);

-- Статистика будет вычисляться с помощью приложения,
-- используя doctor_id для нахождения данных в таблицу receptions
CREATE TABLE IF NOT EXISTS statistics_doctors
(
    id                            INTEGER PRIMARY KEY,
    doctor_id                     INTEGER NOT NULL,
    count_receptions              INTEGER NOT NULL,
    count_successfully_receptions INTEGER NOT NULL,
    count_working_day             INTEGER,
    first_working_day             TEXT,
    last_working_day              TEXT,
    FOREIGN KEY (doctor_id) REFERENCES doctors (id)
);

CREATE TABLE IF NOT EXISTS billings
(
    id              INTEGER PRIMARY KEY,
    doctor_id       INTEGER NOT NULL,
    paid_at         TEXT,
    original_amount DECIMAL NOT NULL DEFAULT 0,
    earnings_amount DECIMAL NOT NULL DEFAULT 0,
    FOREIGN KEY (doctor_id) REFERENCES doctors (id)
);


--- ДАННЫЕ
INSERT INTO reception_statuses (title)
VALUES ('NEW'),
       ('DONE'),
       ('CANCELLED');

INSERT INTO employee_statuses (title)
VALUES ('WORKING'),
       ('ABSENT'),
       ('FIRED'),
       ('VACATIONED');

INSERT INTO specialties (title)
VALUES ('THERAPIST'),
       ('SURGEON'),
       ('RADIOLOGIST');

INSERT INTO categories (title)
VALUES ('INSPECTION'),
       ('TOOTH TREATMENT'),
       ('PROPHYLAXIS'),
       ('TREATMENT');

INSERT INTO clients (first_name, last_name, patronymic, date_of_birth)
VALUES ('Asadbek', 'Isokov', 'Anvarzhonovich', '13.07.2000'),
       ('Vladislav', 'Puzin', 'Alekseevich', '08.03.2000'),
       ('Kirill', 'Kvashnin', 'Alekseevich', '06.03.1999');

INSERT INTO doctors (first_name, last_name, patronymic, date_of_birth, speciality_id, earning_in_percents,
                     employee_status_id)
VALUES ('Van', 'Darkholme', 'Ivanovich', '24.10.1972', 1, 90, 1),
       ('Valery', 'Aboba', null, '11.03.1954', 2, 80, 1),
       ('Billy', 'Herington', null, '14.07.1969', 3, 70, 1);

INSERT INTO services (title, price, duration_in_minutes, category_id)
VALUES ('Initial inspection', 300, 10, 1),
       ('Removal of a tooth ', 1000, 30, 3),
       ('Fluorography', 500, 5, 1),
       ('Removal of kidney stones', 2000, 120, 4);

INSERT INTO receptions (doctor_id, client_id, service_id, scheduled_at, started_at, ended_at, cancelled_at, is_completed, status_id)
VALUES (1, 1, 1,'2020-04-12 10:30:00', null, null, null, 0, 1),
       (1, 2, 2,'2020-04-13 11:30:00', null, null, null, 0, 1),
       (2, 3, 1,'2020-04-14 12:00:00', null, null, null, 0, 1),
       (2, 3, 4,null, '2020-04-08 12:00:00', '2020-04-08 12:05:00', null, 1, 2),
       (3, 3, 3,null, '2020-04-08 12:00:00', '2020-04-08 12:06:00', null, 1, 2);

INSERT INTO statistics_doctors (doctor_id, count_receptions, count_successfully_receptions, count_working_day,
                                first_working_day, last_working_day)
VALUES (1, 5, 4, 20, '2020-04-01', '2020-04-28'),
       (2, 8, 6, 22, '2020-04-01', '2020-04-25'),
       (3, 11, 10, 18, '2020-04-05', '2020-04-30');

INSERT INTO billings (doctor_id, paid_at, original_amount, earnings_amount)
VALUES (1, '2020-04-02 00:00:00', 10000, 8000),
       (2, '2020-04-03 00:00:00', 15000, 12000),
       (3, '2020-04-04 00:00:00', 20000, 15000);