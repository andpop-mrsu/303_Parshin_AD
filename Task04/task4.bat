#!/bin/bash
chcp 65001

sqlite3 movies_rating.db < db_init.sql

echo "1. Найти все драмы, выпущенные после 2005 года, которые понравились женщинам (оценка не ниже 4.5). Для каждого фильма в этом списке вывести название, год выпуска и количество таких оценок."
echo "--------------------------------------------------"
sqlite3 movies_rating.db -box -echo "select m.title, m.year, count(rating) as ratings_count from movies m join ratings r on m.id = r.movie_id join users u on r.user_id = u.id where instr(m.genres, 'Drama') > 0 and u.gender = 'female' and m.year > 2005 and rating >= 4.5 group by m.id;"
echo " "

echo "2. Провести анализ востребованности ресурса - вывести количество пользователей, регистрировавшихся на сайте в каждом году. Найти, в каких годах регистрировалось больше всего и меньше всего пользователей."
echo "--------------------------------------------------"
sqlite3 movies_rating.db "create view registration_statistics as select substr(u.register_date, 0, 5) as registered_at,count(u.id) as registrations_count from users u group by registered_at;"
sqlite3 movies_rating.db -box -echo "select * from registration_statistics;" 
sqlite3 movies_rating.db -box -echo "select max(registrations_count) as max_count, registered_at from registration_statistics;"
sqlite3 movies_rating.db -box -echo "select min(registrations_count) as min_count, registered_at from registration_statistics;"
sqlite3 movies_rating.db -box -echo "drop view registration_statistics;"
echo " "

echo "3. Найти все пары пользователей, оценивших один и тот же фильм. Устранить дубликаты, проверить отсутствие пар с самим собой. Для каждой пары должны быть указаны имена пользователей и название фильма, который они ценили."
echo "--------------------------------------------------"
sqlite3 movies_rating.db -box -echo "select m.title as movie, u1.name as first_user, u2.name as second_user from ratings r1 join ratings r2 on r1.movie_id = r2.movie_id and r1.id > r2.id join movies m on r1.movie_id = m.id join users u1 on r1.user_id = u1.id join users u2 on r2.user_id = u2.id order by m.id limit 100;"
echo " "

echo "4. Найти 10 самых старых оценок от разных пользователей, вывести названия фильмов, имена пользователей, оценку, дату отзыва в формате ГГГГ-ММ-ДД."
echo "--------------------------------------------------"
sqlite3 movies_rating.db -box -echo "select distinct date(r.timestamp, 'unixepoch') as rated_at, u.name, r.rating from ratings r join users u on r.user_id = u.id group by u.name order by rated_at limit 10;"
echo " "

echo "5. Вывести в одном списке все фильмы с максимальным средним рейтингом и все фильмы с минимальным средним рейтингом. Общий список отсортировать по году выпуска и названию фильма. В зависимости от рейтинга в колонке 'Рекомендуем' для фильмов должно быть написано 'Да' или 'Нет'."
echo "--------------------------------------------------"
sqlite3 movies_rating.db -box -echo "select title,year,rating,case when max_rating = rating then 'Yes' else 'No' end as recomendation from (select *, max(rating) over () as max_rating, min(rating) over () as min_rating from (select movies.title as title, movies.year as year, rating from movies join(select ratings.movie_id, avg(ratings.rating) as rating from ratings group by ratings.movie_id) ratings on ratings.movie_id = movies.id)) as avg_ratings where rating = max_rating or rating = min_rating order by year, title;"
echo " "

echo "6. Вычислить количество оценок и среднюю оценку, которую дали фильмам пользователи-мужчины в период с 2011 по 2014 год."
echo "--------------------------------------------------"
sqlite3 movies_rating.db -box -echo "select count(*) as ratings_count, round(avg(r.rating), 2) as ratings_average from ratings r join users u on r.user_id = u.id where u.gender = 'male' and datetime(r.timestamp, 'unixepoch') between '2011-01-01' and '2014-01-01';"
echo " "

echo "7. Составить список фильмов с указанием средней оценки и количества пользователей, которые их оценили. Полученный список отсортировать по году выпуска и названиям фильмов. В списке оставить первые 20 записей."
echo "--------------------------------------------------"
sqlite3 movies_rating.db -box -echo "select m.title, m.year, count(*) as ratings_count, round(avg(r.rating), 2) as ratings_average from movies m join ratings r on m.id = r.movie_id group by movie_id order by m.year, m.title limit 20;"
echo " "

echo "8. Определить самый распространенный жанр фильма и количество фильмов в этом жанре."
echo "--------------------------------------------------"
sqlite3 movies_rating.db "create view statistics_genres as select value, genre from ((select count(*) as value, '(no genres listed)' as genre from movies where instr(movies.genres, '(no genres listed)') > 0)) union select value, genre from (select count(*) as value, 'Action' as genre from movies where instr(movies.genres, 'Action') > 0) union select value, genre from (select count(*) as value, 'Adventure' as genre from movies where instr(movies.genres, 'Adventure') > 0) union select value, genre from (select count(*) as value, 'Animation' as genre from movies where instr(movies.genres, 'Animation') > 0) union select value, genre from (select count(*) as value, 'Children' as genre from movies where instr(movies.genres, 'Children') > 0) union select value, genre from (select count(*) as value, 'Comedy' as genre from movies where instr(movies.genres, 'Comedy') > 0) union select value, genre from (select count(*) as value, 'Crime' as genre from movies where instr(movies.genres, 'Crime') > 0) union select value, genre from (select count(*) as value, 'Documentary' as genre from movies where instr(movies.genres, 'Documentary') > 0) union select value, genre from (select count(*) as value, 'Drama' as genre from movies where instr(movies.genres, 'Drama') > 0) union select value, genre from (select count(*) as value, 'Fantasy' as genre from movies where instr(movies.genres, 'Fantasy') > 0) union select value, genre from (select count(*) as value, 'Film-Noir' as genre from movies where instr(movies.genres, 'Film-Noir') > 0) union select value, genre from (select count(*) as value, 'Horror' as genre from movies where instr(movies.genres, 'Horror') > 0) union select value, genre from (select count(*) as value, 'Musical' as genre from movies where instr(movies.genres, 'Musical') > 0) union select value, genre from (select count(*) as value, 'Mystery' as genre from movies where instr(movies.genres, 'Mystery') > 0) union select value, genre from (select count(*) as value, 'Romance' as genre from movies where instr(movies.genres, 'Romance') > 0) union select value, genre from (select count(*) as value, 'Sci-Fi' as genre from movies where instr(movies.genres, 'Sci-Fi') > 0) union select value, genre from (select count(*) as value, 'Thriller' as genre from movies where instr(movies.genres, 'Thriller') > 0) union select value, genre from (select count(*) as value, 'War' as genre from movies where instr(movies.genres, 'War') > 0) union select value, genre from (select count(*) as value, 'Western' as genre from movies where instr(movies.genres, 'Western') > 0) union select value, genre from (select count(*) as value, 'IMAX' as genre from movies where instr(movies.genres, 'IMAX') > 0);"
sqlite3 movies_rating.db -box -echo "select max(value) as count, genre as genre_title from statistics_genres;"
sqlite3 movies_rating.db "drop view statistics_genres;"