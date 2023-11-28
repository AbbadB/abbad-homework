INSERT INTO abbad.actors
WITH last_year AS (
    SELECT *
    FROM abbad.actors
    WHERE current_year = 1999
),
this_year AS (
    SELECT
        *,
        ARRAY_AGG(ROW(film, votes, rating, film_id)) OVER (PARTITION BY actor_id) AS films,
        CASE
            WHEN AVG(rating) OVER (PARTITION BY actor_id) > 8 THEN 'star'
            WHEN AVG(rating) OVER (PARTITION BY actor_id) > 7 OR AVG(rating) OVER (PARTITION BY actor_id) <= 8 THEN 'good'
            WHEN AVG(rating) OVER (PARTITION BY actor_id) > 6 OR AVG(rating) OVER (PARTITION BY actor_id) <= 7 THEN 'average'
            ELSE 'bad'
        END AS quality_class
    FROM bootcamp.actor_films
    WHERE year = 2000
),
actor_data AS (
    SELECT
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.actorid, ty.actor_id) AS actor_id,
        CASE
            WHEN ty.films IS NULL THEN ly.films
            WHEN ly.films IS NULL AND ty.films IS NOT NULL THEN ty.films
            WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN
                ty.films || ly.films
        END AS films,
        COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
        ty.year is not null as is_active,
        COALESCE(ty.year, ly.current_year + 1) as current_year
    FROM last_year ly
    FULL OUTER JOIN this_year ty
    ON ly.actorid = ty.actor_id
)
SELECT * FROM actor_data 
GROUP BY actor, actor_id, films, quality_class, is_active, current_year