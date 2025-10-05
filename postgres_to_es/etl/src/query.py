query = """
WITH directors AS (
    SELECT 
        fw.id AS film_id,
        COALESCE(array_agg(DISTINCT p.full_name), '{}') AS directors_names,
        COALESCE(
            json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE p.id IS NOT NULL), 
            '[]'
        ) AS directors
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE pfw.role = 'director'
    GROUP BY fw.id
),

actors AS (
    SELECT 
        fw.id AS film_id,
        COALESCE(array_agg(DISTINCT p.full_name), '{}') AS actors_names,
        COALESCE(
            json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE p.id IS NOT NULL), 
            '[]'
        ) AS actors
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE pfw.role = 'actor'
    GROUP BY fw.id
),

writers AS (
    SELECT 
        fw.id AS film_id,
        COALESCE(array_agg(DISTINCT p.full_name), '{}') AS writers_names,
        COALESCE(
            json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE p.id IS NOT NULL), 
            '[]'
        ) AS writers
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE pfw.role = 'writer'
    GROUP BY fw.id
),

genres AS (
    SELECT 
        fw.id AS film_id,
        array_agg(DISTINCT g.name) AS genres
    FROM content.film_work fw
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    GROUP BY fw.id
),

film_details AS (
    SELECT 
        fw.id,
        fw.title,
        fw.description,
        fw.rating AS imdb_rating,
        GREATEST(MAX(fw.modified), MAX(g.modified), MAX(p.modified)) AS last_modified_date
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    GROUP BY fw.id
)

SELECT 
    fd.id,
    fd.title,
    fd.description,
    fd.imdb_rating,
    COALESCE(d.directors_names, '{}') AS directors_names,
    COALESCE(a.actors_names, '{}') AS actors_names,
    COALESCE(w.writers_names, '{}') AS writers_names,
    COALESCE(d.directors, '[]') AS directors,
    COALESCE(a.actors, '[]') AS actors,
    COALESCE(w.writers, '[]') AS writers,
    COALESCE(genres.genres, '{}') AS genres,
    fd.last_modified_date
FROM film_details fd
LEFT JOIN directors d ON fd.id = d.film_id
LEFT JOIN actors a ON fd.id = a.film_id
LEFT JOIN writers w ON fd.id = w.film_id
LEFT JOIN genres ON fd.id = genres.film_id
WHERE fd.last_modified_date > %s
ORDER BY fd.last_modified_date;

"""
