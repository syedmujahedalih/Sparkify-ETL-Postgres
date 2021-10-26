# DROP TABLES

drop_statement = "DROP TABLE IF EXISTS "

songplay_table_drop = drop_statement+"songplays;"
user_table_drop = drop_statement+"users;"
song_table_drop = drop_statement+"songs;"
artist_table_drop = drop_statement+"artists;"
time_table_drop = drop_statement+"time;"

# CREATE TABLES
create_statement = "CREATE TABLE IF NOT EXISTS "
songplay_table_create = create_statement + "songplays(songplay_id SERIAL PRIMARY KEY, start_time timestamp NOT NULL, level varchar, user_id int NOT NULL, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);"

user_table_create = create_statement + "users(user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar);"

song_table_create = create_statement + "songs(song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float);"

artist_table_create = create_statement + "artists(artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude float, longitude float);"

time_table_create = create_statement + "time(start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);"

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = "INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")


time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT s.song_id, a.artist_id from songs s join artists a on s.artist_id = a.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]