class SqlQueries:
    songplay_table_insert = ("""
    INSERT INTO public.songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)			
            SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM public.staging_events
            WHERE page='NextSong' 
                  AND ( sessionid IS NOT NULL AND ts IS NOT NULL)
            ) events
            LEFT JOIN public.staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)
    
    user_table_insert_delete = ("""
    DELETE FROM public.users;
    INSERT INTO public.users (userid, first_name, last_name, gender, "level") 
        (
        SELECT distinct e.userid, e.firstname, e.lastname, e.gender, e.level
        FROM public.staging_events as e
        WHERE e.page='NextSong' AND e.userid IS NOT NULL AND e.ts = (SELECT max(ts) FROM public.staging_events WHERE userId = e.userId)
        );
    """)
    
    user_table_insert = ("""
    INSERT INTO public.users (userid, first_name, last_name, gender, "level") 
        (
        SELECT distinct e.userid, e.firstname, e.lastname, e.gender, e.level
        FROM public.staging_events as e
        WHERE e.page='NextSong' AND e.userid IS NOT NULL AND e.ts = (SELECT max(ts) FROM public.staging_events WHERE userId = e.userId)
        );
    """)

    song_table_insert_delete = ("""
    DELETE FROM public.songs;
    INSERT INTO public.songs (songid, title, artistid, "year", duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM public.staging_songs
    """)
    
    song_table_insert = ("""
    INSERT INTO public.songs (songid, title, artistid, "year", duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM public.staging_songs
    """)

    artist_table_insert_delete = ("""
    DELETE FROM public.artists;
    INSERT INTO public.artists (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM public.staging_songs
    """)
    
    artist_table_insert = ("""
    INSERT INTO public.artists (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM public.staging_songs
    """)

    time_table_insert_delete = ("""
    DELETE FROM public.time;
    INSERT INTO public.time (start_time, "hour", "day", week, "month", "year", weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM public.songplays
    """)
    
    time_table_insert = ("""
    INSERT INTO public.time (start_time, "hour", "day", week, "month", "year", weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM public.songplays
    """)