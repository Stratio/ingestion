CREATE KEYSPACE IF NOT EXISTS films WITH REPLICATION = {
 'class' : 'SimpleStrategy', 'replication_factor' : 1
 };


CREATE TABLE IF NOT EXISTS films.frozen (
        titulo text,
        ano text,
        rated text,
        released text,
        runtime text,
        genre text,
        director text,
        writer text,
        actors text,
        plot text,
        language text,
        country text,
        awards text,
        poster text,
        metascore text,
        imdbrating text,
        imdbvotes text,
        type text,
        response text,
  imdbid text PRIMARY KEY);
