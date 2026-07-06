"""ETL pipeline: load Sparkify JSON song and log files into the Postgres
star schema defined in sql_queries.py.

Run after create_tables.py:
    python create_tables.py
    python etl.py
"""
import glob
import os

import pandas as pd
import psycopg2

from sql_queries import (
    artist_table_insert,
    song_select,
    song_table_insert,
    songplay_table_insert,
    time_table_insert,
    user_table_insert,
)


def process_song_file(cur, filepath):
    """Insert one song file's song and artist records."""
    df = pd.read_json(filepath, lines=True)

    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    artist_data = df[
        ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    ].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Insert one log file's time, user, and songplay records."""
    df = pd.read_json(filepath, lines=True)
    df = df[df["page"] == "NextSong"]

    # time dimension: derive calendar fields from the ms timestamp
    t = pd.to_datetime(df["ts"], unit="ms")
    time_data = list(
        zip(t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    )
    for row in time_data:
        cur.execute(time_table_insert, row)

    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates()
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # songplays fact: resolve song_id/artist_id by (title, artist, duration) lookup
    for _, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        song_id, artist_id = results if results else (None, None)

        songplay_data = (
            row.ts, row.userId, row.level, song_id, artist_id,
            row.sessionId, row.location, row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Walk a data directory and apply `func` to every JSON file found."""
    all_files = []
    for root, _dirs, files in os.walk(filepath):
        all_files.extend(glob.glob(os.path.join(root, "*.json")))

    print(f"{len(all_files)} files found in {filepath}")
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{len(all_files)} files processed.")


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
