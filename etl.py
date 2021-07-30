import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
  Description: This function can be used to read the file in the filepath (data/song_data)
  to get the song and artist data used to populate the song and artist dim tables. 

  Arguments:
      cur: the cursor object. 
      filepath: song data file path. 

  Returns:
      None
  """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
  Description: This function can be used to read the file in the filepath (data/log_data)
  to get the user and time info and used to populate the users and time dim tables. 
  After populating the user and time dim tables, it then populates the songplay fact table. 

  Arguments:
      cur: the cursor object. 
      filepath: log data file path. 

  Returns:
      None
  """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.head(), t.dt.hour.head(), t.dt.day.head(), t.dt.weekofyear.head(), t.dt.month.head(), t.dt.year.head(), t.dt.weekday.head())
    column_labels = ('timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday')
    time_dict = {k:v for k,v in zip(column_labels,time_data)}
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = user_df = df[["firstName", "lastName", "gender", "level"]].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (datetime.datetime.fromtimestamp(row.ts / 1e3), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
  Description: This function finds all the files in a specified directory and processes each one. 

  Arguments:
      cur: the cursor object.
      conn:connection to the sparkify database
      filepath: log data file path. 
      func: fuction to process the data

  Returns:
      None
  """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
  Description: This function serves as the initiating function that begins the ETL process. 

  Arguments:
      None 

  Returns:
      None
  """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
