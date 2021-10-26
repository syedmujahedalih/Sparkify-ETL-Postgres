# Importing all the required packages

import os
import glob
import psycopg2
import pandas as pd
import json
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    
    This function processes the song dataset and separates the fields as needed for the song and artist tables
    
    Function Parameters: 
    
    cur: SQL/Database connection cursor
    filepath: OS path to the datasets
    
    
    """
    
    # opening the song file
    df = pd.read_json(filepath,lines=True)

    # inserting a song record from the song file
    song_data = df[['song_id','title','artist_id','year','duration']]
    song_data = song_data.values[0,:].tolist()
    cur.execute(song_table_insert, song_data)
    
    # inserting an artist record from the song file by selecting the required columns
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data = artist_data.values[0,:].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    
    This function processes the log dataset and separates the fields as needed for the time, user and songplay tables
    
    Function Parameters: 
    
    cur: SQL/Database connection cursor
    filepath: OS path to the datasets
    
    
    """
    
    
    # opening the log dataset
    
    df = pd.read_json(filepath,lines = True)

    # filtering by NextSong action
    
    df = df.loc[df["page"]=="NextSong"]

    # converting timestamp column to datetime
    
    t = df[['ts']].apply(pd.to_datetime) 
    
    # Applying transformations for the time table
    
    timestamp = t['ts']
    hour = t['ts'].dt.hour
    day = t['ts'].dt.day
    week_of_year = t['ts'].dt.weekofyear
    month = t['ts'].dt.month
    year = t['ts'].dt.year
    weekday = t['ts'].dt.weekday
    time_data = [timestamp, hour, day, week_of_year, month, year, weekday]
    column_labels = ('timestamp','hour','day','week_of_year','month','year','weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # loading fields for user table
    
    user_df = df[['userId','firstName','lastName','gender','level']]

    # inserting user records
    
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    df[['ts']] = df[['ts']].apply(pd.to_datetime)
    
    # inserting songplay records
    
    for index, row in df.iterrows():
        
        # getting songid and artistid from song and artist tables
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    
    This function processes all the files as needed for fact and dimension tables 
    
    Function Parameters: 
    
    cur: SQL/Database connection cursor
    conn: Database connection pointer
    filepath: OS path to the datasets
    func: placeholder argument for process_song_file and process_log_file functions defined above
    
    """
    
    # getting all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterating over files and process
    
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    
    The main function that drives the etl script by connecting to the database, initiating a DB cursor, 
    calling the process_data function once for processing song_data and log_data respectively and finally closing the DB connection.
    
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()