import requests
import argparse
import logging
import pymysql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(message)s')

def _get_cli_args():
    """
    this function is developed so that in future to remove the hard coding of the variables and make it dynamic
    """
    parser=argparse.ArgumentParser()
    parser.add_argument('--api_secret',type=str,default="nds8furbpdvjgttc8rjev8hc")
    parser.add_argument('--zip_code',type=str,default="78701")
    parser.add_argument('--line_up_id',type=str,default="USA-TX42500-X")
    parser.add_argument('--user', type=str, default="root")
    parser.add_argument('--pw', type=str, default="#######")
    parser.add_argument('--db', type=str, default="MovieBuzz")
    return parser.parse_args()


def get_data_api_theatres(url1,engine):
    """
    This function is used to fetch the data for movies playing in Theatres and then convert that to dataframe
    and then dataframe is getting uploaded to the table
    """
    title=[]
    release_year=[]
    genres=[]
    description=[]
    theatres=[]
    try:
        r=requests.get(url1)
        logging.info(r.status_code)
        results=r.json()
        print(results[0])
        for i in range (20):
            title.append(results[i]['title'])
            release_year.append(results[i]['releaseYear'])
            genres.append(results[i]['genres'])
            description.append(results[i]['longDescription'])
            theatres.append(results[i]['showtimes'][0]['theatre']['name'])
        logging.info(title)
        logging.info(release_year)
        logging.info(genres)
        logging.info(description)
        logging.info(theatres)
        """
        Creating the dataframe by using all the list which will act as an individual column
        """
        df_theatres=pd.DataFrame(list(zip(title,release_year,genres,description,theatres)),
                                 columns=['title','release_year','genres','description','theatres'])
        df_theatres['genres'] = [','.join(map(str, l)) for l in df_theatres['genres']]
        # df_theatres.to_sql("theatres_tables",schema="moviebuzz",con=engine,if_exists="append",chunksize=None,index=False)
        logging.info("Data updated to the Theatres Table")
    except requests.exceptions.Timeout as e:
        logging.exception(f"Exception of type {type(e).__name__} is raised")

def get_data_api_tv(url2,engine):
    """
    This function is used to fetch the data for movies playing in TV then convert that to dataframe
    and then dataframe is getting uploaded to the table
    """
    title=[]
    release_year=[]
    genres=[]
    description=[]
    channels=[]
    try:
        r=requests.get(url2)
        logging.info(r.status_code)
        results=r.json()
        logging.info(results[0])
        for i in range(len(results)):
            title.append(results[i]['program']['title'])
            release_year.append(results[i]['program']['releaseYear'])
            genres.append(results[i]['program']['genres'])
            description.append(results[i]['program']['longDescription'])
            channels.append(results[i]['channels'])
        logging.info(title)
        logging.info(release_year)
        logging.info(genres)
        logging.info(description)
        logging.info(channels)
        """
        Creating the dataframe by using all the list which will act as an individual column
        """
        df_tv = pd.DataFrame(list(zip(title, release_year, genres, description, channels)),
                             columns=['title','release_year','genres','description','channels'])

        df_tv['channels'] = [','.join(map(str, l)) for l in df_tv['channels']]
        df_tv['genres'] = [','.join(map(str, l)) for l in df_tv['genres']]
        # df_tv.to_sql('tv_tables',schema='moviebuzz', con=engine, if_exists="append", chunksize=None,index=False)
        logging.info("Data updated to the TV table")
    except requests.exceptions.Timeout as e:
        logging.exception(f"Exception of type {type(e).__name__} is raised")

def exec_query(cursor,conn):
    """
    Execute query to Combine the movie lists (Theatre and Channel movies) based on the
    Genres and find out the top 5 genres with most movies count
    """
    query_1="insert into moviebuzz.combined_table " \
            "(select theatres_tables.title as theatres_movies,tv_tables.title as channels_movies ," \
            "theatres_tables.genres from theatres_tables inner join tv_tables on theatres_tables.genres=tv_tables.genres );"
    query_2="select * from moviebuzz.combined_table "
    cursor.execute(query_1)
    conn.commit()
    logging.info("Combined Movie list on the basis of Genres is Inserted to Combined table")
    df=pd.read_sql(query_2,conn)
    logging.info(f"Top 5 genres with count are {df['genres'].value_counts().head(5)}")

def main():
    args=_get_cli_args()
    api_secret=args.api_secret
    zip_code=args.zip_code
    start_date=datetime.today().strftime('%Y-%m-%d')
    line_up_id=args.line_up_id
    date_time=datetime.now().strftime("%Y-%m-%dT%H:%MZ")
    user=args.user
    pw=args.pw
    db=args.db
    engine = create_engine(f"mysql+pymysql://{user}:{pw}@localhost/{db}")
    conn=pymysql.connect(database=db, user=user, password=pw, host='localhost', port=3306)
    cursor=conn.cursor()
    url1=f"http://data.tmsapi.com/v1.1/movies/showings?startDate={start_date}&zip={zip_code}&api_key={api_secret}"
    url2=f"http://data.tmsapi.com/v1.1/movies/airings?lineupId={line_up_id}&startDateTime={date_time}&api_key={api_secret}"
    get_data_api_tv(url2, engine)
    get_data_api_theatres(url1,engine)
    exec_query(cursor,conn)

if __name__ == '__main__':
    main()