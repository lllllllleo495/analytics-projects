import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import requests
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

year = 1994 + hash(f'l-sharipkov-25') % 23

CHAT_ID =-895086558
BOT_TOKEN = '5451339037:AAF-BXWu3AREyWhXq0jBhl5cza636KKj3Wo'


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

default_args = {
    'owner': 'l.sharipkov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 16),
    'schedule_interval': '0 12 * * *'
}

@dag(default_args=default_args, catchup=False)
def l_sharipkov_hw3():
    @task()
    def get_data():

        video_game = pd.read_csv(path)
        video_game = video_game.query('Year == @year')

        return video_game

    @task()
    #Какая игра была самой продаваемой в этом году во всем мире?
    def get_top_game(video_game):
        top_game = video_game.sort_values('Global_Sales',ascending=False).head(1)['Name'].values[0]
        return {'top_game': top_game}

    @task()
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def top_eu_genre(video_game):

        top_genre = video_game \
            .groupby(['Genre'],as_index=False) \
            .agg({'EU_Sales':'max'}) \
            .sort_values('EU_Sales',ascending=False).head(1).values[0][0]

        return {'top_genre':top_genre}

    @task()
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    #Перечислить все, если их несколько
    def get_na_platforms(video_game):

        top_NA_platforms = video_game \
            .query('NA_Sales > 1.00') \
            .groupby('Platform',as_index=False) \
            .Rank.count() \
            .rename(columns={'Rank':'Num_of_games'}) \
            .sort_values('Num_of_games',ascending=False)

        return top_NA_platforms.to_csv(index=False)

    @task()
    #У какого издателя самые высокие средние продажи в Японии?
    #Перечислить все, если их несколько
    def get_jp_publisher(video_game):

        top_JP_Sales_Publisher = video_game \
            .groupby('Publisher',as_index=False) \
            .JP_Sales.mean() \
            .rename(columns={'JP_Sales':'Mean_JP_Sales'}) \
            .sort_values('Mean_JP_Sales',ascending=False).head(5)

        return top_JP_Sales_Publisher.to_csv(index=False)

    @task()
    #Сколько игр продались лучше в Европе, чем в Японии?
    def get_games_eu_vs_jp(video_game):
        num_of_games = video_game.query('EU_Sales > JP_Sales').Name.nunique()
        return {'num_of_games':num_of_games}

    @task(on_success_callback=send_message)
    def print_data(task2, task3, task4, task5, task6):

        context = get_current_context()
        date = context['ds']

        top_game, top_eu_genre, num_of_games  = task2['top_game'], task3['top_genre'], task6['num_of_games']
        top_NA_platforms = task4
        top_JP_Sales_Publisher = task5

        print(f'{date}')
        print(f'Top sales game worldwide in {year}: {top_game}')
        print(f'Top genre in EU in {year}: {top_eu_genre}')  
        print(f'Top platforms in North America in {year}')
        print(top_NA_platforms)
        print(f'Top publisher in Japan in {year}')
        print(top_JP_Sales_Publisher)
        print(f'Number of Games EU vs. JP in {year}: {num_of_games}')


    video_game = get_data()

    task2 = get_top_game(video_game)
    task3 = top_eu_genre(video_game)
    task4 = get_na_platforms(video_game)
    task5 = get_jp_publisher(video_game)
    task6 = get_games_eu_vs_jp(video_game)

    print_data(task2, task3, task4, task5, task6)
                                   
l_sharipkov_hw3 = l_sharipkov_hw3()