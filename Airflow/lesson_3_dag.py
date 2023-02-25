import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

login = 'd-dontsov'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'd.dontsov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 27),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def les_3_d_dontsov():

    @task()
    def get_data():
        df = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')
        my_df = df.query('Year == @year')
        return my_df

    @task()
    def get_best_game(my_df):
        best_game = my_df.groupby('Name')['Global_Sales'] \
                        .agg(['sum']) \
                        .sort_values('sum', ascending=False) \
                        .reset_index().head(1)['Name']
        return best_game

    @task()
    def get_best_genre(my_df):
        best_genres = my_df.groupby('Genre')['EU_Sales'] \
                            .agg(['sum']) \
                            .query('sum == sum.max()') \
                            .reset_index()['Genre']
        return best_genres

    @task()
    def best_platform(my_df):
        best_platforms = my_df.query('NA_Sales > 1') \
                                .groupby('Platform')['Name'] \
                                .agg(['nunique']) \
                                .query('nunique == nunique.max()') \
                                .reset_index()['Platform']
        return best_platforms

    @task()
    def get_best_jp_publ(my_df):
        best_jp_publishers = my_df.groupby('Publisher') \
                                    .agg({'JP_Sales': 'mean'}) \
                                    .query('JP_Sales == JP_Sales.max()') \
                                    .reset_index()['Publisher']
        return best_jp_publishers

    @task()
    def get_games_eu_better_jp(my_df):
        num_games_eu_better_jp = my_df.query('EU_Sales > JP_Sales') \
                                        .agg({'Name': 'nunique'})
        return num_games_eu_better_jp

    @task()
    def print_data(best_game, best_genres, best_platforms, best_jp_publishers, num_games_eu_better_jp):
        
        context = get_current_context()
        date    = context['ds']
        
        print(f'For {date} best selling game is {best_game} in {year}')
        print(f'For {date} best selling genres is/are {best_genres} in {year}')
        print(f'For {date} best selling platforms is/are {best_platforms} in {year}')
        print(f'For {date} best selling Japan publisher is/are {best_jp_publishers} in {year}')
        print(f'For {date} the number of game sales in the EU is better than in Japan {num_games_eu_better_jp} in {year}')

    my_df = get_data()
    best_game = get_best_game(my_df)
    best_genres = get_best_genre(my_df)
    best_platforms = best_platform(my_df)
    best_jp_publishers = get_best_jp_publ(my_df)
    num_games_eu_better_jp = get_games_eu_better_jp(my_df)
    print_data(best_game, best_genres, best_platforms, best_jp_publishers, num_games_eu_better_jp)

les_3_d_dontsov = les_3_d_dontsov()
