from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import m1_functions as m1
import os
from sqlalchemy import create_engine, inspect
import pandas as pd

import dash
import dash_core_components as dcc
import dash_html_components as html

def extract_clean(data_file_name, cleaned_file_name, lookup_file_name):
    if os.path.exists(cleaned_file_name):
        print("Cleaned file already exists. Skipping cleaning...", flush=True)
        return
    print("Running cleanup...", flush=True)
    trip_df_original = m1.load_data(data_file_name)
    print("Loaded original trip data...", flush=True)
    trip_df = trip_df_original.copy()
    print("created a copy of trip data...", flush=True)
    lookup_table = m1.create_lookup_table()
    print("created lookup table...", flush=True)
    m1.rename_columns(trip_df)
    print("renamed columns...", flush=True)
    trip_df = m1.drop_duplicates(trip_df)
    print("dropped duplicates...", flush=True)
    m1.correct_negative_values(trip_df)
    print("corrected negative values...", flush=True)
    m1.correct_uknown_payment_type(trip_df)
    print("corrected unknown payment type...", flush=True)
    m1.correct_lpep_pickup_datetime(trip_df)
    print("corrected pickup datetime...", flush=True)
    m1.correct_passenger_count(trip_df)
    print("corrected passenger count...", flush=True)
    m1.impute_congestion_surcharge(trip_df)
    print("imputed congestion surcharge...", flush=True)
    m1.update_lookup_table(lookup_table, 'congestion_surcharge', 'NaN', 0.0)
    print("updated lookup table with congestion_surcharge...", flush=True)
    m1.impute_ehail_fee(trip_df)
    print("imputed ehail fee...", flush=True)
    m1.update_lookup_table(lookup_table, 'ehail_fee', 'NaN', 0.0)
    print("updated lookup table with ehail_fee...", flush=True)
    m1.impute_extra(trip_df)
    print("imputed extra...", flush=True)
    m1.impute_payment_type(lookup_table, trip_df)
    print("imputed payment type...", flush=True)
    m1.impute_null_passenger_count(lookup_table, trip_df)
    print("imputed null passenger count...", flush=True)
    m1.impute_store_and_fwd_flag(lookup_table, trip_df)
    print("imputed store and fwd flag...", flush=True)
    m1.correct_passenger_count_outliers(trip_df)
    print("corrected passenger count outliers...", flush=True)
    m1.correct_mta_tax_outlier(trip_df)
    print("corrected mta tax outliers...", flush=True)
    m1.correct_extra_outliers(trip_df)
    print("corrected extra outliers...", flush=True)
    trip_df = m1.correct_tip_amount_outliers(trip_df)
    print("corrected tip amount outliers...", flush=True)
    m1.correct_tolls_amount_outliers(trip_df)
    print("corrected tolls amount outliers...", flush=True)
    m1.correct_trip_distance_outliers(trip_df)
    print("corrected trip distance outliers...", flush=True)
    m1.correct_fare_amount_outliers(trip_df)
    print("corrected fare amount outliers...", flush=True)
    m1.correct_total_amount(trip_df)
    print("corrected total amount...", flush=True)
    m1.discretize_column(lookup_table, trip_df, 'trip_distance', ['Short', 'Medium', 'Long', 'Very Long', 'Extremely Long'])
    print("discretized trip distance...", flush=True)
    m1.discretize_column(lookup_table, trip_df, 'total_amount', ['Very Low', 'Low', 'Medium', 'High', 'Very High'])
    print("discretized total amount...", flush=True)
    m1.add_week_number(trip_df)
    print("added week number...", flush=True)
    m1.add_date_range(trip_df)
    print("added date range...", flush=True)
    m1.encode_store_and_fwd_flag(lookup_table, trip_df)
    print("encoded store and fwd flag...", flush=True)
    m1.one_hot_encode(trip_df, 'payment_type')
    print("one hot encoded payment type...", flush=True)
    m1.one_hot_encode(trip_df, 'rate_type')
    print("one hot encoded rate type...", flush=True)
    m1.one_hot_encode(trip_df, 'trip_type')
    print("one hot encoded trip type...", flush=True)
    m1.one_hot_encode(trip_df, 'vendor')
    print("one hot encoded vendor...", flush=True)
    m1.label_encode(trip_df)
    print("label encoded...", flush=True)
    m1.rename_columns(trip_df)
    print("renamed columns...", flush=True)
    m1.save_dataframe(trip_df, cleaned_file_name)
    print(f"saved cleaned dataframe to {cleaned_file_name}...", flush=True)
    m1.save_dataframe(lookup_table, lookup_file_name)
    print(f"saved lookup table to {lookup_file_name}...", flush=True)

def add_gps_locations(cleaned_file_name, transformed_file_name, locations_file_name):
    if os.path.exists(transformed_file_name):
        print("Transformed file already exists. Skipping transformation...", flush=True)
        return
    locations_dict = m1.load_coordinates(locations_file_name)
    print("loaded coordinates...", flush=True)
    trip_df = m1.load_data(cleaned_file_name)
    trip_df['pu_latitude'], trip_df['pu_longitude'] = zip(*trip_df['pu_location'].apply(lambda l: m1.get_gps_coordinates_bylocation(l, locations_dict, locations_file)))
    print("added pickup coordinates...", flush=True)
    trip_df['do_latitude'], trip_df['do_longitude'] = zip(*trip_df['do_location'].apply(lambda l: m1.get_gps_coordinates_bylocation(l, locations_dict, locations_file)))
    print("added dropoff coordinates...", flush=True)
    m1.save_dataframe(trip_df, transformed_file_name)
    print(f"saved transformed dataframe to {transformed_file_name}...", flush=True)

def load_to_postgres(filename, table_name): 
    host = os.environ['GREEN_TAXI_PG_HOST']
    port = '5432'
    user = os.environ['GREEN_TAXI_PG_USER']
    password = os.environ['GREEN_TAXI_PG_PASSWORD']
    db_name = os.environ['GREEN_TAXI_PG_DB']

    # Create the engine
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')

    # Save the DataFrame to the database
    if not inspect(engine).has_table(table_name):
        print(f"Table {table_name} does not exist. Creating it...", flush=True)
        # Load the DataFrame
        df = pd.read_csv(filename)
        connection = engine.connect()
        df.to_sql(table_name, connection)
        print(f"Table {table_name} created!")
    else:
        print(f"Table {table_name} already exists. Skipping copying data...", flush=True)

def create_dashboard(filename):
    df = pd.read_csv(filename)

    # Group by pickup and dropoff locations and count the number of rows
    route_counts = df.groupby(['pu_location', 'do_location']).size().reset_index(name='trip_count')

    # Sort by trip count and limit to top 10
    top_10_routes = route_counts.sort_values('trip_count', ascending=False).head(10)
    # Create 'route' column
    top_10_routes['route'] = top_10_routes.apply(lambda row: f"{row['pu_location']} to {row['do_location']}", axis=1)
    
    average_tip = df.groupby('passenger_count')['tip_amount'].mean().reset_index(name='average_tip')
    average_fare = df.groupby('passenger_count')['total_amount'].mean().reset_index(name='average_fare')

    # Convert 'date' column to datetime format
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])

    # Extract day of the week
    df['day_of_week'] = df['lpep_pickup_datetime'].dt.day_name()

    # Group by day of the week and count the number of rows
    day_of_week_counts = df.groupby('day_of_week').size().reset_index(name='trip_count')

    # Create 'day_type' column
    df['day_type'] = df['day_of_week'].apply(lambda x: 'Weekend' if x in ['Saturday', 'Sunday'] else 'Weekday')

    # Group by 'day_type' and calculate average distance
    average_distance = df.groupby('day_type')['trip_distance'].mean().reset_index(name='average_distance')

    app = dash.Dash()
    app.layout = html.Div(
        children=[
            html.H1(
                children="Green Taxi DataSet",
                style={"text-align" : "center"},
            ),
            html.H2(
                children="Omar Sherif Elmeteny (MET 49-3317)",
                style={"text-align" : "center"},
            ),
            html.H3(
                children="Top 10 Routes and their trip counts",
                style={"text-align" : "center"},
            ),
            dcc.Graph(
                figure={
                    "data": [
                        {
                            "x": top_10_routes['route'],
                            "y": top_10_routes['trip_count'],
                            "type": "bar",
                        },
                    ],
                    "layout": {"title": "Route vs Trip Count"},
                },
            ),
            html.H2(
                children="Omar Sherif Elmeteny (MET 49-3317)",
                style={"text-align" : "center"},
            ),
            html.H3(
                children="Average Tip Amount by Passenger Count",
                style={"text-align" : "center"}
            ),
            dcc.Graph(
                figure={
                    "data": [
                        {
                            "x": average_tip['passenger_count'],
                            "y": average_tip['average_tip'],
                            "type": "bar",
                        },
                    ],
                    "layout": {"title": "Passenger Count vs Tip Amount"},
                },
            ),
            html.H2(
                children="Omar Sherif Elmeteny (MET 49-3317)",
                style={"text-align" : "center"}
            ),
            html.H3(
                children="Total Fare Amount by Passenger Count",
                style={"text-align" : "center"}
            ),
            dcc.Graph(
                figure={
                    "data": [
                        {
                            "x": average_fare['passenger_count'],
                            "y": average_fare['average_fare'],
                            "type": "bar",
                        },
                    ],
                    "layout": {"title": "Passenger Count vs Fare Amount"},
                },
            ),
            html.H2(
                children="Omar Sherif Elmeteny (MET 49-3317)",
                style={"text-align" : "center"}
            ),
            html.H3(
                children="Trip Count by Day of the Week",
                style={"text-align" : "center"}
            ),
            dcc.Graph(
                figure={
                    "data": [
                        {
                            "x": day_of_week_counts['day_of_week'],
                            "y": day_of_week_counts['trip_count'],
                            "type": "bar",
                        },
                    ],
                    "layout": {"title": "Day of the Week vs Trip Count"},
                },
            ),
            html.H2(
                children="Omar Sherif Elmeteny (MET 49-3317)",
                style={"text-align" : "center"}
            ),
            html.H3(
                children="Average distance travelled by on Weekdays and Weekends",
                style={"text-align" : "center"}
            ),
            dcc.Graph(
                figure={
                    "data": [
                        {
                            "x": average_distance['day_type'],
                            "y": average_distance['average_distance'],
                            "type": "bar",
                        },
                    ],
                    "layout": {"title": "Average Distance vs Day Type"},
                },
            ),
        ]
    )
    app.run_server(host='0.0.0.0')
    print('dashboard is successful and running on port 8000')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

green_taxi_file = '/opt/airflow/data/green_tripdata_2018-11.csv'
green_taxi_cleaned_file = '/opt/airflow/data/green_tripdata_2018-11_clean.csv'
green_taxi_transformed_file = '/opt/airflow/data/green_tripdata_2018-11_transformed.csv'
lookup_file = '/opt/airflow/data/lookup_green_taxi_11_2018.csv'
locations_file = '/opt/airflow/data/gps_locations.csv'
lookup_table_name = 'lookup_green_taxi_11_2018'
green_taxi_table_name = 'green_taxi_11_2018'

dag = DAG(
    'green_taxi_etl_pipeline',
    default_args=default_args,
    description='Green Taxi ETL pipeline',
)
with DAG(
    dag_id = 'green_taxi_etl_pipeline',
    schedule_interval = '@once',
    default_args = default_args,
    tags = ['green_taxi-pipeline'],
)as dag:
    extract_clean_task= PythonOperator(
        task_id = 'extract_dataset',
        python_callable = extract_clean,
        op_kwargs={
            "data_file_name": green_taxi_file,
            "cleaned_file_name": green_taxi_cleaned_file,
            "lookup_file_name": lookup_file
        },
    )
    transform_task= PythonOperator(
        task_id = 'transform_dataset',
        python_callable = add_gps_locations,
        op_kwargs={
            "cleaned_file_name": green_taxi_cleaned_file,
            "transformed_file_name": green_taxi_transformed_file,
            "locations_file_name": locations_file
        },
    )
    load_data_to_postgres_task=PythonOperator(
        task_id = 'load_data_to_postgres',
        python_callable = load_to_postgres,
        op_kwargs={
            "filename": green_taxi_transformed_file,
            "table_name": green_taxi_table_name
        },
    )
    load_lookup_to_postgres_task=PythonOperator(
        task_id = 'load_lookup_to_postgres',
        python_callable = load_to_postgres,
        op_kwargs={
            "filename": lookup_file,
            "table_name": lookup_table_name
        },
    )
    create_dashboard_task= PythonOperator(
        task_id = 'create_dashboard_task',
        python_callable = create_dashboard,
        op_kwargs={
            "filename": green_taxi_transformed_file
        },
    )
    


extract_clean_task >> transform_task >> load_data_to_postgres_task >> load_lookup_to_postgres_task >> create_dashboard_task