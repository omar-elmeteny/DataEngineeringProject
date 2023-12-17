import m1_functions as m1
import os
from sqlalchemy import create_engine, inspect
import pandas as pd

data_dir = "/data/"
original_file = data_dir + "green_tripdata_2018-11.csv"
cleaned_file = data_dir + "green_trip_data_2018-11clean.csv"
lookup_file = data_dir + "lookup_table_green_taxis.csv"
locations_file = data_dir + 'location_coordinates.csv'

def run_cleanup():
    print("Running cleanup...", flush=True)
    trip_df_original = m1.load_data(original_file)
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
    locations_dict = m1.load_coordinates(locations_file)
    print("loaded coordinates...", flush=True)
    trip_df['pu_latitude'], trip_df['pu_longitude'] = zip(*trip_df['pu_location'].apply(lambda l: m1.get_gps_coordinates_bylocation(l, locations_dict, locations_file)))
    print("added pickup coordinates...", flush=True)
    trip_df['do_latitude'], trip_df['do_longitude'] = zip(*trip_df['do_location'].apply(lambda l: m1.get_gps_coordinates_bylocation(l, locations_dict, locations_file)))
    print("added dropoff coordinates...", flush=True)
    m1.save_dataframe(trip_df, cleaned_file)
    print(f"saved cleaned dataframe to {cleaned_file}...", flush=True)
    m1.save_dataframe(lookup_table, lookup_file)
    print(f"saved lookup table to {lookup_file}...", flush=True)

if not os.path.isfile(cleaned_file) or not os.path.isfile(lookup_file):
    print(f"cleanup file {cleaned_file} or lookup file {lookup_file} do not exist!", flush=True)
    run_cleanup()



def save_dataframe_to_postgres(filename: str, table_name: str):
    host = 'db-service'
    port = '5432'
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    db_name = os.environ['POSTGRES_DB']

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

save_dataframe_to_postgres(cleaned_file, 'green_taxi_11_2018')
save_dataframe_to_postgres(lookup_file, 'lookup_green_taxi_11_2018')