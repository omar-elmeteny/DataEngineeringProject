import pandas as pd
from scipy.stats import linregress
from sklearn.preprocessing import LabelEncoder
import datetime
import os
import requests


import warnings
warnings.filterwarnings('ignore')

def load_data(data_dir: str):
    df = pd.read_csv(data_dir)
    return df

def create_lookup_table():
    lookup = pd.DataFrame(columns=['Column name', 'Original value', 'Imputed value'])
    return lookup


def update_lookup_table(table, column_name, original_value, imputed_value):
    table.loc[len(table)] ={'Column name': column_name, 'Original value': original_value, 'Imputed value': imputed_value}


def rename_columns(df):
    df.columns = df.columns.str.lower()
    df.columns = [col.replace(' ', '_') for col in df.columns]
    return df



def drop_duplicates(df: pd.DataFrame):
    return df.drop_duplicates()


def correct_negative_values(df: pd.DataFrame):
    for col in df.columns:
        if(df[col].dtype == 'float64'):
            df[col] = df[col].abs()    
    return df



def correct_uknown_payment_type(df: pd.DataFrame):
    df['payment_type'].replace("Uknown", "Unknown", inplace=True)
    return df



def correct_lpep_pickup_datetime(df: pd.DataFrame):
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
    df.drop(df[(df['lpep_pickup_datetime'].dt.year != 2018) | (df['lpep_pickup_datetime'].dt.month != 11)].index, inplace=True)
    return df



def correct_passenger_count(df: pd.DataFrame):
    df['passenger_count'].replace(333, 3, inplace=True)
    return df



def impute_congestion_surcharge(df: pd.DataFrame):
    df['congestion_surcharge'].fillna(0.0, inplace=True)
    return df



def impute_ehail_fee(df: pd.DataFrame):
    df['ehail_fee'].fillna(0, inplace=True)
    return df



def impute_extra(df: pd.DataFrame):
    df['extra'].fillna(df.groupby(['payment_type', 'rate_type'])['extra'].transform('median'), inplace=True)
    df['extra'].fillna(0, inplace=True)
    return df



def impute_payment_type(lookup_table: pd.DataFrame, df: pd.DataFrame):
    mode = df['payment_type'].mode()[0]
    df['payment_type'].fillna(mode, inplace=True)
    update_lookup_table(lookup_table, 'payment_type', 'None', mode)
    return df



def impute_null_passenger_count(lookup_table: pd.DataFrame, df: pd.DataFrame):
    mode = df['passenger_count'].mode()[0]
    df['passenger_count'].fillna(mode, inplace=True)
    update_lookup_table(lookup_table, 'passenger_count', 'NaN', mode)
    return df 



def impute_store_and_fwd_flag(lookup_table: pd.DataFrame, df: pd.DataFrame):
    mode = df['store_and_fwd_flag'].mode()[0]
    df['store_and_fwd_flag'].fillna(mode, inplace=True)
    update_lookup_table(lookup_table, 'store_and_fwd_flag', 'None', mode)
    return df



def correct_passenger_count_outliers(df: pd.DataFrame):
    lower = df['passenger_count'].mean() - 4 * df['passenger_count'].std()
    upper = df['passenger_count'].mean() + 4 * df['passenger_count'].std()
    df_new = df[(df['passenger_count'] > lower) & (df['passenger_count'] < upper)]
    median = df_new['passenger_count'].median()
    df['passenger_count'] = df['passenger_count'].apply(lambda x: median if (x < lower or x > upper) else x)
    return df



def correct_mta_tax_outlier(df: pd.DataFrame):
    median = df['mta_tax'].median()
    df_new = df[df['mta_tax'] != median]
    lower = df_new['mta_tax'].mean() - 4 * df_new['mta_tax'].std()
    upper = df_new['mta_tax'].mean() + 4 * df_new['mta_tax'].std()
    df['mta_tax'] = df['mta_tax'].apply(lambda x: median if (x < lower or x > upper) else x)
    return df



def correct_extra_outliers(df: pd.DataFrame):
    rate_types = df['rate_type'].unique()
    payment_type = df['payment_type'].unique()
    for rate_type in rate_types:
        for payment in payment_type:
            df_temp = df[(df['rate_type'] == rate_type) & (df['payment_type'] == payment)]
            lower = df_temp['extra'].mean() - 3 * df_temp['extra'].std()
            upper = df_temp['extra'].mean() + 3 * df_temp['extra'].std()
            median = df_temp['extra'].median()
            df['extra'] = df['extra'].apply(lambda x: median if (x < lower or x > upper) else x)
    return df



def correct_tip_amount_outliers(df: pd.DataFrame):
    lower = df['tip_amount'].mean() - 3 * df['tip_amount'].std()
    upper = df['tip_amount'].mean() + 3 * df['tip_amount'].std()
    a = df[(df['tip_amount'] >= lower) & (df['tip_amount'] <= upper)].groupby(['payment_type'])['tip_amount'].mean()
    for col in a.index:
        a[col] = round(a[col], 1)
    df['tip_amount'] = df['tip_amount'].apply(lambda x: None if (x < lower or x > upper) else x)
    df['tip_amount'].fillna(df['payment_type'].map(a), inplace=True)
    return df



def correct_tolls_amount_outliers(df: pd.DataFrame):
    df_new = df[df['tolls_amount'] != 0]
    series = df_new['tolls_amount']
    lower = series.mean() - 3 * series.std()
    upper = series.mean() + 3 * series.std()
    mode = df_new['tolls_amount'].mode()
    df.loc[((df['tolls_amount'] < lower) | (df['tolls_amount'] > upper)) & (df['tolls_amount'] != 0), 'tolls_amount'] = mode[0]
    return df



def correct_trip_distance_outliers(df: pd.DataFrame):
    lower = df['trip_distance'].mean() - 5 * df['trip_distance'].std()
    upper = df['trip_distance'].mean() + 5 * df['trip_distance'].std()
    df_new = df[(df['trip_distance'] >= lower) & (df['trip_distance'] <= upper)]
    mean = df_new['trip_distance'].mean()
    mean = round(mean, 2)
    df['trip_distance'] = df['trip_distance'].apply(lambda x: mean if (x < lower or x > upper) else x)
    return df



def calculate_fare_amount(distance, slope, intercept):
    return slope * distance + intercept

def is_fare_amount_outlier(distance, fare_amount, slope, intercept):
    fixed_fare_amount = calculate_fare_amount(distance, slope, intercept)
    return (fixed_fare_amount != 0) & ((abs(fare_amount - fixed_fare_amount) / fixed_fare_amount) > 0.8)



def correct_fare_amount_outliers(df: pd.DataFrame):
    slope, intercept, rvalue, pvalue, stderr = linregress(df['trip_distance'], df['fare_amount'])
    df_new = df[~is_fare_amount_outlier(df['trip_distance'], df['fare_amount'], slope, intercept)]
    slope_new, intercept_new, rvalue, pvalue, stderr = linregress(df_new['trip_distance'], df_new['fare_amount'])
    
    df['fare_amount'] = df.apply(lambda row: round(calculate_fare_amount(row['trip_distance'], slope_new,intercept_new), 2) if (is_fare_amount_outlier(row['trip_distance'], row['fare_amount'], slope, intercept)) else row['fare_amount'], axis=1)
    return df



def correct_total_amount(df: pd.DataFrame):
    df['total_amount'] = df['fare_amount'] + df['extra'] + df['mta_tax'] + df['tip_amount'] + df['tolls_amount'] + df['ehail_fee'] + df['improvement_surcharge'] + df['congestion_surcharge']
    return df



def discretize_column(lookup_table: pd.DataFrame, df: pd.DataFrame, col: str, bins: list):
    df[col+"_range"] = pd.cut(df[col], bins=(len(bins)), labels=False)
    for i in range(len(bins)):
        update_lookup_table(lookup_table, col + "_range", bins[i], i)
    return df



def add_week_number(df: pd.DataFrame):
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
    year, month, day = df['lpep_pickup_datetime'].dt.year, df['lpep_pickup_datetime'].dt.month, df['lpep_pickup_datetime'].dt.day
    dataframe = pd.DataFrame({'year': year, 'month': month, 'day': day})
    dataframe['week_number'] = dataframe.apply(lambda row: datetime.date(row['year'], row['month'], row['day']).isocalendar()[1], axis=1)
    df['week_number'] = dataframe['week_number']
    return df



def add_date_range(df: pd.DataFrame):
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
    year, month, day = df['lpep_pickup_datetime'].dt.year, df['lpep_pickup_datetime'].dt.month, df['lpep_pickup_datetime'].dt.day
    dataframe = pd.DataFrame({'year': year, 'month': month, 'day': day})
    dataframe['week_number'] = dataframe.apply(lambda row: datetime.date(row['year'], row['month'], row['day']).isocalendar()[1], axis=1)
    dataframe['date_range'] = dataframe.apply(lambda row: [datetime.datetime.fromisocalendar(row['year'], row['week_number'], 1).date(), datetime.datetime.fromisocalendar(row['year'], row['week_number'], 7).date()], axis=1)
    df['date_range'] = dataframe['date_range']
    return df



def encode_store_and_fwd_flag(lookup_table: pd.DataFrame, df: pd.DataFrame):
    dict = {'N': 0, 'Y': 1}
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].map(dict)
    
    for key, value in dict.items():
        update_lookup_table(lookup_table, 'store_and_fwd_flag', key, value)
    return df



def one_hot_encode(df: pd.DataFrame, col: str):
    df_dummies = pd.get_dummies(df[col], prefix=col).astype(int)
    df.drop([col], axis=1, inplace=True)
    df[df_dummies.columns] = df_dummies
    return df



def label_encode(df: pd.DataFrame):
    le = LabelEncoder()

    
    combined = pd.concat([df['pu_location'], df['do_location']])

    
    le.fit(combined)

    
    df['pu_location_labels'] = le.transform(df['pu_location'])
    df['do_location_labels'] = le.transform(df['do_location'])
    
    return df



def get_gps_coordinates_from_api(location):
    print(f"Getting coordinates for {location}", flush=True)
    
    if "unknown" in location.lower():
        return 360, 360
    
    api_key = os.environ.get('GOOGLE_API_KEY')
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    endpoint = f"{base_url}?address={location}&key={api_key}"
    response = requests.get(endpoint)
    if response.status_code not in range(200, 299):
        return None
    try:

        loc = response.json()['results'][0]['geometry']['location']
        result = loc['lat'], loc['lng']
        print(f"Coordinates for {location} is {result}", flush=True)
        return result
    except:
        pass
    print(f"Coordinates for {location} is None", flush=True)
    return None

def get_gps_coordinates_from_cache(location, locations_df):
    location_data = locations_df.loc[locations_df['location'] == location]
    if not location_data.empty:
        return location_data.iloc[0]['latitude'], location_data.iloc[0]['longitude']
    else:
        return None

def load_coordinates(filename):
    
    if not os.path.isfile(filename):
        return {}
    
    df = pd.read_csv(filename)

    
    coordinates_dict = df.set_index('location')[['latitude', 'longitude']].T.to_dict('list')

    return coordinates_dict

def save_coordinates_to_csv(coordinates_dict, filename):
    
    df = pd.DataFrame.from_dict(coordinates_dict, orient='index', columns=['latitude', 'longitude'])

    
    df.reset_index(inplace=True)

    
    df.columns = ['location', 'latitude', 'longitude']

    
    df.to_csv(filename, index=False)

def get_gps_coordinates(location, coordinates_dict, filename):
    
    
    if location in coordinates_dict:
        coordinates = coordinates_dict[location]
    else:
        
        coordinates = get_gps_coordinates_from_api(location)
        if coordinates == None:
            coordinates = (0, 0)
        
        coordinates_dict[location] = coordinates
        
        
        save_coordinates_to_csv(coordinates_dict, filename)

    return coordinates



def get_gps_coordinates_bylocation(location, locations_dict, locations_filename):
    return get_gps_coordinates(location, locations_dict, locations_filename)
    


def save_dataframe(df: pd.DataFrame, filename: str):
    df.to_csv(filename, index=False)