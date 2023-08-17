# Integrating the Strava API pulling logic with the database insertion and processing steps

import psycopg2
import pandas as pd
import requests
import json
import time
import configparser
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def hours_to_interval(hours_float):
    """Function to convert hours to interval string."""
    whole_hours = int(hours_float)
    fraction_hour = hours_float - whole_hours
    minutes = int(fraction_hour * 60)
    interval_string = f"{whole_hours} hours {minutes} minutes"
    return interval_string


# PostgreSQL connection details
conn = psycopg2.connect(
    host="localhost",
    user="####",
    password="####",
    database="####",
    port="####"
)
cursor = conn.cursor()

# Strava API connection details
config = configparser.ConfigParser()
config.read("config.cfg")

with open('strava_tokens.json') as json_file:
    strava_tokens = json.load(json_file)

if strava_tokens['expires_at'] < time.time():
    response = requests.post(
        url='https://www.strava.com/oauth/token',
        data={
            'client_id': config.get("CLIENT_ID", 'client_id'),
            'client_secret': config.get("CLIENT_SECRET", 'client_secret'),
            'grant_type': 'refresh_token',
            'refresh_token': strava_tokens['refresh_token']
        }
    )
    new_strava_tokens = response.json()
    with open('strava_tokens.json', 'w') as outfile:
        json.dump(new_strava_tokens, outfile)
    strava_tokens = new_strava_tokens

# Get activities from Strava API
logging.info("Fetching activities from Strava API...")

page = 1
url = "https://www.strava.com/api/v3/activities"
access_token = strava_tokens['access_token']
activities = pd.DataFrame(
    columns=[
        "id",
        "start_date_local",
        "type",
        "distance",
        "elapsed_time"
    ]
)

while True:
    response = requests.get(url + '?access_token=' + access_token + '&per_page=200' + '&page=' + str(page))
    # Error handling for the API request
    if response.status_code != 200:
        logging.error(f"Error fetching activities for page {page}. Status code: {response.status_code}")
        break

    r = response.json()
    if (not r):
        logging.info(f"Finished fetching activities. Total pages: {page - 1}")
        break

    new_data = pd.DataFrame(r)
    activities = pd.concat([activities, new_data], ignore_index=True)
    logging.info(f"Fetched {len(r)} activities from page {page}")
    page += 1

# Convert the activities to a DataFrame
logging.info(f"Converted {len(activities)} activities to a DataFrame.")
df = pd.DataFrame(activities)
# Filter for activities after 8/1/2023
# Convert 'start_date_local' to a date format
df['date'] = pd.to_datetime(df['start_date_local']).dt.date
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

# Filter for activities after 8/1/2023
df = df[df['date'] > '2023-08-01']

# Apply necessary transformations
df['elapsed_time'] = df['elapsed_time'].apply(
    lambda x: hours_to_interval(x / 3600))  # Convert seconds to hours, then to interval string
df['distance'] = df['distance'] * 0.000621371  # Convert meters to miles
df['ShoeId'] = df['type'].map({'Run': 1, 'Walk': 5, 'Hike': 2})

# Reorder columns to match database table structure
desired_column_order = ['id', 'ShoeId', 'type', 'distance', 'elapsed_time',
                        'start_date_local']
df = df[desired_column_order]

# Insert data into PostgreSQL database
logging.info("Inserting data into PostgreSQL database...")
for index, row in df.iterrows():
    cursor.execute("SELECT * FROM Activities WHERE ActivityId = %s", (row['id'],))
    if not cursor.fetchone():  # Check if the activity already exists
        logging.info(f"Inserting activity with ID: {row['id']}")
        cursor.execute("""
            INSERT INTO Activities (ActivityId, ShoeId, Type, Distance, Time, Date, WithDog)
            VALUES (%s, %s, %s, %s, %s, %s, 'f');
        """, (row['id'], row['ShoeId'], row['type'], row['distance'], row['elapsed_time'], row['start_date_local']))

    else:
        logging.info(f"Activity with ID: {row['id']} already exists in the database.")
conn.commit()
logging.info("Data insertion complete!")
conn.close()
