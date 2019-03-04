import pandas as pd
from faker import Faker
from faker.providers import internet, date_time
import uuid
import numpy as np
import numpy.random
import random
import string

# Helper to generate some random data
fake = Faker()
fake.add_provider(internet)
fake.add_provider(date_time)

def adding_some_na(v, ratio_na):
    '''
    Add some missing values for a fraction of data (defined by ratio_na)
    '''
    filter_na = np.random.choice([False, True], size=len(v), p=[ratio_na, 1-ratio_na]).astype(bool)
    v[filter_na==False] = np.nan
    return v

def generator(data, nb_row):
    '''
    Take as input a generator and will generate as many rows as 'nb_row'
    If the data in not "callable", it should be a list of length 'nb_row'
    '''
    if callable(data):
        data = pd.Series([str(data()) for _ in range(nb_row)])
    else:
        if len(data) != nb_row:
            raise ValueError('data should be the same length as nb_row')
        data = pd.Series(data)

    return data

def turn_meta_in_df(metadata, size):
    '''
    Given some metadata given in a dict, return a dataframe
    the key of metadata is the name of the column. The value of the dict should be:
        - data: a callable to generate data or a list
        - ratio_na: percentage of data that will become np.nan
    e.g., turn_meta_in_df({'age': {data:[12,76,32,14], ratio_na:0.3}, 'email': {data:fake.email, ratio_na:0.5}})
    will return a df with 4 rows and two columns 'age' and 'email' (50% of email will become np.nan)
    '''
    df = {}
    for name, meta in metadata.items():
        df[name] = adding_some_na(generator(meta['data'], nb_row=size), ratio_na=meta['ratio_na'])
    return pd.DataFrame(df)

# Generate some customers
nb_customer = 100
customer_meta_data = {
    'CUSTOMER_id': {'data':uuid.uuid4, 'ratio_na':0.0},
    'CUSTOMER_first_name': {'data':fake.first_name, 'ratio_na':0.05},
    'CUSTOMER_last_name': {'data':fake.last_name, 'ratio_na':0.05},
    'CUSTOMER_country': {'data':fake.country, 'ratio_na':0.1},
    'CUSTOMER_email': {'data':fake.email, 'ratio_na':0.05},
    'CUSTOMER_address': {'data':fake.address, 'ratio_na':0.05},
    'CUSTOMER_url': {'data':fake.url, 'ratio_na':0.05},
    'CUSTOMER_member_since': {'data':fake.year, 'ratio_na':0.},
    'CUSTOMER_subscription_plan': {'data':np.random.choice(['plan A', 'plan B', 'Plan C'], size=nb_customer, p=[0.7,0.2,0.1]), 'ratio_na':0.05},
    'CUSTOMER_gender': {'data':np.random.choice(['male', 'female'], size=nb_customer, p=[0.5,0.5]), 'ratio_na':0.05},
    'CUSTOMER_favorite_color': {'data':fake.color_name, 'ratio_na':0.2},
}
customer = turn_meta_in_df(customer_meta_data, nb_customer)
customer.set_index('CUSTOMER_id', drop=False, inplace=True)

# Generate some journeys
nb_journey = 1000
journey_meta_data = {
    'JOURNEY_id': {'data':uuid.uuid4, 'ratio_na':0.0},
    'CUSTOMER_id': {'data':np.random.choice(customer['CUSTOMER_id'], size=nb_journey), 'ratio_na':0.0},
    'JOURNEY_satisfaction': {'data':np.random.choice(['very low', 'low', 'neutral', 'high', 'very high'], size=nb_journey, p=[0.15,0.15,0.4,.15,.15]), 'ratio_na':0.05},
    'JOURNEY_textual_feedback': {'data':fake.text, 'ratio_na':0.9},
    'JOURNEY_cost': {'data':np.random.randint(5,50000,size=nb_journey)/100, 'ratio_na':0.2},
    'JOURNEY_success': {'data':np.random.randint(0,2,size=nb_journey).astype(bool), 'ratio_na':0.05},
    'JOURNEY_Sales_store_attribution': {'data':np.random.choice(['store a', 'store b', 'store c', 'store d', 'store e', 'store f', 'store g'], size=nb_journey), 'ratio_na':0.05},
}
journey = turn_meta_in_df(journey_meta_data, nb_journey)
journey.set_index('JOURNEY_id', drop=False, inplace=True)

# Generate some events
nb_event = 10000

event_meta_data = {
    'EVENT_id': {'data':uuid.uuid4, 'ratio_na':0.0},
    'JOURNEY_id': {'data':np.random.choice(journey['JOURNEY_id'], size=nb_event), 'ratio_na':0.0},
    'EVENT_activity': {'data': [random.choice(string.ascii_letters) for _ in range(nb_event)], 'ratio_na':0},
    'EVENT_satisfaction': {'data':np.random.choice(['very low', 'low', 'neutral', 'high', 'very high'], size=nb_event, p=[0.15,0.15,0.4,.15,.15]), 'ratio_na':0.05},
    'EVENT_text_associated': {'data':fake.text, 'ratio_na':0.95},
    'EVENT_geo-location': {'data': fake.latlng, 'ratio_na':0.8},
    'EVENT_timestamp': {'data': fake.date_time, 'ratio_na':0},
}
event = turn_meta_in_df(event_meta_data, nb_event)
event.set_index('EVENT_id', drop=False, inplace=True)

# Joining the customers/journeys/events together
journey = journey.join(customer, on='CUSTOMER_id', lsuffix='_left')
journey.drop(['CUSTOMER_id_left'], inplace=True, axis=1)
event = event.join(journey, on='JOURNEY_id', lsuffix='_left')
event.drop(['JOURNEY_id_left'], inplace=True, axis=1)

#Exporting the result in a CSV
event.to_csv('sample1.csv')
print (event)

#Exporting a CSV that describes the data
meta_description = []
for col in event.columns:
    m = {}
    m['ratio_na'] = event[col].isna().astype(int).sum()/len(event[col])
    m['count_unique_values'] = event[col].unique().shape[0]
    m['sample_10_values'] = event[col].unique()[:10]
    meta_description.append(m)
pd.DataFrame(meta_description).to_csv('sample1_metadata.csv')
