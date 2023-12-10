'''
    Running DAG from here 
    PythonOperator fetches data
'''

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    
    'owner': 'Himanshu',
    'start_date': datetime(2023, 12, 9, 9, 00)
}


def get_data():
    
    import requests
    
    res = requests.get('https://randomuser.me/api/')
    '''with open('test.json', 'w') as test:
        
        writer = json.dumps(res.json()['results'][0], indent=3)
        test.write(writer)'''
    res = res.json()['results'][0]
    return res


def format_data(res):
    
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    location = res['location']
    data['address'] = f'{str(location["street"]["number"])}' + ' '\
                    + location["street"]["name"] + ', '\
                    + location['city'] + ', ' + location['state'] + ', '\
                        + location['country']
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    
    
    '''with open('test.json', 'w') as file:
        writer = json.dumps(data, indent = 3)
        file.write(writer)'''
        
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        
        if time.time() > curr_time + 60:
            break
        
        try:
            res = get_data()
            data = format_data(res)
            producer.send('user_created', json.dumps(data).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue
        
        

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup= False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )
    
stream_data()