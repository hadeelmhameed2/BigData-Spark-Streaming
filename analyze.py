import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_csv_to_kafka(file_path, topic_name):
    print(f"Reading data from {file_path}...")
    
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            count = 0
            for row in reader:
                
                if not row['County']: row['County'] = 'King' 
                if not row['City']: row['City'] = 'Seattle'
                
                if not row['Postal Code']: row['Postal Code'] = '98101'
                if not row['Legislative District']: row['Legislative District'] = '29'
                
                if 'Electric Utility' in row:
                    del row['Electric Utility']

                if not row['Vehicle Location']:
                    row['Vehicle Location'] = 'Unknown'

                producer.send(topic_name, value=row)
                
                count += 1
                if count % 10000 == 0:
                    print(f"Produced {count} rows...")

        producer.flush()
        print(f"Successfully finished! Total rows sent: {count}")

    except FileNotFoundError:
        print(f"Error: Could not find file at {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    FILE_PATH = '/root/pyspark/electriccars/input/Electric_Vehicle_Population_Data.csv'
    TOPIC = 'electric_vehicles'
    
    stream_csv_to_kafka(FILE_PATH, TOPIC)