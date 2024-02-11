from kafka import KafkaConsumer
import pandas as pd
import json,time

consumer = KafkaConsumer('belajar-topik-lagi', bootstrap_servers=['192.168.0.6:9092'],
                         auto_offset_reset='earliest',
                         api_version=(0, 10))

dfs = []  # Initialize an empty list to store DataFrames

for message in consumer:
    message_value = json.loads(message.value.decode('utf-8'))
    df = pd.DataFrame([message_value])  # Create a DataFrame from the value
    dfs.append(df)  # Append to the list of DataFrames

    df = pd.concat(dfs, ignore_index=True)  # Concatenate all DataFrames

    hasil1=len(df.index)
    hasil2=len(df[df['nm_brng'] =='tepung'].index)
    print("Hasil_1 : ",hasil1, 
          "Hasil_2 : ",hasil2,
          "Persentase Hasil 2 : ",(hasil2/hasil1)*100,
          end="\r")  # Display the resulting DataFrame
    
    time.sleep(0.2)
