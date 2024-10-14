from quixstreams import Application  # import the Quix Streams modules for interacting with Kafka:
# (see https://quix.io/docs/quix-streams/v2-0-latest/api-reference/quixstreams.html for more details)

# import additional modules as needed
import random
import os
import json
import glob
import tqdm
import pandas as pd

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="data_source", auto_create_topics=True)  # create an Application

# define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(topic_name)



def main():
    """
    Read data from the hardcoded dataset and publish it to Kafka
    """

    # create a pre-configured Producer object.
    with app.get_producer() as producer:
        # iterate over the data from the hardcoded dataset
        files = glob.glob('dataset/*.zst')
        files.sort()
        
        for file_path in tqdm.tqdm(files):
            df = pd.read_csv(file_path)
            for _, row in df.iterrows():
                
                trade_data = row.to_dict()
                json_data = json.dumps(trade_data)  # convert the row to JSON

                # publish the data to the topic
                producer.produce(
                    topic=topic.name,
                    key=trade_data['symbol'],
                    value=json_data,
                )

        print("All rows published")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting.")