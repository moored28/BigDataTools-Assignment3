import requests
import json
import matplotlib.pyplot as plt
import redis

class DataProcessor:
    """
    A class to handle processing of JSON data from an API, insertion into Redis, and some basic data processing tasks.

    Attributes:
        api_url (str): The URL of the API to fetch JSON data from.
        redis_client (redis.Redis): The Redis client object.
    """

    def __init__(self, api_url, redis_client):
        """
        Initialize the DataProcessor with API URL and Redis client object.

        Args:
            api_url (str): The URL of the API to fetch JSON data from.
            redis_client (redis.Redis): The Redis client object.
        """
        self.api_url = api_url
        self.redis_client = redis_client

    def fetch_json_from_api(self):
        """
        Fetch JSON data from the specified API URL.

        Returns:
            dict: The JSON data fetched from the API or Redis.
        """
        try:
            data = self.redis_client.get("exchange_rate_data")
            if data:
                return json.loads(data)
            else:
                response = requests.get(self.api_url)
                response.raise_for_status()  # Raise an exception for HTTP errors
                data = response.json()
                self.redis_client.json().set("exchange_rate_data", json.dumps(data))
                return data
        except (requests.RequestException, json.JSONDecodeError, redis.RedisError) as e:
            print("Error fetching data:", e)
            return None

    def insert_into_redis(self, data):
        """
        Insert JSON data into Redis using RedisJSON.

        Args:
            data (dict): The JSON data to insert into Redis.
        """
        try:
            if 'conversion_rates' in data:
                conversion_rates = data['conversion_rates']
                # Use RedisJSON to set the JSON data
                self.redis_client.json().set("exchange_rates", '.', json.dumps(conversion_rates))
        except Exception as e:
            print("Error inserting data into Redis using RedisJSON:", e)
    
    def plot_exchange_rates(self):
        """
        Plot exchange rates using matplotlib.
        """
        data = self.fetch_json_from_api()
        if data is not None and 'conversion_rates' in data:
            conversion_rates = data['conversion_rates']
            sorted_rates = sorted(conversion_rates.items(), key=lambda x: x[1])  # Sort rates by value
            currencies = [currency for currency, _ in sorted_rates]
            values = [value for _, value in sorted_rates]
            plt.figure(figsize=(14, 8))
            plt.bar(currencies, values)
            plt.xlabel('Currency')
            plt.ylabel('Exchange Rate')
            plt.title('Exchange Rates')
            plt.xticks(rotation=45, ha='right')  # Rotate x-labels for better readability
            plt.tight_layout()  # Adjust layout for better spacing
            plt.show()


    def aggregate_exchange_rates(self):
        """
        Aggregate exchange rates to calculate statistics.
        """
        data = self.fetch_json_from_api()
        if data is not None and 'conversion_rates' in data:
            conversion_rates = data['conversion_rates']
            # Calculate statistics
            average_rate = sum(conversion_rates.values()) / len(conversion_rates)
            min_rate = min(conversion_rates.values())
            max_rate = max(conversion_rates.values())
            median_rate = sorted(conversion_rates.values())[len(conversion_rates) // 2]
            print("Average Exchange Rate:", average_rate)
            print("Minimum Exchange Rate:", min_rate)
            print("Maximum Exchange Rate:", max_rate)
            print("Median Exchange Rate:", median_rate)

    def search_exchange_rates(self, min_value, max_value):
        """
        Search for currencies within a given value range.
        """
        data = self.fetch_json_from_api()
        if data is not None and 'conversion_rates' in data:
            conversion_rates = data['conversion_rates']
            filtered_currencies = [currency for currency, rate in conversion_rates.items() if min_value <= rate <= max_value]
            print("Currencies within the given value range:", filtered_currencies)



if __name__ == "__main__":
    # Establish Redis connection
    r = redis.Redis(
        host='redis-13937.c322.us-east-1-2.ec2.cloud.redislabs.com',
        port=13937,
        password='3OZRwR6p43WtDXfxlNsanGSwOspzWBk4')

    # Example usage
    processor = DataProcessor(api_url="https://v6.exchangerate-api.com/v6/8e2176698c889d952944aca4/latest/USD",
                              redis_client=r)
    processor.plot_exchange_rates()
    processor.aggregate_exchange_rates()
    processor.search_exchange_rates(min_value=0.5, max_value=2.0)