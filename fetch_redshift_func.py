import boto3
import json
import base64
import pandas as pd
import os
from io import StringIO

# Initialize AWS Lambda client
aws_region = 'eu-central-1'
lambda_client = boto3.client(
    'lambda',
    region_name=aws_region,
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

lambda_function_name = 'fetch_redshift_tables'


def run_redshift_query(sql_query: str, file_name: str) -> pd.DataFrame:
    """
    Executes a SQL query on Redshift via Lambda and returns the result as a DataFrame.
    """

    # Prepare payload (Lambda expects a dict of queries)
    payload = {
        'sql_queries_map': {"query": sql_query}
    }

    # Invoke Lambda
    response = lambda_client.invoke(
        FunctionName=lambda_function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )

    response_payload = response['Payload'].read().decode("utf-8")

    try:
        response_json = json.loads(response_payload)
        if 'body' not in response_json:
            raise ValueError(f"'body' key not found. Full response: {response_payload}")

        results_map = json.loads(response_json['body'])

        # Our payload has only one query: "query"
        encoded_csv_data = results_map.get("query")
        if not encoded_csv_data:
            raise ValueError("No data returned from Lambda.")

        decoded_csv_data = base64.b64decode(encoded_csv_data.encode('utf-8')).decode('utf-8')

        # Convert CSV string to DataFrame
        df = pd.read_csv(StringIO(decoded_csv_data))

        # Create folder
        os.makedirs("Pull", exist_ok=True)

        file_path = os.path.join("Pull", f"{file_name}.csv")

        # Save DataFrame to CSV
        df.to_csv(file_path, index=False, encoding='utf-8')

        print(f"\nSQL pull saved to '{file_path}'")

        return df

    except Exception as e:
        raise RuntimeError(f"Failed to parse Lambda response. Error: {e}\nResponse: {len(response_payload,100)}")


# Example usage:
if __name__ == "__main__":
    sql = "SELECT * FROM my_table LIMIT 10;"
    df = run_redshift_query(sql)
    print(df.head())
