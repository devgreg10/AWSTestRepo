import json
from datetime import datetime
import pytz

def lambda_handler(event, context):
    # Define the time zone
    eastern = pytz.timezone('America/New_York')
    
    # Get the current time
    now = datetime.now(eastern)
    
    # Format the timestamp
    formatted_timestamp = now.strftime('%Y-%m-%dT%H:%M:%S%z')
    
    # Adjust the format to include the colon in the offset
    formatted_timestamp = f"{formatted_timestamp[:-2]}:{formatted_timestamp[-2:]}"
    
    return {
        'statusCode': 200,
        'timestamp': formatted_timestamp
    }
