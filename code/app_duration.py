import requests
import json
from datetime import datetime

def AppDuration(app_id):
    server_url = "http://snf-39553.ok-kno.grnetcloud.net:18080"

    job_details_endpoint = f"{server_url}/api/v1/applications/{app_id}/jobs"

    # Make a GET request to the job details endpoint
    response = requests.get(job_details_endpoint)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        jobs_data = json.loads(response.content.decode('utf-8'))

        sum = 0
        for job in jobs_data:
    # Convert submissionTime and completionTime to datetime objects
            
            start_time = datetime.strptime(job["submissionTime"], "%Y-%m-%dT%H:%M:%S.%fGMT")
            end_time = datetime.strptime(job["completionTime"], "%Y-%m-%dT%H:%M:%S.%fGMT")

            # Calculate the duration in seconds
            duration_seconds = (end_time - start_time).total_seconds()
            sum += duration_seconds
        return sum 
    else:
        print(f"Failed to retrieve job details. Status code: {response.status_code}")
