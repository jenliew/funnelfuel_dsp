# FunnelFuel Enginnering Challenge
## Overview
This is the project to the FunnelFuel challenge. It demostrates an integration service that submit challenge campaign data to a 3rd-party DSP API, it include most of the real-world constraints such as:
* Rate limites
* Scalability
* Dependency chaining( eg: campaign -> Ad Group -> Ads)
* Error handling and retry logic.

## Tech Stack
| Component | Tech |
|---|---|
| Languange | Python 3.10+ |
| Async Framework | asyncio |
| HTTP Client | aiohttp |
| Rate Limiting | aiolimiter |
| Task Queue | asyncio.Queue |
| Theards | Python threading |
| Testing | pytest |

This the high level of the demmand_link component architecture.
![alt text](misc/image.png)

## How it works:
1. The python script support loading CSV file or process one campaign data (in dict format).
    * Loading CSV file
        it will load each lines of the data from the CSV file. The data will be process and convert each campagain as a "Record" object, as it processing each line of campagin data, it will group the *ad_groups* and *ads* to any existing *campaign* before create a new entry.
    * Loading single line (dict format)
        It will convert the dict into the *Record* object.

2. Once the list of *Record* object successfully created, jobs will be divided across N worker threads (default is 3 worker threads but configurable via the argument)
3. Each thread  runs its own asyncio envent loop, ```aiohttp.ClientSession``` and has it owns Queue.
4. Each job is processed by making DSP API endpoint:
    * ``` POST /campaigns ```
    * ``` GET /campaigns/<id>/status ``` and wait for the response from the DSP server. If the status returns:
        - ``` { "status": "success" }``` - this means successfully create campaign data
        - ``` { "status": "failed" } ``` - this means failed to create campaign data. It will raise exception and try again for 3 attempts before process next campaign data.
    * ``` POST /ad-groups ``` and wait for the response.
        - A successful request, it will expecting the reply will contain the *ad-groups-id" key in the dict response.
        - If not, it will assume the post request is failed and raise exeception.
    * ``` GET /ad-groups/<id>/status ``` and wait for the response from the DSP server. If the status returns:
        - ``` { "status": "success" }``` - this means successfully create campaign data
        - ``` { "status": "failed" } ``` - this means failed to create campaign data.
    * ``` POST /ads ``` and wait for the response.
        - A successful resquest, it will expecting the reply will contain the *ads-id" key in the dict response.
        - If not, it will assume the post request is failed and raise exeception.
    * ``` GET /ads/<id>/status ``` and wait for the response from the DSP server. If the status returns:
        - ``` { "status": "success" }``` - this means successfully create campaign data
        - ``` { "status": "failed" } ``` - this means failed to create campaign data.
5. All requests are rate-limited (40 requests/min) using ```AsyncLimiter```.
6. Any failed request or jobs, they will be retired up to ```MAX_RETRIES``.


## Running the project
### Install Dependencies
```bash
# Make sure poetry is available in your environment.
poetry install

```
### Running simple and dummy mock DSP API
```bash
uvicorn scripts.mock_dsp_api:app --reload --port 8000
```

### Run the submission enginer
```bash
python demand_link/demand_link/main.py --file dsp_merged_data.csv
# or
python demand_link/demand_link/main.py --line  '{"campaign_id": "cmp_2025_004", "campaign_name": "Autumn Styles 2024", "campaign_budget": 21000, "start_date": "2024-09-01", "end_date": "2024-10-15", "objective": "video_views", "ad_groups": [ { "id": "ag_1003", "name": "Adults - City", "bid":4.0, "targeting_ages": "35-44;45-64", "targeting_interests": "fashion;streetwear", "targeting_geo": "UK;IE", "ads": [ { "id": "ad_6001","type": "video","creative_url": "https://cdn.example.com/creatives/autumn_style_city.jpg", "click_url": "https://shop.example.com/city-autumn", "status": "new" }]}]}'
```

### Run mocked script to generate dummy data csv file
```bash
# The 'dsp_merged_data.csv' should be save at the directory where you run the generate_test_csv.py
python scripts/generate_test_csv.py
```

### Run Tests
```
pytest -s
```

The script does have the option to change some of the parameters:
```
~/funnel_fuel$ /home/jenny/funnel_fuel/.venv/bin/python /home/jenny/funnel_fuel/demand_link/demand_link/main.py --help
usage: main.py [-h] [--log {DEBUG,INFO,WARNING,ERROR,CRITICAL}] (--line LINE | --file FILE)
               [--rate-limit RATE_LIMIT] [--endpoint ENDPOINT] [--worker WORKER]

Submission Campaign data

options:
  -h, --help            show this help message and exit
  --log {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Enabled logging
  --line LINE           Dict of campaign data to process
  --file FILE           Full path for the CSV file of campaign data
  --rate-limit RATE_LIMIT
                        Rate Limit to restrict the request limit to the DSP Endpoint
  --endpoint ENDPOINT             The URL of the DSP API endpoint.
  --worker WORKER       The number of worker to the queue..
```


## Improve/Extension feature
This script is a protype where start from the basic and minimum requirements. It can be easily extend the functinalities:
* Support DSP API require API credential/token to make the request. This can be easily done by create the AuthBasic object by adding the username and password/construct the header with the API token in ```Notifier```.
```
        self.auth_dsp = None  # This is to authaiohttp.BasicAuth
        ## eg: self.auth_dsp = authaiohttp.BasicAuth(user, password)
        ## or
        self.api_header = None  # This is for API key.
        ## eg: self.api_header = {"Authorization": "Bearer MYREALLYLONGTOKENIGOT"}

```
    depends on the DSP API endpoint support which type of credential, it should able to easily to adding this feature.
* Once the submission job completed, there's a ```complete_task(...)``` in the Submission class. THis can be used for insert the job into the campaign data to a database or update the status to an endpoint.
```
async def complete_task(self, is_success, job_id):
```

* Error code of the demand_link script interaction with the DSP API endpoint. There is more error code need to add into it and handle.

Below is the high level for the systems. The DB service can be easily to setup as soon have more information about final part of the assignment.
*Note: the yellow components are the future works.
![alt text](misc/future_work_image.png)

The logging of the application can be easily to push to the Datadog, there's datadog agent be can handling the pushing the logs to the Datadog when the script is being running. (https://docs.datadoghq.com/logs/log_collection/python/)
