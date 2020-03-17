# Assignment-2

This repository contains codes and responses to JP Morgans's internship assignment 2.

## Contents
I.   [**Hands on with AWS**](https://github.com/surya-de/JPM#i-hands-on-with-aws)- This section caters to provide a brief explanation on the steps performed to solve the 1st question.

II.  [**Solution Design**](https://github.com/surya-de/JPM#ii-solution-design)- This section answers the Design question on Graph database.

III. [**Folder Structure**](https://github.com/surya-de/JPM#iii-folder-structure)- This section is a brief explanation of the project's folder structure.

IV.  [**How to execute**](https://github.com/surya-de/JPM#iii-folder-structure)- This section has a small writeup on how to execute this project.

## I. Hands on with AWS

The questions asks us to answer the following question-
### 1. CloudFormation to configure AWS services
#### Approach
To decide a set of AWS services, I used the following 3 step process-
1. Firstly, I looked into the dashboard provided. This gave me an idea of the kind of aggregations that are made from the dataset.
2. Then, I analyzed the dataset and figured out the transformations that are required to make the aggregations easier. 
3. Finally, listed out a set of AWS services that can perform the required tasks and from that list I shortlisted the services that are **cost effective** and **can be implemented within the given time frame.**
#### A. Services used-
1. AWS S3
2. AWS Athena
3. AWS Sagemaker
4. AWS Lambda
5. AWS SQS
#### Cloudformation Structure
![Architecture Diagram](/images/cloudformation.png)
#### Code Snippet

I am adding a small preview on role and policy creation, the entire cloudformation script can be found [here](https://github.com/surya-de/JPM/blob/master/codes/cloud_formation/scripts/final_clf.json).

```json
{
    "Resources": {
        "LambdaS3ExecutionRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": "*",
                            "Service": ["lambda.amazonaws.com"]
                        },
                        "Action": ["sts:AssumeRole"]
                    }]
                },
                "Path": "/"
            }
        },
        "LambdaS3ExecutionPolicy": {
            "DependsOn": [
                "LambdaS3ExecutionRole"
            ],
            "Type": "AWS::IAM::Policy",
            "Properties": {
                "PolicyName": "NewLambdaS3policy",
                "Roles": [
                    {"Ref": "LambdaS3ExecutionRole"}
                ],
                "PolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": "s3:*",
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "sqs:GetQueueUrl",
                            "sqs:ChangeMessageVisibility",
                            "sqs:ListDeadLetterSourceQueues",
                            "sqs:SendMessageBatch",
                            "sqs:PurgeQueue",
                            "sqs:ReceiveMessage",
                            "sqs:SendMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:CreateQueue",
                            "sqs:DeleteMessage",
                            "sqs:ListQueueTags",
                            "sqs:ChangeMessageVisibilityBatch",
                            "sqs:SetQueueAttributes"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket",
                            "s3:ListBucketVersions",
                            "s3:ListBucketMultipartUploads",
                            "s3:GetObjectTorrent",
                            "s3:GetObjectVersion",
                            "s3:GetObjectAcl",
                            "s3:GetObjectVersionAcl",
                            "s3:PutObjectAcl",
                            "s3:PutObjectVersionAcl",
                            "s3:GetBucketAcl",
                            "s3:PutBucketAcl"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:ListAllMyBuckets",
                            "s3:GetBucketLocation"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "athena:StartQueryExecution",
                            "athena:GetQueryExecution"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "glue:CreateDatabase",
                            "glue:CreateTable",
                            "glue:GetTable",
                            "glue:GetTables",
                            "glue:UpdateTable",
                            "glue:UpdateDatabase",
                            "glue:GetDatabases"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": "logs:CreateLogGroup",
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": "lambda:*",
                        "Resource": "*"
                    }
                ]}
            }
        },
    }
}
```

#### B. Reason behind the chosen services-
##### Design
![Architecture Diagram](/images/architecture.png)
***The numbers in the diagram denotes the order of execution***

step 1- On executing the python sdk, it creates an s3 bucket 'surya-lambda-code-store' and pushes the lambda codes into this bucket.

step 2- The python sdk then excutes the cloudformation script (jpmstack).

step 3- The cloudformation script launces and configures the AWS services.

step 4- Once, the AWS services are configured, the the python sdk pushes the raw data into s3 bucket(surya-landing) and executes the lambda code(surya-transform-files) for data transformation.

step 5- The lambda performs data transformation and pushes the curated data into s3 bucket(surya-curated).

step 6- Once the curated bucket is filled, an SQS event is triggered(surya-send-message).

step 7- The SQS event triggers another lambda which is responsible to call Athena database. This lambda creates a database and table in Athena when called for the first time.

##### AWS S3
i. I have used S3 buckets to store the raw data(Landing bucket) and transformed data (Curated bucket).

ii. The solution I am offering is an automated solution with event triggers. So, I have one two extra S3 buckets. One to store the **lambda codes** and another one to sore the **Athena query results.**

##### AWS Athena
i. I am using Athena to perform ad hoc queries on the the transformed data to check if the data is fetching desired output.

ii. Athena is an AWS managed serverless query service hence I did not need to manage the underlying infrastructure.

iii. AWS Athena charges $5 on per TB of data scanned which can further be reduced by **partitioning the data** and **converting into columnar format(parquet etc).**
##### AWS SageMaker
i. I have used a notebook instance to write my python code to transform the input raw data and merge the two datasets into one unified data model.

ii. This notebook instance has been used in the development phase and I won't be using it during the deployment phase.

iii. The code developed using sage maker will be deployed on AWS lambda.
##### AWS Lambda
i. To create the automated data pipeline I wanted to leverage the serverless event driven Lambda platform.

ii. I have 2 lambda functions in my architecture. One is responsible for performing transformations on the raw data and the other is responsible to call Athena and query the database based on an event.
##### AWS SQS
i. For this assignment I tried to develop a pipeline which can perform the entire data ingestion and transformation task with a single click.

ii.In order to achieve that I needed to use SQS; a scalable queue service offered by AWS. 

iii. Using SQS I have created a trigger which calls a lambda function to query the Athene database when the data transformation task is completed.

### 2. Upload raw data into AWS service
#### Introduction
I have used **Python** to push the raw excel file into S3 bucket.
#### Modules used
1. AWS SDK for Python 3.6
2. Boto3 package

#### Prerequisite
To successfully run the python program, the user must have the following things configured.
1. **AWS CLI**
```bash
Mr-MacBook-Pro:codes Ra$ aws --version
aws-cli/1.16.289 Python/3.7.6 Darwin/18.7.0 botocore/1.13.37
```
2. AWS configure with **AWS Access key** and **AWS secret access id**.
```bash
Mr-MacBook-Pro:codes Ra$ aws configure list
      Name                    Value             Type    Location
      ----                    -----             ----    --------
   profile                <not set>             None    None
access_key     ****************PIE6 shared-credentials-file    
secret_key     ****************DEVo shared-credentials-file    
    region                us-west-2      config-file    ~/.aws/config
```
3. **BOTO3** must be installed
```bash
pip install boto3
```
4. Python 3.6 or higher is required
```bash
Mr-MacBook-Pro:codes Ra$ python3 --version
Python 3.7.6
```
#### Code Snippet
To find the entire code look into the file **push_to_s3.py**
```python
'''
@Module: Module to push raw data to
			s3 bucket.
@Language: Python 3.7
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.0
@Author: Suryadeep(schatt37@asu.edu)
@Version: 1.2
'''

# Import Boto3 client for S3.
s3 = boto3.client('s3')
# This module uploads the raw data
# to s3 bucket.
def file_upload():
	try:
		s3.upload_file('../Data/county-to-county-2013-2017-current-residence-sort.xlsx', 
			'surya-landing','inflow/data.xlsx')
		print('1st file uploaded')
		
		s3.upload_file('../Data/county-to-county-2013-2017-previous-residence-sort.xlsx', 
			'surya-landing','outflow/data.xlsx')
		print('2nd file uploaded')
		
		return True
	
	except FileNotFoundError:
		print("Please check the location of your file")
		return False
	
	except NoCredentialsError:
		print("AWS credentials are not available")
		return False
```
### 3. Load Pre processed data
I have loaded the pre processed data into **Athena** using a serverless lambda function to automate the flow.

![table](/images/athenatable.png)
### 4. Aggregation query 

**Query**-
This query returns the top 5 counties with maximum inflow in population.

```sql
SELECT cunty_curr_res AS county_name, SUM(mvrs_in_cunty_to_cunty_flw_est) AS inflow_value
FROM "curated_db"."surya_final_table"
GROUP BY identifier, cunty_curr_res
HAVING identifier LIKE 'inflow'
ORDER BY SUM(mvrs_in_cunty_to_cunty_flw_est) desc
LIMIT 5
```

**Output**

| county_name        | inflow_value |
|--------------------|--------------|
| Orange County      | 289306       |
| Los Angeles County | 287588       |
| Montgomery County  | 256565       |
| Harris County      | 228421       |
| Maricopa County    | 208279       |

![Query](/images/query.png)

### 5. Cost of services
The cost calculation below makes an assumption that the entire pipeline will be executed 1 time per month. So, the metrics associated with the calculations are assumed based on 1 execution.

S3 buckets
 	1.1. surya-athena-output
	   Assumptions-
S3 storage
2 GB/month
Upload requests
6 requests

	   Calculations-
		2 GB per month x 0.004 USD = 0.01 USD (Glacier storage cost)
6 requests x 0.00005 USD = 0.00 USD (UPLOAD Requests cost)
0.01 USD + 0.00 USD = 0.01 USD (Total Data Retrieval cost )
S3 Glacier cost_1.1 (monthly): 0.01 USD

	1.2. surya-landing
	   Assumptions-
S3 storage
2 GB/month
Upload requests
2 requests

	   Calculations-
		2 GB per month x 0.004 USD = 0.01 USD (Glacier storage cost)
2 requests x 0.00005 USD = 0.00 USD (UPLOAD Requests cost)
0.01 USD + 0.00 USD = 0.01 USD (Total Data Retrieval cost )
S3 Glacier cost_1.2 (monthly): 0.01 USD

	1.3. surya-curated
	   Assumptions-
S3 storage
2 GB/month
Upload requests
104 requests

	   Calculations-
		2 GB per month x 0.004 USD = 0.01 USD (Glacier storage cost)
104 requests x 0.00005 USD = 0.01 USD (UPLOAD Requests cost)
0.01 USD + 0.01 USD = 0.01 USD (Total Data Retrieval cost )
S3 Glacier cost_1.3 (monthly): 0.02 USD





	1.4. Surya-store-lambda-code

	   Assumptions-
S3 storage
1 GB/month
Upload requests
2 requests

	   Calculations-
		1 GB per month x 0.004 USD = 0.00 USD (Glacier storage cost)
2 requests x 0.00005 USD = 0.00 USD (UPLOAD Requests cost)
0.00 USD + 0.00 USD = 0.00 USD (Total Data Retrieval cost )
S3 Glacier cost_1.4 (monthly): 0.00 USD

TOTAL S3 COST-
	S3 Glacier cost_1.1 + S3 Glacier cost_1.2 + S3 Glacier cost_1.3 + S3 Glacier cost_1.4
	= 0.01 + 0.01 + 0.02 + 0.00
	= 0.04 USD

Lambda function
	2.1. transformation-lambda
   Assumptions-
Number of requests
1
Duration of each request(ms)
600000
Amount of memory allocated
1024(MB)

	   Calculations-
		Unit conversions
Amount of memory allocated: 1024 MB x 0.0009765625 GB in a MB = 1 GB
Pricing calculations
RoundUp (600000) = 600000 Duration rounded to nearest 100ms
1 requests x 600,000 ms x 0.001 ms to sec conversion factor = 600.00 total compute (seconds)
1 GB x 600.00 seconds = 600.00 total compute (GB-s)
600.00 GB-s x 0.0000166667 USD = 0.01 USD (monthly compute charges)
1 requests x 0.0000002 USD = 0.00 USD (monthly request charges)
0.01 USD + 0.00 USD = 0.01 USD
Lambda cost_2.1(monthly): 0.01 USD

	2.2. call-athena
	   Assumptions-
Number of requests
1
Duration of each request(ms)
180000
Amount of memory allocated
600(MB)

	   Calculations-
		Unit conversions
Amount of memory allocated: 600 MB x 0.0009765625 GB in a MB = 0.5859375 GB

Pricing calculations
RoundUp (180000) = 180000 Duration rounded to nearest 100ms
1 requests x 180,000 ms x 0.001 ms to sec conversion factor = 180.00 total compute (seconds)
0.5859375 GB x 180.00 seconds = 105.47 total compute (GB-s)
105.47 GB-s x 0.0000166667 USD = 0.00 USD (monthly compute charges)
1 requests x 0.0000002 USD = 0.00 USD (monthly request charges)
0.00 USD + 0.00 USD = 0.00 USD
Lambda cost_2.2(monthly): 0.00 USD
     
       total_lambda_cost = Lambda cost_2.1 + Lambda cost_2.2
			= 0.01 + 0.00
			= 0.01

Athena
	   Assumptions-
Number of Query 
10 per day
Data scanned per query
700 MB

   Calculations-
Total number of queries: 10 per day * (730 hours in a month / 24 hours in a day)
= 304.1666666666667 queries per month

Data amount scanned per query: 700 MB x 9.5367432e-7 TB in a MB              	= 0.000667572024 TB

Pricing calculations
Rounding (304.1666666666667) = 304 Rounded total number of queries
304 queries per month x 0.000667572024 TB x 5.00 USD = 1.01 USD
Athena cost (monthly): 1.01 USD

Total cost (monthly) - Total_S3_cost + total_lambda_cost + Athena cost
			= 0.04 + 0.01 + 1.01
			= 1.06 USD per month

## II. Solution Design
### 1. AWS Neptune vs Neo4j
| Criteria                 | AWS Neptune                                                                                                                                              | Neo4j                                                                                            | Decision     |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|--------------|
|        Open Source       | It is a closed source managed by amazon.  Hence it limits users to stick to Amazon platform.                                                             | It is an open source and can be deployed on-prem if not willing  to share the data online.       | Neo4j        |
|  Community Support       | While Amazon has fair decumentation but Neptune  being a relatively new platform the community  support is not yet that wide spread.                     | On the other hand Neo4j has a very good community support  along with the awesome documentation. | Neo4j        |
| Advanced Data  Analytics | Does not support advanced analytics with solutions  such as spark.                                                                                       | Allows specific delegation for ad hoc reporting and  analytics instances.                        | Neo4j        |
| Managed Service          | AWS Neptune is a managed service by AWS. Hence, if user is using the AWS stack it can be a better option as it will be easier to configure the pipeline. | If we are using AWS tool stack it will require an extra set of configuration.                    |  AWS Neptune |

Keeping in mind the above points I decided that at this point **Neo4j** will be a better option as it will allow to user to keep the data on premise and it's good community support will help in seamless development.
### 2. How to load data?
These are the following staeps that I would perform to load data in Neo4j-
1. I would decide the nodes and properties that can be extracted from the data.

2. In this case I was provided with excel files with multiple tabs. So, I would convert the various tabs into individual csv files using python script.

```python
# Read excel file.
read_file = pd.ExcelFile (data_location)
        # Get the sheets in excel file.
	file_sheets = read_file.sheet_names
        for sheets in file_sheets:
            print('##Reading sheet- ', sheets)
	    # Read each sheet with column names and
	    # skip rows if necessary.
            df = read_file.parse(sheets, header = None, names = col_names, skiprows = 4)
            # Drop garbage values that are present
	    # at the end of each sheet.
	    df.drop(df.tail(6).index,inplace = True)
            content_buffer = StringIO()
	    # Convert to csv.
            df.to_csv(content_buffer)
```

3. I will use Neo4j's **LOAD CSV** module to load the csv files into the graph database.

```cypher
LOAD CSV FROM '/path/to/file' AS line
```
4. While loading I will mention the **nodes**, **labels** and **properties** in the data.

```cypher
CREATE (label_name:node_name { property_name: property_value imported as line in last step})
```

### 3. Design of graph relation

**Diagram**
![graph](/images/graphdatabase.png)

To decide the graph relation I have taken the following steps-

**1. Decide Nodes**

Nodes are the data points that is the subject of calculations. 

**Example**

In this dataset we will be interested in knowing the inflow and outflow of the **Counties**.

**Outcome**

I have considered each **county** as a Node and labeled it as County.

**Label-**

:County

**2. Add properties to the node**

Each node will have the following properties which explains the county-

a. **county_name** - Name of the county under consideration.

b. **state_name** - Name of the state of the county under consideration.

c. **state_code** - State code of.

d. **county_code** - Unique county code.

e. **current_population** - Present population of the county.

**3. Add relationships**

To find the relationships I figured the questions that we want to handle. I studied the dashboard and figured out the questions that are pertinent.

Q1. How many pople **moved to** <county name>?
	
Q2. How many people **moved out of** <county name> ?

Each node will have 2 relationships -

a. **MOVED_TO** - Each node will have a move to relationship entering the node which signifies the number of people entering the county from a different county.

Example-
```
    MOVED_TO
    count:50
B -------------> A   
50 people entered county A from county B.
```

b. **MOVED_OUT_OF** - Each node will have a moved out of relationship leaving the node which signifies the number of people who left the current node.

Example-
```
    MOVED_OUT_OF    
    count:60    
B <---------------- A
60 people left county A for a different county.
```

## III. Folder Structure
### How the folders look
![folders](/images/folderstructure.png)
### Explanation
1. **Architecture**- Contains the LLD and HLD diagrams of the implementation.

2. **codes**- This folder contains the codes in a packaged format which will be deployed during execution. 

   2.1. **cloud_formation**- Contains the init files and some subfolders which holds the cloud formation scripts.
   
   2.2. **check_create_cf.py**- This is the python file which will execute the cloud formation scripts to spin the resources.
   
   2.3. **scripts**- This folder contains the cloud formation JSON script.
   
      2.3.1. **final_clf.json**- The cloud formation json file.
      
      2.3.2. **paramerter.json**- This is the parameter json file required to call cloud formation using BOTO3.
      
3. **lambda_functions**- This folder contains 2 zip folders which are the deployemnt packages of the 2 lambda functions used.

   3.1. **athena_lambda_function.zip**- The lambda code that calls the athena module on an SQS event.
   
   3.2. **lambda.zip**- This contains the lambda code that reads the raw excel files and performs basic transformations.
   
4. **Data**- This is the data folder which contains the two excel raw files.

5. **Resources**- In this folder I am adding all the code files so that the codes can be viewed using any editor.

## IV. How to execute
### Pre requisite
To successfully run the code the user must have-
1. **aws cli** installed and configured with the access keys.
2. Python3.6 installed
3. BOTO3 installed.
### Steps
Once the environment is set up, perform the following steps-
1. clone the git repository
```bash
git clone https://github.com/surya-de/JPM.git
```
2. Navigate to the **codes** folder
```bash
cd JPM/codes
```
3. Execute the **push_to_s3.py** file along with the path to cloud formation script avalibale in the cloud_formation folder.
```bash
python3 push_to_s3.py jpmstack cloud_formation/scripts/final_clf.json cloud_formation/scripts/paramerter.json
```

Once these steps are performed, the following actions will happen-

a. AWS services will be created.

b. Lambda codes will be pushed to the buckets and will be executed atomated.

c. Raw files will be pushed to the landing bucket.

d. SQS trigger will be initiated.

e. Athena database and tables will be created.


The logs can be checked in **cloudwatch**. Once completed the user can check the data in athena database.
