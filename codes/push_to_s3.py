import boto3
import sys
import pandas as pd
from cloud_formation.check_create_cf import caller
from botocore.exceptions import NoCredentialsError

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

def invoke_lambda():
	client = boto3.client('lambda')
	paload = b"""{ "status" : "true"}"""
	response = client.invoke( FunctionName = 'surya-transform-files', InvocationType = 'Event',  Payload = paload)
	print(response)

if __name__ == '__main__':
	print(sys.argv[1])
	s_name = sys.argv[1]
	cf_loc = sys.argv[2]
	param_loc = sys.argv[3]
	if caller(s_name, cf_loc, param_loc) == True:
		if file_upload() == True:
			print('Calling lambda')
			invoke_lambda()

