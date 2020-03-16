# Time taken for the entire function- 1 min 43 seconds
import pandas as pd
import boto3
from io import StringIO

def callSqs():
    # Create SQS client
    sqs = boto3.client('sqs')
    SQS_QUEUE_NAME = 'surya-send-message'
    queue_resp = sqs.get_queue_url(QueueName = SQS_QUEUE_NAME)
    queue_url = queue_resp['QueueUrl']
    response = sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = 10,
        MessageAttributes = {
            'status': {
                'DataType': 'String',
                'StringValue': 'Done'
            }
        },
        MessageBody = (
            'files uploaded'
        )
    )
    return True

def lambda_handler(event, context):
    file_type = ['inflow', 'outflow']
    s3_resource = boto3.resource('s3')
    src_bucket='surya-landing'
    push_bucket = 'surya-curated'
    col_names = ['curr_res_ste_cde', 'curr_res_fifs_cunty_cde', 'res_1_ago_ste_isl_frgn_rgn_cde', 
                 'res_1_ago_fips_cunty_cde', 'state_curr_res', 'cunty_curr_res', 'cunty_curr_res_pop_1_ovr_est', 
                 'cunty_curr_res_pop_1_ovr_moe', 'cunty_curr_res_nonmvrs_est', 'cunty_curr_res_nonmvrs_moe', 
                 'cunty_curr_res_mvrs_in_us_est', 'cunty_curr_res_mvrs_in_us_moe', 
                 'cunty_curr_res_mvrs_in_sme_cunty_est', 'cunty_curr_res_mvrs_in_sme_cunty_moe', 
                 'cunty_curr_res_mvrs_frm_diff_cunty_sme_ste_est', 'cunty_curr_res_mvrs_frm_diff_cunty_sme_ste_moe', 
                 'cunty_curr_res_mvrs_frm_diff_ste_est', 'cunty_curr_res_mvrs_frm_diff_ste_moe', 
                 'cunty_curr_res_mvrs_frm_abrd_est', 'cunty_curr_res_mvrs_frm_abrd_moe', 
                 'ste_isl_frgn_rgn_res_1_ago', 'cunty_res_1_ago', 'cunty_res_1_ago_pop_1_ovr_est', 
                 'cunty_res_1_ago_pop_1_ovr_moe', 'cunty_res_1_ago_nonmvrs_est', 'cunty_res_1_ago_nonmvrs_moe', 
                 'cunty_res_1_ago_mvrs_in_us_pr_est', 'cunty_res_1_ago_mvrs_in_us_pr_moe', 
                 'cunty_res_1_ago_mvrs_in_sme_cunty_est', 'cunty_res_1_ago_mvrs_in_sme_cunty_moe', 
                 'cunty_res_1_ago_mvrs_diff_cunty_sme_ste_est', 'cunty_res_1_ago_mvrs_diff_cunty_sme_ste_moe', 
                 'cunty_res_1_ago_mvrs_to_diff_ste_est', 'cunty_res_1_ago_mvrs_to_diff_ste_moe', 
                 'cunty_res_1_ago_mvrs_to_pr_est', 'cunty_res_1_ago_mvrs_to_pr_moe', 
                 'mvrs_in_cunty_to_cunty_flw_est', 'mvrs_in_cunty_to_cunty_flw_moe']
    for types in file_type:
        data_key = types + '/data.xlsx'
        data_location = 's3://{}/{}'.format(src_bucket, data_key)
        read_file = pd.ExcelFile (data_location)
        file_sheets = read_file.sheet_names
        for sheets in file_sheets:
            print('##Reading sheet- ', sheets)
            df = read_file.parse(sheets, header = None, names = col_names, skiprows = 4)
            df['identifier'] = types
            df.drop(df.tail(6).index,inplace = True)
            content_buffer = StringIO()
            df.to_csv(content_buffer)
            push_key = types + '_' + sheets + '.csv'
            push_location = 's3://{}/{}'.format(push_bucket, push_key)
            s3_resource.Object(push_bucket, push_key).put(Body = content_buffer.getvalue())
        print('###file transformed-', types)

    # Call the sqs trigger
    if callSqs() == True:
        print('###SQS event triggered')
