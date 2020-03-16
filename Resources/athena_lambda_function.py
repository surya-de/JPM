import json
import boto3
from time import sleep

def lambda_handler(event, context):
    client = boto3.client('athena')
    def check_status(exc_id):
        exec_status = True
        while exec_status == True:
            rp = client.get_query_execution(QueryExecutionId = exc_id)
            if rp["QueryExecution"]["Status"]["State"] == 'SUCCEEDED':
                exec_status = False
            elif rp["QueryExecution"]["Status"]["State"] in ["QUEUED", "RUNNING"]:
                sleep(5)
            else:
                return 'FAILED'
        return True
    def create_db_table():
        table_name = 'surya_final_table'
        db_name = 'curated_db'
        table_query = "CREATE EXTERNAL TABLE IF NOT EXISTS surya_final_table \
                        (id INT,\
                        curr_res_ste_cde STRING,\
                        curr_res_fifs_cunty_cde STRING,\
                        res_1_ago_ste_isl_frgn_rgn_cde STRING,\
                        res_1_ago_fips_cunty_cde STRING,\
                        state_curr_res STRING,\
                        cunty_curr_res STRING,\
                        cunty_curr_res_pop_1_ovr_est STRING,\
                        cunty_curr_res_pop_1_ovr_moe STRING,\
                        cunty_curr_res_nonmvrs_est STRING,\
                        cunty_curr_res_nonmvrs_moe STRING,\
                        cunty_curr_res_mvrs_in_us_est STRING,\
                        cunty_curr_res_mvrs_in_us_moe STRING,\
                        cunty_curr_res_mvrs_in_sme_cunty_est STRING,\
                        cunty_curr_res_mvrs_in_sme_cunty_moe STRING,\
                        cunty_curr_res_mvrs_frm_diff_cunty_sme_ste_est STRING,\
                        cunty_curr_res_mvrs_frm_diff_cunty_sme_ste_moe STRING,\
                        cunty_curr_res_mvrs_frm_diff_ste_est STRING,\
                        cunty_curr_res_mvrs_frm_diff_ste_moe STRING,\
                        cunty_curr_res_mvrs_frm_abrd_est STRING,\
                        cunty_curr_res_mvrs_frm_abrd_moe STRING,\
                        ste_isl_frgn_rgn_res_1_ago STRING,\
                        cunty_res_1_ago STRING,\
                        cunty_res_1_ago_pop_1_ovr_est STRING,\
                        cunty_res_1_ago_pop_1_ovr_moe STRING,\
                        cunty_res_1_ago_nonmvrs_est STRING,\
                        cunty_res_1_ago_nonmvrs_moe STRING,\
                        cunty_res_1_ago_mvrs_in_us_pr_est STRING,\
                        cunty_res_1_ago_mvrs_in_us_pr_moe STRING,\
                        cunty_res_1_ago_mvrs_in_sme_cunty_est STRING,\
                        cunty_res_1_ago_mvrs_in_sme_cunty_moe STRING,\
                        cunty_res_1_ago_mvrs_diff_cunty_sme_ste_est STRING,\
                        cunty_res_1_ago_mvrs_diff_cunty_sme_ste_moe STRING,\
                        cunty_res_1_ago_mvrs_to_diff_ste_est STRING,\
                        cunty_res_1_ago_mvrs_to_diff_ste_moe STRING,\
                        cunty_res_1_ago_mvrs_to_pr_est STRING,\
                        cunty_res_1_ago_mvrs_to_pr_moe STRING,\
                        mvrs_in_cunty_to_cunty_flw_est INT,\
                        mvrs_in_cunty_to_cunty_flw_moe STRING,\
                        identifier STRING)\
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','STORED \
                        AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' \
                        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \
                        LOCATION 's3://surya-curated/' TBLPROPERTIES ('compressionType'='none', \
                        'delimiter'=',', 'objectCount'='1', 'skip.header.line.count'='1', \
                        'typeOfData'='file')"
        
        database_query = "create database IF NOT EXISTS curated_db"
        athena_result_bucket = "s3://athena-query-opt/"
        response_db = client.start_query_execution(
            QueryString = database_query,
            ResultConfiguration={
                'OutputLocation': athena_result_bucket,
            }
        )
        if check_status(response_db['QueryExecutionId']) == True:
            response_tab = client.start_query_execution(
                QueryString = table_query,
                QueryExecutionContext={
                    'Database': db_name
                },
                ResultConfiguration={
                    'OutputLocation': athena_result_bucket,
                }
            )
    
    create_db_table()