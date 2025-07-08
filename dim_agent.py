from teradatasql import connect
import psycopg2
import csv
import numpy as np
from datetime import datetime,date
import logging
import os
import sys
import traceback
import subprocess
from common.common_utils import load_yaml_file, start_logging, get_prm_dtls_ssm_pms

def get_pc_start_exec_ts(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,
                                    rshft_db_port):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname,user=rshft_db_user,password=rshft_db_password,
                                    host=rshft_db_host,port=rshft_db_port) as conn:
            
            logging.info("Redshift Database Connection successful!")
            
            with conn.cursor() as cur:
                
                               
                select_control_query = """select start_exec_ts as date from 
                                  edw_ods.process_control WHERE process_nm='DIM_AGENT';"""
                                                               
                cur.execute(select_control_query)
                
                pc_start_exec_ts = cur.fetchone()

                # If a pc_start_exec_ts is found, return the date
                if pc_start_exec_ts and pc_start_exec_ts[0]:
                    logging.info(f"Timestamp fetched successfully for process_nm='DIM_AGENT'")
                    return pc_start_exec_ts[0]
                else:
                    # If no pc_start_exec_ts is found, log an error and exit
                    logging.error("No timestamp found for process_nm='DIM_AGENT'")
                    sys.exit(1)
        
    except Exception as e:
        logging.error(f"An unexpected error occurred in function {get_pc_start_exec_ts.__name__}: {str(e)}")
        logging.error(traceback.format_exc())  
        sys.exit(1)
        

def td_to_file(td_db_host,td_db_user,td_db_password,pc_start_exec_date):
    try:
        td_conn=connect(host=td_db_host, user=td_db_user, password=td_db_password)
        logging.info("Teradata Database Connection successful!")
        
        src_sql_query=f"""
                    SELECT a.Agn_Rec_Key as agent_key_id,
                    a.CSR_ID as csr_id,
                    a.Agn_Role_Strt_Dt as REC_EFF_TS,
                    a.Agn_Role_End_Dt as REC_EXP_TS,
                    a.Agn_id as agent_id,
                    case when a.Agn_Fst_NM is NULL then 'Not Defined' else a.Agn_Fst_NM end as AGENT_FIRST_NM,
                    case when a.Agn_Last_NM is NULL then 'Not Defined' else a.Agn_Last_NM end as AGENT_LAST_NM,
                    case when a.Agn_Vend is NULL then 'Not Defined' else a.Agn_Vend end as VENDOR_NM,
                    case when a.Agn_Site is NULL then 'Not Defined' else a.Agn_Site end as VENDOR_SITE_NM,
                    case when a.Agn_Loc is NULL then 'Not Defined' else a.Agn_Loc end as VENDOR_LCTN_NM,
                    case when a.Agn_LOB is NULL then 'Not Defined' else a.Agn_LOB end as AGENT_LOB_CD,
                    case when a.Agn_Role_SMS is NULL then 'Not Defined' else a.Agn_Role_SMS end as AGENT_SMS_ROLE_CD,
                    case when a.Agn_Role_Mktg is NULL then 'Not Defined' else a.Agn_Role_Mktg end as AGENT_MRKTNG_ROLE_CD,
                    case when a.Avtn_Src is NULL then 'Not Defined' else a.Avtn_Src end as ACTVTN_SRC_CD,
                    case when a.Agn_Intrctn_Chnl_Nm is NULL then 'Not Defined' else a.Agn_Intrctn_Chnl_Nm end as INTRCTN_CHNL_NM,
                    a.IS_WORK_AT_HOME_IND as IS_WORK_AT_HOME_IND,
                    a.Agn_Sts as IS_AGENT_ACTV_IND,
                    a.Agn_Supr_CSR_ID as SUPRVSR_CSR_ID,
                    case when b.Agn_Fst_NM is NULL then 'Not Defined' else b.Agn_Fst_NM end as SUPRVSR_FIRST_NM,
                    case when b.Agn_Last_NM is NULL then 'Not Defined' else b.Agn_Last_NM end as SUPRVSR_LAST_NM,
                    case when b.Agn_LOB is NULL then 'Not Defined' else b.Agn_LOB end as SUPRVSR_LOB_CD,
                    case when b.Agn_Role_SMS is NULL then 'Not Defined' else b.Agn_Role_SMS end as SUPRVSR_SMS_ROLE_CD,
                    case when b.Agn_Role_Mktg is NULL then 'Not Defined' else b.Agn_Role_Mktg end as SUPRVSR_MRKTNG_ROLE_CD,
                    a.Cur_Rec_Fl as IS_LATEST_IND,
                    case when a.Lob_tnre_band_cd is NULL then 'Not Defined' else a.Lob_tnre_band_cd end as Lob_tenure_band_code,
                    case when a.Lob_tnre_eff_dt is NULL then cast('9999-12-31' as date) else a.Lob_tnre_eff_dt end as Lob_tenure_effective_date,
                    case when a.Intrctn_chnl_tnre_band_cd is NULL then 'Not Defined' else a.Intrctn_chnl_tnre_band_cd end as Interaction_channel_band_code,
                    case when a.Intrctn_chnl_tnre_eff_dt is NULL then cast('9999-12-31' as date) else a.Intrctn_chnl_tnre_eff_dt end as Interaction_channel_effective_date,
                    case when a.Overall_tnre_band_cd is NULL then 'Not Defined' else a.Overall_tnre_band_cd end as Overall_tenure_band_code,
                    case when a.Overall_tnre_eff_dt is NULL then cast('9999-12-31' as date) else a.Overall_tnre_eff_dt end as Overall_tenure_effective_date,
                    a.cep_fl as Agent_cep_flag, 
                    a.unvrsl_fl as Agent_universal_flag,
                    case when a.lob_msg_ctgry is NULL then 'Not Defined' else a.lob_msg_ctgry end as Agent_lob_messaging_category,
                    case when a.lob_msg_ctgry2 is NULL then 'Not Defined' else a.lob_msg_ctgry2 end as Agent_lob_messaging_category_2,
                    case when a.Agn_tier is NULL then 'Not Defined' else a.Agn_tier end as Agent_tier,
                    CURRENT_TIMESTAMP(0) as create_ts,
                    a.updt_ts as update_ts,
                    a.IS_LRNG_LAB_AGN_IND as IS_LRNG_LAB_AGENT_IND,
                    case when a.AGN_TNRE_BAND_CD is NULL then 'Not Defined' else a.AGN_TNRE_BAND_CD end as agent_tenure_band_code,
                    case when a.AGN_TNRE_EFF_DT is NULL then cast('9999-12-31' as date) else a.AGN_TNRE_EFF_DT end as agent_tenure_effective_date
                    FROM DP_VEDW_SRC_COD.DIM_AGENT a
                    left join DP_VEDW_SRC_COD.DIM_AGENT b
                    on a.Agn_Supr_CSR_ID=b.CSR_ID and a.Agn_Supr_Rec_Key=b.Agn_Rec_Key
                    where a.UPDT_TS > to_date('{pc_start_exec_date}', 'yyyy-MM-dd hh24:mi:ss');"""
        
        logging.info(f"<====source sql query====>\n {src_sql_query}")

        try:
            # Execute the SQL query
            cursor = td_conn.cursor()
            cursor.execute(src_sql_query)
            
            # Fetch all rows
            rows = cursor.fetchall()
            if not rows:
                logging.warning("The query returned no rows.")
                sys.exit(1)

            # Fetch column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Get the absolute path of the current script
            script_path = os.path.abspath(__file__)
            script_dir = os.path.dirname(script_path)
            output_file = f"{script_dir}/dim_agent.csv"

            # Write to CSV
            with open(output_file, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file, delimiter='|')
                writer.writerow(column_names)  # Write header
                writer.writerows(rows)  # Write data

            logging.info(f"Data successfully written to {output_file}")
            return output_file

        except Exception as e:
            logging.error(f"Error: {e}")
            sys.exit(1)

        finally:
            cursor.close()
        
    except Exception as e:
        logging.error(f"An unexpected error occurred in function {td_to_file.__name__}: {str(e)}")
        logging.error(traceback.format_exc())  
        sys.exit(1)
        

def file_to_s3_rd_stg_table(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,
                            rshft_db_port,bucket_name,s3_copy_path,iam_role,get_file,
                           ):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname,user=rshft_db_user,password=rshft_db_password,
                                    host=rshft_db_host,port=rshft_db_port) as conn:
            
            logging.info("Redshift Database Connection successful!")
            
            with conn.cursor() as cur:
                # Truncate the table
                truncate_sql = f"TRUNCATE TABLE edw_datamart_stg.stg_dim_agent;"
                cur.execute(truncate_sql)
                conn.commit()
                logging.info(f"edw_datamart_stg.stg_dim_agent table truncated successfully.")
            
                aws_copy_command = ['/usr/local/bin/aws', 's3', 'cp',
                                        f'{get_file}',
                                        f's3://{bucket_name}/{s3_copy_path}'
                                        ]
                logging.info(f"<====aws-copy-command====>\n{' '.join(aws_copy_command)}")
                    
                #copy command for copy the processed file 
                aws_copy_command_result = subprocess.run(
                                            aws_copy_command,
                                            text=True, 
                                            capture_output=True  # Capture stdout and stderr
                                            )
                logging.info(f"Command executed with return code: {aws_copy_command_result.returncode}")
                # logging.info(f"Standard Output: {aws_copy_command_result.stdout}")
                logging.info(f"Standard Error: {aws_copy_command_result.stderr}")
                                            
                if aws_copy_command_result.returncode == 0:
                    logging.info(f"{get_file} copied to {bucket_name}/{s3_copy_path} successfully")
                else:
                    logging.error(f"aws_copy_command command failed with return code {aws_copy_command_result.returncode}")
                    logging.error(f"aws_copy_command Error:\n{aws_copy_command_result.stderr}")
                    sys.exit(1)
                    
                copy_file = f"""COPY edw_datamart_stg.stg_dim_agent
                            FROM 's3://{bucket_name}/{s3_copy_path}'
                            IAM_ROLE '{iam_role}'
                            DELIMITER '|'
                            region 'us-east-1'
                            IGNOREHEADER 1
                            DATEFORMAT 'auto'
                            CSV;"""
                
                cur.execute(copy_file)
                conn.commit()
                count_query = f"""select count(*) from edw_datamart_stg.stg_dim_agent;"""
                cur.execute(count_query)
                stage_table_count = cur.fetchone()[0]
                logging.info(f"{stage_table_count} records are inserted into the edw_datamart_stg.stg_dim_agent stage table ")
                
                get_upd_ts_query = """select max(update_ts) as DeltaStartDt  FROM edw_datamart_stg.stg_dim_agent;"""
                cur.execute(get_upd_ts_query)
                update_ts = cur.fetchone()
                
                return update_ts[0]
                
    except Exception as e:
        conn.rollback()  # Rollback in case of any error
        logging.error(f"An unexpected error occurred in function {file_to_s3_rd_stg_table.__name__}: {str(e)}")
        logging.error(traceback.format_exc())  
        sys.exit(1) 
        
def stg_to_tgt_load(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,rshft_db_port,
                    ):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname,user=rshft_db_user,password=rshft_db_password,
                                    host=rshft_db_host,port=rshft_db_port) as conn:
            with conn.cursor() as cur:
            # Perform an upsert operation  by first deleting matching rows in the target table,
            # and then inserting new or updated rows from the staging table.
                delete_query = f"""delete from edw_datamart.dim_agent
                                    where exists (select AGENT_KEY_ID  from 
                                    edw_datamart_stg.stg_dim_agent stg 
                                    where stg.AGENT_KEY_ID  = edw_datamart.dim_agent.AGENT_KEY_ID );
                                """
                logging.info(f"<====delete sql query====>\n {delete_query}")
                cur.execute(delete_query)
                delete_count = cur.rowcount
                conn.commit() 
                logging.info(f"{delete_count} rows are deleted successfully from edw_datamart.dim_agent table")
                insert_query = f"""insert into edw_datamart.dim_agent (
                                AGENT_KEY_ID,
                                CSR_ID,
                                REC_EFF_TS,
                                REC_EXP_TS,
                                AGENT_ID,
                                AGENT_FIRST_NM,
                                AGENT_LAST_NM,
                                VENDOR_NM,
                                VENDOR_SITE_NM,
                                VENDOR_LCTN_NM,
                                AGENT_LOB_CD,
                                AGENT_SMS_ROLE_CD,
                                AGENT_MRKTNG_ROLE_CD,
                                ACTVTN_SRC_CD,
                                INTRCTN_CHNL_NM,
                                IS_WORK_AT_HOME_IND,
                                IS_AGENT_ACTV_IND,
                                SUPRVSR_CSR_ID,
                                SUPRVSR_FIRST_NM,
                                SUPRVSR_LAST_NM,
                                SUPRVSR_LOB_CD,
                                SUPRVSR_SMS_ROLE_CD,
                                SUPRVSR_MRKTNG_ROLE_CD,
                                LOB_TENURE_BAND_CODE,
                                LOB_TENURE_EFFECTIVE_DATE,
                                INTERACTION_CHANNEL_BAND_CODE,
                                INTERACTION_CHANNEL_EFFECTIVE_DATE,
                                OVERALL_TENURE_BAND_CODE,
                                OVERALL_TENURE_EFFECTIVE_DATE,
                                AGENT_CEP_FLAG,
                                AGENT_UNIVERSAL_FLAG,
                                AGENT_LOB_MESSAGING_CATEGORY,
                                AGENT_LOB_MESSAGING_CATEGORY_2,
                                AGENT_TIER,
                                IS_LATEST_IND,
                                CREATE_TS,
                                UPDATE_TS,
                                IS_LRNG_LAB_AGENT_IND,
                                agent_tenure_band_code,
                                agent_tenure_effective_date
                                )
                                SELECT
                                AGENT_KEY_ID,
                                CSR_ID,
                                REC_EFF_TS,
                                REC_EXP_TS,
                                AGENT_ID,
                                AGENT_FIRST_NM,
                                AGENT_LAST_NM,
                                VENDOR_NM,
                                VENDOR_SITE_NM,
                                VENDOR_LCTN_NM,
                                AGENT_LOB_CD,
                                AGENT_SMS_ROLE_CD,
                                AGENT_MRKTNG_ROLE_CD,
                                ACTVTN_SRC_CD,
                                INTRCTN_CHNL_NM,
                                IS_WORK_AT_HOME_IND,
                                IS_AGENT_ACTV_IND,
                                SUPRVSR_CSR_ID,
                                SUPRVSR_FIRST_NM,
                                SUPRVSR_LAST_NM,
                                SUPRVSR_LOB_CD,
                                SUPRVSR_SMS_ROLE_CD,
                                SUPRVSR_MRKTNG_ROLE_CD,
                                LOB_TENURE_BAND_CODE,
                                LOB_TENURE_EFFECTIVE_DATE,
                                INTERACTION_CHANNEL_BAND_CODE,
                                INTERACTION_CHANNEL_EFFECTIVE_DATE,
                                OVERALL_TENURE_BAND_CODE,
                                OVERALL_TENURE_EFFECTIVE_DATE,
                                AGENT_CEP_FLAG,
                                AGENT_UNIVERSAL_FLAG,
                                AGENT_LOB_MESSAGING_CATEGORY,
                                AGENT_LOB_MESSAGING_CATEGORY_2,
                                AGENT_TIER,
                                IS_LATEST_IND,
                                CREATE_TS,
                                UPDATE_TS,
                                IS_LRNG_LAB_AGENT_IND,
                                agent_tenure_band_code,
                                agent_tenure_effective_date
                                from 
                                edw_datamart_stg.stg_dim_agent;
                                """
                logging.info(f"<====inserte sql query====>\n {insert_query}")
                cur.execute(insert_query)
                insert_count = cur.rowcount
                conn.commit() 
                logging.info(f"{insert_count} rows are inserted successfully into the edw_datamart_stg.stg_dim_agent table")
                    
    except Exception as e:
        logging.error(f"An unexpected error occurred in function {stg_to_tgt_load.__name__}: {str(e)}")
        logging.error(traceback.format_exc())  
        sys.exit(1)
    
def update_pc_start_exec_ts(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,
                                    rshft_db_port,get_update_ts):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname,user=rshft_db_user,password=rshft_db_password,
                                    host=rshft_db_host,port=rshft_db_port) as conn:
            
            logging.info("Redshift Database Connection successful!")
            
            with conn.cursor() as cur:
                
                update_query = f"""update edw_ods.process_control  
                                set start_exec_ts = '{get_update_ts}' where process_nm='DIM_AGENT';"""
                logging.info(f"<====update sql query====>\n {update_query}")
                cur.execute(update_query)
                conn.commit()
                           
    except Exception as e:
        logging.error(f"An unexpected error occurred in function {update_pc_start_exec_ts.__name__}: {str(e)}")
        logging.error(traceback.format_exc())  
        sys.exit(1) 

if __name__ == "__main__":
    
    try:
        if len(sys.argv) != 2:
            print("Usage: {sys.argv[0]} <path_to_config.yaml>")
            sys.exit(1)

        yaml_file_path = sys.argv[1]

        # Load YAML configuration
        config = load_yaml_file(yaml_file_path)
        
        # Extract database configuration
        db_config = config.get('database_connectivity_details', {})
        td_db_host = db_config.get('td_host')
        td_db_user = db_config.get('td_username')
        td_db_password = db_config.get('td_password')
        
        # Extract S3 configuration
        s3_config = config.get('s3_bucket_details', {})
        bucket_name = s3_config.get('bucket_nm')
        s3_copy_path = s3_config.get('s3_copy_path')
        iam_role = s3_config.get('iam_role')
        
        
        rshft_db_host = db_config.get('rshft_host')
        rshft_db_user = db_config.get('rshft_username')
        rshft_db_password = db_config.get('rshft_password')
        rshft_db_dbname = db_config.get('dbname')
        rshft_db_port = db_config.get('rshft_port')
    
        
        
        # Get the absolute path of the current script
        script_path=os.path.abspath(__file__)
        # Get the directory of the current script
        script_dir=os.path.dirname(script_path)
        
        # Go one directory up
        parent_dir = os.path.dirname(script_dir)
        
        # Navigate to the 'logs' folder within the parent directory
        logs_dir = os.path.join(parent_dir, 'logs')
        
        log_file_nm=os.path.basename(script_path).replace('.py','')


        # Create log file with timestamp
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name_with_timestamp = f"{log_file_nm}_{current_time}.log"
        full_log_path = os.path.join(logs_dir, log_file_name_with_timestamp)
        
        # Setup logging
        start_logging(full_log_path)
        
        # Retrieve sensitive database details securely
        td_db_host = get_prm_dtls_ssm_pms(td_db_host)
        td_db_user =  get_prm_dtls_ssm_pms(td_db_user)
        td_db_password = get_prm_dtls_ssm_pms(td_db_password)
        
        rshft_db_host = get_prm_dtls_ssm_pms(rshft_db_host)
        rshft_db_user = get_prm_dtls_ssm_pms(rshft_db_user)
        rshft_db_password = get_prm_dtls_ssm_pms(rshft_db_password)
        rshft_db_port = get_prm_dtls_ssm_pms(rshft_db_port)
        
        # Extract startexects from redshfit process control table
        
        pc_start_exec_date = get_pc_start_exec_ts(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,
                                    rshft_db_port)
        if pc_start_exec_date:
            
            # Teradata to file load
            
            get_file = td_to_file(td_db_host,td_db_user,td_db_password,pc_start_exec_date)
        
            if get_file:
                
                # file to Redshift stage load
                get_update_ts = file_to_s3_rd_stg_table(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,
                                        rshft_db_port,bucket_name,s3_copy_path,iam_role,get_file,
                                        )
                #stage to target load 
                stg_to_tgt_load(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,
                                rshft_db_port)
                
                # remove s3 file  from ec2 server
                if os.path.exists(get_file):
                    os.remove(get_file)
                    logging.info(f"Removed processed file from ec2 server: {get_file}")
                    
                # Update the start_exec_ts for the next run.
                update_pc_start_exec_ts(rshft_db_dbname,rshft_db_user,rshft_db_password,rshft_db_host,
                                rshft_db_port,get_update_ts)
                logging.info("Bye bye, script works successfully.")
                       
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main block: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)     