from teradatasql import connect
import psycopg2
import csv
from datetime import datetime
import logging
import os
import sys
import traceback
import subprocess
from common.common_utils import load_yaml_file, start_logging, get_prm_dtls_ssm_pms


def get_pc_start_exec_ts(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                          rshft_db_port):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname, user=rshft_db_user, password=rshft_db_password,
                              host=rshft_db_host, port=rshft_db_port) as conn:
            logging.info("Redshift Database Connection successful!")
            with conn.cursor() as cur:
                select_control_query = """select start_exec_ts as date from
                                  edw_ods.process_control WHERE process_nm='FACT_TRIAL_FEATURE';"""
                cur.execute(select_control_query)
                pc_start_exec_ts = cur.fetchone()
                if pc_start_exec_ts and pc_start_exec_ts[0]:
                    logging.info("Timestamp fetched successfully for process_nm='FACT_TRIAL_FEATURE'")
                    return pc_start_exec_ts[0]
                else:
                    logging.error("No timestamp found for process_nm='FACT_TRIAL_FEATURE'")
                    sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred in function {get_pc_start_exec_ts.__name__}: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)


def td_to_file(td_db_host, td_db_user, td_db_password, pc_start_exec_date):
    try:
        td_conn = connect(host=td_db_host, user=td_db_user, password=td_db_password)
        logging.info("Teradata Database Connection successful!")
        src_sql_query = f"""
                    SELECT *
                    FROM DP_VEDW_SRC_TLD.FACT_TRIAL_FEATURE
                    WHERE CHG_DT_STRT >= DATE '{pc_start_exec_date}'
                """
        cursor = td_conn.cursor()
        cursor.execute(src_sql_query)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        output_file = f"fact_trial_feature_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        with open(output_file, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter='|')
            writer.writerow(column_names)
            writer.writerows(rows)
        logging.info(f"Data successfully written to {output_file}")
        return output_file
    except Exception as e:
        logging.error(f"Error: {e}")
        sys.exit(1)
    finally:
        cursor.close()


def file_to_s3_rd_stg_table(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                            rshft_db_port, bucket_name, s3_copy_path, iam_role, get_file):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname, user=rshft_db_user, password=rshft_db_password,
                              host=rshft_db_host, port=rshft_db_port) as conn:
            logging.info("Redshift Database Connection successful!")
            with conn.cursor() as cur:
                truncate_sql = "TRUNCATE TABLE edw_datamart_stg.stg_fact_trial_feature;"
                cur.execute(truncate_sql)
                conn.commit()
                logging.info("edw_datamart_stg.stg_fact_trial_feature table truncated successfully.")
                aws_copy_command = ['/usr/local/bin/aws', 's3', 'cp', f'{get_file}', f's3://{bucket_name}/{s3_copy_path}']
                logging.info(f"<====aws-copy-command====>\n{' '.join(aws_copy_command)}")
                aws_copy_command_result = subprocess.run(aws_copy_command, text=True, capture_output=True)
                logging.info(f"Command executed with return code: {aws_copy_command_result.returncode}")
                logging.info(f"Standard Error: {aws_copy_command_result.stderr}")
                if aws_copy_command_result.returncode == 0:
                    logging.info(f"{get_file} copied to {bucket_name}/{s3_copy_path} successfully")
                else:
                    logging.error(f"aws_copy_command command failed with return code {aws_copy_command_result.returncode}")
                    logging.error(f"aws_copy_command Error:\n{aws_copy_command_result.stderr}")
                    sys.exit(1)
                copy_file = f"""COPY edw_datamart_stg.stg_fact_trial_feature
                            FROM 's3://{bucket_name}/{s3_copy_path}'
                            IAM_ROLE '{iam_role}'
                            DELIMITER '|'
                            region 'us-east-1'
                            IGNOREHEADER 1
                            DATEFORMAT 'auto'
                            CSV;"""
                cur.execute(copy_file)
                conn.commit()
                count_query = "select count(*) from edw_datamart_stg.stg_fact_trial_feature;"
                cur.execute(count_query)
                stage_table_count = cur.fetchone()[0]
                logging.info(f"{stage_table_count} records are inserted into the edw_datamart_stg.stg_fact_trial_feature stage table ")
                get_upd_ts_query = "select max(updt_ts) as DeltaStartDt  FROM edw_datamart_stg.stg_fact_trial_feature;"
                cur.execute(get_upd_ts_query)
                update_ts = cur.fetchone()
                return update_ts[0]
    except Exception as e:
        conn.rollback()
        logging.error(f"An unexpected error occurred in function {file_to_s3_rd_stg_table.__name__}: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)


def stg_to_tgt_load(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host, rshft_db_port):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname, user=rshft_db_user, password=rshft_db_password,
                              host=rshft_db_host, port=rshft_db_port) as conn:
            with conn.cursor() as cur:
                delete_query = """delete from edw_datamart.fact_trial_feature
                                    where exists (select trial_ftre_key_id  from
                                    edw_datamart_stg.stg_fact_trial_feature stg
                                    where stg.trial_ftre_key_id  = edw_datamart.fact_trial_feature.trial_ftre_key_id );"""
                logging.info(f"<====delete sql query====>\n {delete_query}")
                cur.execute(delete_query)
                delete_count = cur.rowcount
                conn.commit()
                logging.info(f"{delete_count} rows are deleted successfully from edw_datamart.fact_trial_feature table")
                insert_query = """INSERT INTO edw_datamart.fact_trial_feature
                                SELECT * FROM edw_datamart_stg.stg_fact_trial_feature;"""
                logging.info(f"<====insert sql query====>\n {insert_query}")
                cur.execute(insert_query)
                insert_count = cur.rowcount
                conn.commit()
                logging.info(f"{insert_count} rows are inserted successfully into the edw_datamart.fact_trial_feature table")
    except Exception as e:
        logging.error(f"An unexpected error occurred in function {stg_to_tgt_load.__name__}: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)


def update_pc_start_exec_ts(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                            rshft_db_port, get_update_ts):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname, user=rshft_db_user, password=rshft_db_password,
                              host=rshft_db_host, port=rshft_db_port) as conn:
            logging.info("Redshift Database Connection successful!")
            with conn.cursor() as cur:
                update_query = f"update edw_ods.process_control set start_exec_ts = '{get_update_ts}' where process_nm='FACT_TRIAL_FEATURE';"
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
        config = load_yaml_file(yaml_file_path)
        db_config = config.get('database_connectivity_details', {})
        td_db_host = db_config.get('td_host')
        td_db_user = db_config.get('td_username')
        td_db_password = db_config.get('td_password')
        s3_config = config.get('s3_bucket_details', {})
        bucket_name = s3_config.get('bucket_nm')
        s3_copy_path = s3_config.get('s3_copy_path')
        iam_role = s3_config.get('iam_role')
        rshft_db_host = db_config.get('rshft_host')
        rshft_db_user = db_config.get('rshft_username')
        rshft_db_password = db_config.get('rshft_password')
        rshft_db_dbname = db_config.get('dbname')
        rshft_db_port = db_config.get('rshft_port')
        script_path = os.path.abspath(__file__)
        script_dir = os.path.dirname(script_path)
        parent_dir = os.path.dirname(script_dir)
        logs_dir = os.path.join(parent_dir, 'logs')
        log_file_nm = os.path.basename(script_path).replace('.py','')
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name_with_timestamp = f"{log_file_nm}_{current_time}.log"
        full_log_path = os.path.join(logs_dir, log_file_name_with_timestamp)
        start_logging(full_log_path)
        td_db_host = get_prm_dtls_ssm_pms(td_db_host)
        td_db_user = get_prm_dtls_ssm_pms(td_db_user)
        td_db_password = get_prm_dtls_ssm_pms(td_db_password)
        rshft_db_host = get_prm_dtls_ssm_pms(rshft_db_host)
        rshft_db_user = get_prm_dtls_ssm_pms(rshft_db_user)
        rshft_db_password = get_prm_dtls_ssm_pms(rshft_db_password)
        rshft_db_port = get_prm_dtls_ssm_pms(rshft_db_port)
        pc_start_exec_date = get_pc_start_exec_ts(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                                    rshft_db_port)
        if pc_start_exec_date:
            get_file = td_to_file(td_db_host, td_db_user, td_db_password, pc_start_exec_date)
            if get_file:
                get_update_ts = file_to_s3_rd_stg_table(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                                        rshft_db_port, bucket_name, s3_copy_path, iam_role, get_file)
                stg_to_tgt_load(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host, rshft_db_port)
                if os.path.exists(get_file):
                    os.remove(get_file)
                    logging.info(f"Removed processed file from ec2 server: {get_file}")
                update_pc_start_exec_ts(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                                rshft_db_port, get_update_ts)
                logging.info("Bye bye, script works successfully.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main block: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)
