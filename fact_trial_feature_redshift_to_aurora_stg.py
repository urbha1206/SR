from teradatasql import connect
import psycopg2
from datetime import datetime
import logging
import os
import sys
import traceback
from common.common_utils import load_yaml_file, start_logging, get_prm_dtls_ssm_pms


def redshift_process(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                      rshft_db_port, bucket_name, s3_copy_path, iam_role):
    try:
        with psycopg2.connect(dbname=rshft_db_dbname, user=rshft_db_user, password=rshft_db_password,
                              host=rshft_db_host, port=rshft_db_port) as conn:

            logging.info("Redshift Database Connection successful!")

            with conn.cursor() as cur:
                unload_command = f""" UNLOAD ('SELECT trial_ftre_key_id, trial_id, ftre_cd, rec_exp_ts
                                           FROM edw_datamart.fact_trial_feature
                                           WHERE cast(rec_exp_ts AS date) = ''2999-12-31''')
                                           TO 's3://{bucket_name}/{s3_copy_path}fact_trial_feature_unload.csv'
                                           IAM_ROLE '{iam_role}'
                                           FORMAT AS CSV
                                           HEADER
                                           DELIMITER '|'
                                           ALLOWOVERWRITE
                                           PARALLEL OFF;"""
                logging.info(f"unload command query {unload_command}")
                cur.execute(unload_command)
                conn.commit()
                logging.info("table unloaded successfully")

    except Exception as e:
        logging.error(f"An unexpected error occurred in function {redshift_process.__name__}: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)


def aurora_proecss(aurora_dbname, aurora_username, aurora_password, aurora_host,
                   aurora_port, bucket_name, s3_copy_path):
    try:
        with psycopg2.connect(dbname=aurora_dbname, user=aurora_username, password=aurora_password,
                              host=aurora_host, port=aurora_port) as conn:

            logging.info("Aurora Database Connection successful!")

            with conn.cursor() as cur:
                truncate_table = """TRUNCATE TABLE edw_ods_stg.preld_stg_fact_trial_feature;"""
                logging.info(f"Truncate query===>{truncate_table}")
                cur.execute(truncate_table)
                logging.info("table truncated successfully")

                load_from_s3_to_stage_table = f"""select aws_s3.table_import_from_s3(
                                                'edw_ods_stg.preld_stg_fact_trial_feature',
                                                'trial_ftre_key_id,trial_id,ftre_cd,rec_exp_ts',
                                                '(format csv,delimiter "|",header)',
                                                '{bucket_name}',
                                                '{s3_copy_path}fact_trial_feature_unload.csv000',
                                                'us-east-1');"""
                logging.info(f"load from s3 query===>{load_from_s3_to_stage_table}")
                cur.execute(load_from_s3_to_stage_table)
                conn.commit()
                logging.info("table inserted successfully")

                update_command = """UPDATE edw_ods_stg.stg_fact_trial_feature s
                                    SET trial_id   = p.trial_id,
                                        ftre_cd    = p.ftre_cd,
                                        rec_exp_ts = p.rec_exp_ts
                                    FROM edw_ods_stg.preld_stg_fact_trial_feature p
                                    WHERE s.trial_ftre_key_id = p.trial_ftre_key_id;"""
                logging.info(f"update query===>{update_command}")
                cur.execute(update_command)
                logging.info(f"updated records count {cur.rowcount}")
                conn.commit()
                logging.info("table updated successfully")

                insert_command = """
                                INSERT INTO edw_ods_stg.stg_fact_trial_feature
                                (trial_ftre_key_id,trial_id,ftre_cd,rec_exp_ts)
                                SELECT p.trial_ftre_key_id,p.trial_id,p.ftre_cd,p.rec_exp_ts
                                FROM edw_ods_stg.preld_stg_fact_trial_feature p
                                WHERE trial_ftre_key_id NOT IN
                                (SELECT trial_ftre_key_id FROM edw_ods_stg.stg_fact_trial_feature);"""
                logging.info(f"insert query===>{insert_command}")
                cur.execute(insert_command)
                logging.info(f"inserted records count {cur.rowcount}")
                conn.commit()
                logging.info("table inserted successfully")

                delete_command = """
                                    DELETE FROM edw_ods_stg.stg_fact_trial_feature
                                    WHERE trial_ftre_key_id NOT IN
                                    (SELECT trial_ftre_key_id FROM edw_ods_stg.preld_stg_fact_trial_feature);"""
                logging.info(f"delete query===>{delete_command}")
                cur.execute(delete_command)
                logging.info(f"deleted records count {cur.rowcount}")
                conn.commit()
                logging.info("table rows deleted successfully")

    except Exception as e:
        logging.error(f"An unexpected error occurred in function {aurora_proecss.__name__}: {str(e)}")
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
        s3_config = config.get('s3_bucket_details', {})

        bucket_name = s3_config.get('bucket_nm')
        s3_copy_path = s3_config.get('s3_copy_path')
        iam_role = s3_config.get('iam_role')

        rshft_db_host = db_config.get('rshft_host')
        rshft_db_user = db_config.get('rshft_username')
        rshft_db_password = db_config.get('rshft_password')
        rshft_db_dbname = db_config.get('dbname')
        rshft_db_port = db_config.get('rshft_port')

        aurora_host = db_config.get('aurora_host')
        aurora_username = db_config.get('aurora_username')
        aurora_password = db_config.get('aurora_password')
        aurora_dbname = db_config.get('aurora_dbname')
        aurora_port = db_config.get('aurora_port')

        script_path = os.path.abspath(__file__)
        script_dir = os.path.dirname(script_path)
        parent_dir = os.path.dirname(script_dir)
        logs_dir = os.path.join(parent_dir, 'logs')
        log_file_nm = os.path.basename(script_path).replace('.py','')
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name_with_timestamp = f"{log_file_nm}_{current_time}.log"
        full_log_path = os.path.join(logs_dir, log_file_name_with_timestamp)

        start_logging(full_log_path)

        rshft_db_host = get_prm_dtls_ssm_pms(rshft_db_host)
        rshft_db_user = get_prm_dtls_ssm_pms(rshft_db_user)
        rshft_db_password = get_prm_dtls_ssm_pms(rshft_db_password)
        rshft_db_port = get_prm_dtls_ssm_pms(rshft_db_port)

        aurora_host = get_prm_dtls_ssm_pms(aurora_host)
        aurora_username = get_prm_dtls_ssm_pms(aurora_username)
        aurora_password = get_prm_dtls_ssm_pms(aurora_password)

        redshift_process(rshft_db_dbname, rshft_db_user, rshft_db_password, rshft_db_host,
                         rshft_db_port, bucket_name, s3_copy_path, iam_role)

        aurora_proecss(aurora_dbname, aurora_username, aurora_password, aurora_host,
                       aurora_port, bucket_name, s3_copy_path)

        logging.info("Bye bye, script works successfully.")

    except Exception as e:
        logging.error(f"An unexpected error occurred in the main block: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)
