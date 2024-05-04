import click
import sqlite3
import logging
import os
from datetime import datetime
from helper import replace_swearwords_parallel, schema_check, load_reviews, load_badwords, execute_query, replace_swearwords_serial, \
      create_folders, write_aggregated_to_bucket, write_discarded_to_bucket, write_processed_to_bucket


logging.basicConfig(level=logging.INFO, \
    format='%(asctime)s:%(levelname)s:%(message)s')

@click.command()
@click.option('--input', 'input_path', required=True, help='Path for Reviews (JSONL File)')
@click.option('--inappropriate_words', 'inappropriate_path', required=True, help='Path for Inappropriate Words (TXT File)')
@click.option('--output', 'output_folder_path', required=True, help='Path for Writing Processed Reviews (FOLDER PATH)')
@click.option('--aggregations', 'aggregations_folder_path', required=True, help='Path for Writing Aggregated Results (FOLDER PATH)')
@click.option('--schema', 'schema_path', required=True, help='Path for JSON Schema for Validating Reviews (JSON FILE)')
def process_reviews(input_path, schema_path, inappropriate_path, output_folder_path, aggregations_folder_path):
    
    db_name = "reviews.db"
    conn = sqlite3.connect(db_name)

    logging.info("Process starts...")
    processed_path, discarded_path, aggregations_path, valid_path, invalid_path = \
        create_folders(output_folder_path, aggregations_folder_path)

    #Run schema check and write the valid and invalid reviews to path
    schema_check(input_path,valid_path,invalid_path,schema_path)
    
    #Create table reviews and insert data
    execute_query(conn, 'sql/create_table_reviews.sql', path=True)
    load_reviews(conn, valid_path)

    #Create table badwords and insert data
    execute_query(conn, 'sql/create_table_badwords.sql', path=True)
    load_badwords(conn, inappropriate_path)

    #Cencor swearwords with "*****"
    #Parallel processing requires each process to create its own db connection
    replace_swearwords_parallel(db_name)
    
    #For small size data the parallel processing can 
    #cause overhead so use serial processing instead
    #replace_swearwords_serial(conn)
    
    #Create view processed reviews
    execute_query(conn, 'sql/create_view_processed.sql', path=True)
    logging.info("Process reviews and create view --> Success.")

    #Create view aggregated results
    execute_query(conn, 'sql/create_view_aggregated.sql', path=True)
    logging.info("Aggregate processed reviews and create view --> Success.")

    #region write processed, discarded, aggregated results to the buckets.
    write_processed_to_bucket(conn, processed_path)
    write_discarded_to_bucket(conn, discarded_path)
    write_aggregated_to_bucket(conn, aggregations_path)
    #endregion

    conn.close()
    logging.info("Process completed successfully.")
if __name__ == '__main__':
    process_reviews()