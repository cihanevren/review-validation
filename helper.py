import os
import re
import json
import logging
import sqlite3
from datetime import datetime
from jsonschema import validate
from multiprocessing import Pool

logging.getLogger(__name__)

def _is_valid_json_structure(filepath, line):
  """
  Checks if the Review Json Entry is Valid
  Input : Json Schema Path, Json Line (From Reviews)
  Output : Returns True if valid schema else False
  """
  with open(filepath, 'r') as f:
    schema = json.load(f)
  try:
    line = json.loads(line)
    validate(instance=line, schema=schema)
    #check the format of date-time string
    datetime.strptime(line["publishedAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
    return True
  except Exception as e:
    return False

def schema_check(inputpath, validpath, invalidpath, schemapath):
  """
  Applies json schema check to the input file
  Writes valid schemas to validpath
  Invalid schema to invalidpath
  """
  try:
    with open(inputpath, 'r') as inputfile, \
      open(validpath, 'w') as validfile, \
      open(invalidpath, 'w') as invalidfile:
      for line in inputfile:
        if _is_valid_json_structure(schemapath, line):
          validfile.write(line)
        else:
          invalidfile.write(line)
    logging.info("Schema validation --> Success.")
  except Exception as e:
    logging.error(f"Failed during schema check --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)

def execute_query(connection, query, path=False):
  """
  Executes the given query in the table
  Input: database name, table name, sql query
  Output: returns the query results
  """
  if path:
    with open(query, 'r') as f:
      query = f.read()
  cursor = connection.cursor()
  try:
    cursor.execute(query)
    return cursor.fetchall()
  except Exception as e:
    logging.error(f"Failed during executing the query --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)
  finally:
    cursor.close()

def load_reviews(connection, file_path, table_name='reviews'):
  """
  Loads reviews from jsonl file to table
  Input: database name, table name, json file path
  """
  cursor = connection.cursor()
  query_load_data = f'''
  INSERT INTO {table_name} (restaurant_id, review_id, text, rating, published_at)
  VALUES (:restaurantId, :reviewId, :text, :rating, :publishedAt)
  '''
  try:
    with open(file_path, 'r') as file:
        for line in file:
            data = json.loads(line)
            cursor.execute(query_load_data, data)
    cursor.close()
    logging.info("Reviews inserted to the table --> Success.")
  except Exception as e:
    logging.error(f"Failed during loading reviews --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)

def load_badwords(connection, file_path, table_name='inappropriate_words'):
  """
  Loads inapporiate words from txt file to table
  Input: database name, table name, txt file path
  """
  cursor = connection.cursor()
  query_load_data = f'''
  INSERT INTO {table_name} (word)
  VALUES (?)
  '''
  try:
    with open(file_path, 'r') as file:
        for line in file:
            word = line.strip()
            if word:
                cursor.execute(query_load_data, (word,))   
    cursor.close()
    connection.commit()
    logging.info("Inappropriate words inserted to the table --> Success.")
  except Exception as e:
    logging.error(f"Failed during loading inappropriate words --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)

def process_batch(batch_args):
  db_name, start_id, end_id, inappropriate_words = batch_args
  conn = sqlite3.connect(db_name)
  cursor = conn.cursor()

  cursor.execute(f"SELECT id, text FROM reviews WHERE id BETWEEN {start_id} AND {end_id}")
  reviews = cursor.fetchall()

  for review in reviews:
    id, text = review
    for word in inappropriate_words:
      pattern = r'\b' + re.escape(word) + r'\w*\b'
      text = re.sub(pattern, "*****", text, flags=re.IGNORECASE)
    cursor.execute("UPDATE reviews SET text = ? WHERE id = ?", (text, id))
  conn.commit()
  cursor.close()
  conn.close()

def replace_swearwords_parallel(db_name, nproc=4):
  """
  Replaces the swear words with "*****"
  Input: database name
  """
  try:
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT word FROM inappropriate_words")
    inappropriate_words = [row[0] for row in cursor.fetchall()]
    conn.close()

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(id), MAX(id) FROM reviews")
    min_id, max_id = cursor.fetchone()
    conn.close()

    #dividing the workload to processorrs
    step_size = (max_id - min_id + 1) // nproc
    batches = [(db_name, min_id + i*step_size, min_id + (i + 1)*step_size - 1, inappropriate_words) for i in range(nproc)]
    if min_id + nproc*step_size <= max_id:
      batches.append((db_name, min_id + nproc*step_size, max_id, inappropriate_words))
    
    pool = Pool(nproc)
    pool.map(process_batch, batches)
    pool.close()
    pool.join()
    logging.info("Inappropriate words censored --> Success.")
  except Exception as e:
    logging.error(f"Failed during censoring --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)


def replace_swearwords_serial(connection):
  """
  Replaces the swear words with "*****"
  Input: database name
  """
  cursor = connection.cursor()
  cursor.execute("SELECT word FROM inappropriate_words")
  inappropriate_words = [row[0] for row in cursor.fetchall()]

  cursor.execute("SELECT id, text FROM reviews")
  reviews = cursor.fetchall()

  for review in reviews:
    id, text = review
    for word in inappropriate_words:
      pattern = r'\b' + re.escape(word) + r'\w*\b'
      text = re.sub(pattern, "*****", text, flags=re.IGNORECASE)
    cursor.execute("UPDATE reviews SET text = ? WHERE id = ?", (text, id))
  cursor.close()

def write_processed_to_bucket(connection, processed_filepath):
  """
  Writes processed reviews to specified filepath
  Writes aggregated reviews to specified filepath
  """
  #write processed reviews to the path
  try:
    cursor = connection.cursor()
    query = "SELECT * FROM view_processed_reviews"
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(processed_filepath, 'w', encoding='utf-8') as file:
      for row in rows:
        data = {
          'restaurantId': row[0],
          'reviewId': row[1],
          'text': row[2],
          'rating': row[3],
          'publishedAt': row[4]
        }
        json.dump(data, file)
        file.write('\n')
    cursor.close()
    logging.info("Processed reviews written to the bucket --> Success.")
  except Exception as e:
    logging.error(f"Failed during writing processed reviews to the bucket --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)

def write_processed_to_bucket(connection, processed_filepath):
  """
  Writes processed reviews to specified filepath
  Writes aggregated reviews to specified filepath
  """
  #write processed reviews to the path
  try:
    cursor = connection.cursor()
    query = """
    SELECT
      restaurant_id,
      review_id,
      text,
      rating,
      published_at
    FROM view_processed_reviews
    WHERE swear_word_ratio < 0.20
    OR is_outdated IS FALSE
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(processed_filepath, 'w', encoding='utf-8') as file:
      for row in rows:
        data = {
          'restaurantId': row[0],
          'reviewId': row[1],
          'text': row[2],
          'rating': row[3],
          'publishedAt': row[4]
        }
        json.dump(data, file)
        file.write('\n')
    cursor.close()
    logging.info("Processed reviews written to the bucket --> Success.")
  except Exception as e:
    logging.error(f"Failed during writing processed reviews to the bucket --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)

def write_discarded_to_bucket(connection, discarded_filepath):
  """
  Writes processed reviews to specified filepath
  Writes aggregated reviews to specified filepath
  """
  #write processed reviews to the path
  try:
    cursor = connection.cursor()
    query = """
    SELECT
      restaurant_id,
      review_id,
      text,
      rating,
      published_at
    FROM view_processed_reviews
    WHERE swear_word_ratio >= 0.20
    OR is_outdated IS TRUE
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(discarded_filepath, 'w', encoding='utf-8') as file:
      for row in rows:
        data = {
          'restaurantId': row[0],
          'reviewId': row[1],
          'text': row[2],
          'rating': row[3],
          'publishedAt': row[4]
        }
        json.dump(data, file)
        file.write('\n')
    cursor.close()
    logging.info("Discarded reviews written to the bucket --> Success.")
  except Exception as e:
    logging.error(f"Failed during writing discarded reviews to the bucket --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)

def write_aggregated_to_bucket(connection, aggregated_filepath):
  #write aggregated reviews to the path
  try:
    cursor = connection.cursor()
    query = "SELECT * FROM view_aggregated_reviews"
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(aggregated_filepath, 'w', encoding='utf-8') as file:
      for row in rows:
        data = {
          'restaurantId': row[0],
          'reviewCount': row[1],
          'averageRating': row[2],
          'averageReviewLength': row[3],
          'reviewAge': json.loads(row[4])
        }
        json.dump(data, file)
        file.write('\n')
    cursor.close()
    logging.info("Aggregated results written to the bucket --> Success.")
  except Exception as e:
    logging.error(f"Failed during writing aggregated results to the bucket --> Error. \n{e}")
    logging.info("Exiting application...")
    exit(1)

def create_folders(output_folder_path, aggregations_folder_path):
    """
    Creates Destination Paths
    Input: Output folder path, aggregations folder path
    Output: output path, aggregations path, valid path, invalid path
    """
    try:
      os.makedirs(f"{output_folder_path}/processed", exist_ok=True)
      os.makedirs(f"{output_folder_path}/discarded", exist_ok=True)
      os.makedirs(aggregations_folder_path, exist_ok=True)
      os.makedirs("invalid", exist_ok=True)
      os.makedirs("valid", exist_ok=True)

      timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
      valid_path = f"valid/valid_reviews_{timestamp}.jsonl"
      invalid_path = f"invalid/invalid_reviews_{timestamp}.jsonl"
      output_processed_path = f"{output_folder_path}/processed/processed_reviews_{timestamp}.jsonl"
      output_discarded_path = f"{output_folder_path}/discarded/discarded_reviews_{timestamp}.jsonl"
      aggregations_path = f"{aggregations_folder_path}/aggregated_reviews_{timestamp}.jsonl"
      logging.info("Folders created --> Success.")
      return output_processed_path, output_discarded_path, aggregations_path, valid_path, invalid_path
    except Exception as e:
      logging.error(f"Failed during folder creation --> Error. \n{e}")
      logging.info("Exiting application...")
      exit(1)
