## Restaurant Review Validation Pipeline

### Takes the following arguments:
 - input --> the input path of the data
 - inappropriate_words -->input path of inappropriate words
 - schema --> path for the json schema file
 - output --> output path for the processed reviews
 - aggregations --> path for the aggregated results

### Example command line:
**install the requirements first**

 - **pip install -r requirements.txt**

**then run the main function**

 - **python main.py --input "data/reviews.jsonl" --inappropriate_words "data/inappropriate_words.txt" --schema "schema/review.json" --output output --aggregations aggregations**
