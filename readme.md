
# Movie Trailer Analysis System

## How to Run the Project

### 1. Set Up the Environment
1. Create a new Python virtual environment:
   ```bash
   python -m venv env
   source env/bin/activate  # For Linux/Mac
   env\Scripts\activate  # For Windows
   ```
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt

---

### 2. Data Collection and Preprocessing (Local Execution)
#### a) Collect Trailer Metadata
- Open the `data_collection.py` file.
- Fill in your **YouTube API key** in the appropriate placeholder.
- Run the script:
  ```bash
  python data_collection.py
  ```
- The script will save metadata for popular movies in the `trailer_data/data` directory.

#### b) Collect Comments
- Open the `get_comments.py` file.
- Fill in your **YouTube API key** in the appropriate placeholder.
- Run the script:
  ```bash
  python get_comments.py
  ```
- The script will fetch comments for the trailers listed in the `trailer_data/data` directory and save them to comments dir.

#### c) Convert JSON to Parquet
- Run the conversion script:
  ```bash
  python convert_to_parquet.py
  ```
- This script will convert all JSON files in `trailer_data/data` to Parquet format. This step ensures efficient storage and compatibility with Spark jobs and save them to comment_parquet dir.

---

### 3. Upload Data to Google Cloud
- Upload the `trailer_data` directory to your Google Cloud Storage (GCS) bucket.

---

### 4. Update Hardcoded Paths
- Open each file in the `src` folder.
- Update any hardcoded paths (e.g., bucket names, file paths) to match your GCS bucket configuration.

---

### 5. Upload Scripts to Google Cloud
- Upload all the updated `src` files to your **GCS bucket**.

---

### 6. Configure Google Cloud Dataproc
- Create a Dataproc cluster for running Spark jobs.
- Follow the [Spark NLP GCP Dataproc Guide](https://sparknlp.org/docs/en/install#gcp-dataproc) to set up your cluster with Spark and Spark NLP preinstalled.

---

### 7. Submit Spark Jobs
- Submit the analysis scripts as Spark jobs using the Google Cloud SDK Shell:
  ```bash
  gcloud dataproc jobs submit pyspark <script_name.py> \
    --cluster=<cluster_name> \
    --region=<region>
  ```
- Example:
  ```bash
  gcloud dataproc jobs submit pyspark sentiment_analysis.py \
    --cluster=my-cluster \
    --region=us-central1
  ```

---

### 8. View Results
- The analysis results (e.g., sentiment distributions, entity recognition, topic models) will be printed in terminal and saved in the `trailer_data/results` directory in your GCS bucket.
- You can download or further process the results as needed.

### 9. Test the Website
 - Run the following command to start a local server:
   ```bash
   python -m http.server
   ```
- Open your browser and go to:  
   [http://localhost:8000/website/main.html](http://localhost:8000/website/main.html)

**Note**: The website is currently in its alpha stage. Future improvements will include a redesigned frontend and integration with APIs to create a dynamic, fully functional website.
---

## Notes
- **Local Execution**: Steps 2(a) to 2(c) are meant to be executed on your local machine.
- **Google Cloud Setup**: Ensure you have the necessary permissions and API keys configured for your GCS bucket and Dataproc cluster.
- **Hardcoded Paths**: It is essential to update the hardcoded paths in all scripts before uploading them to your GCS bucket.

Follow these steps carefully to successfully execute the project. If you encounter any issues, ensure all dependencies are installed, and double-check your Google Cloud configurations.
```

