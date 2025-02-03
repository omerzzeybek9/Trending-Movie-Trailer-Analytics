import sparknlp
from sparknlp.pretrained import PretrainedPipeline
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, collect_list, struct, count, when, input_file_name, regexp_extract
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from google.cloud import storage
from collections import Counter
from datetime import datetime

# Create Spark session with Spark NLP and GCS connector
spark = sparknlp.start()

# Print versions
print(f"Spark NLP Version: {sparknlp.version()}")
print(f"Spark Version: {spark.version}")

# Load the NER pipeline
print("Loading NER pipeline...")
pipeline = PretrainedPipeline('recognize_entities_dl', 'en')
print("Pipeline loaded successfully!")

# GCS Configuration - Hardcoded values
BUCKET_NAME = "2324_fall_yzv_2024"
COMMENTS_DIR = "comments_parquet"
RESULTS_DIR = "results"

# Full GCS paths
GCS_COMMENTS_PATH = "gs://2324_fall_yzv_2024/trailer_data/comments_parquet"
GCS_RESULTS_PATH = "gs://2324_fall_yzv_2024/trailer_data/results"

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

def get_latest_parquet_path():
    """Return the hardcoded path for the latest comments.parquet directory in GCS."""
    return "gs://2324_fall_yzv_2024/trailer_data/comments_parquet/20250108_115121"

def extract_entities(annotations):
    """Extract entities and their types from annotations."""
    entities = []
    for i in range(len(annotations['token'])):
        if annotations['ner'][i] != 'O':  # 'O' means no entity
            entity_type = annotations['ner'][i].split('-')[1] if '-' in annotations['ner'][i] else annotations['ner'][i]
            entity_text = annotations['token'][i]
            entities.append({
                'text': entity_text.lower(),
                'type': entity_type
            })
    return entities

def analyze_entities_batch(comments, batch_size=1000):
    """Analyze entities for a batch of comments."""
    all_entities = []
    batch = []
    
    for comment in comments:
        if not comment or not comment.text:
            continue
            
        batch.append(comment)
        
        if len(batch) >= batch_size:
            try:
                # Combine texts for batch processing
                combined_text = " [SEP] ".join(c.text for c in batch)
                result = pipeline.annotate(combined_text)
                entities = extract_entities(result)
                all_entities.extend(entities)
            except Exception as e:
                print(f"Error analyzing batch: {str(e)}")
            
            batch = []
    
    # Process remaining comments
    if batch:
        try:
            combined_text = " [SEP] ".join(c.text for c in batch)
            result = pipeline.annotate(combined_text)
            entities = extract_entities(result)
            all_entities.extend(entities)
        except Exception as e:
            print(f"Error analyzing final batch: {str(e)}")
    
    return all_entities

def get_top_entities(entities_list, top_n=10):
    """Get top N entities with their types and counts."""
    entity_counter = Counter()
    entity_types = {}
    
    for entity in entities_list:
        entity_text = entity['text']
        entity_counter[entity_text] += 1
        entity_types[entity_text] = entity['type']
    
    top_entities = []
    for text, count in entity_counter.most_common(top_n):
        top_entities.append({
            'text': text,
            'type': entity_types[text],
            'count': count
        })
    
    return top_entities

# Register UDF for entity analysis
entity_udf = udf(lambda arr: analyze_entities_batch(arr), ArrayType(StringType()))

try:
    # Get the latest parquet file path
    parquet_path = get_latest_parquet_path()
    if not parquet_path:
        raise ValueError("No comments.parquet files found in GCS")
    
    print(f"\nReading comments from: {parquet_path}")
    
    # Read all parquet files in the directory and add video_title column
    comments_df = spark.read.parquet(f"{parquet_path}/*") \
        .withColumn("input_file", input_file_name()) \
        .withColumn("video_title", regexp_extract(col("input_file"), "([^/]+)_comments\.parquet", 1)) \
        .cache()  # Cache the DataFrame
    
    total_comments = comments_df.count()
    print(f"Loaded {total_comments} comments")
    
    # Process comments by video
    movie_entities = {}
    
    # Process each video separately
    for video_title in comments_df.select("video_title").distinct().collect():
        video_title = video_title.video_title
        
        # Get comments for this video
        video_comments = comments_df.filter(col("video_title") == video_title)
        total_video_comments = video_comments.count()
        
        print(f"\nAnalyzing entities for: {video_title}")
        print(f"Total comments to process: {total_video_comments}")
        
        # Process in chunks of 1000 comments
        chunk_size = 1000
        total_processed = 0
        all_entities = []
        
        # Convert to RDD, add indices, and back to DataFrame for proper chunking
        video_comments_indexed = video_comments.rdd.zipWithIndex() \
            .map(lambda x: (*x[0], x[1])) \
            .toDF(video_comments.schema.names + ["index"])
        
        while total_processed < total_video_comments:
            # Get next chunk of comments using index
            chunk_comments = video_comments_indexed \
                .where((col("index") >= total_processed) & (col("index") < total_processed + chunk_size)) \
                .collect()
                
            if not chunk_comments:
                break
                
            # Process the chunk in smaller batches
            chunk_entities = analyze_entities_batch(chunk_comments, batch_size=1000)
            all_entities.extend(chunk_entities)
            
            total_processed += len(chunk_comments)
            progress = (total_processed / total_video_comments) * 100
            print(f"Progress: {progress:.1f}% ({total_processed}/{total_video_comments} comments)")
        
        # Get top entities
        top_entities = get_top_entities(all_entities)
        
        # Group entities by type
        entities_by_type = {}
        for entity in top_entities:
            entity_type = entity['type']
            if entity_type not in entities_by_type:
                entities_by_type[entity_type] = []
            entities_by_type[entity_type].append({
                'text': entity['text'],
                'count': entity['count']
            })
        
        # Store results
        movie_entities[video_title] = {
            'total_comments': total_video_comments,
            'processed_comments': total_processed,
            'total_entities': len(all_entities),
            'top_entities': top_entities,
            'entities_by_type': entities_by_type
        }
        
        print(f"Found {len(all_entities)} entities")
        print(f"Top entities: {top_entities}")
    
    # Save the final results
    final_results = {
        'movie_entities': movie_entities,
        'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_movies_analyzed': len(movie_entities),
        'analysis_parameters': {
            'chunk_size': chunk_size,
            'batch_size': 1000
        }
    }
    
    # Upload results to GCS
    print("\nSaving results...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{GCS_RESULTS_PATH}/entity_analysis_{timestamp}.json"
    
    # Convert to JSON string
    results_json = json.dumps(final_results, ensure_ascii=False, indent=2)
    
    # Write to GCS
    blob = bucket.blob(f"trailer_data/results/entity_analysis_{timestamp}.json")
    blob.upload_from_string(results_json, content_type='application/json')
    
    print(f"\nAnalysis completed! Results saved to: {output_path}")
    
    # Print summary
    print("\nFinal Entity Analysis Summary:")
    for movie, data in movie_entities.items():
        print(f"\n{movie}:")
        print(f"Processed Comments: {data['processed_comments']}/{data['total_comments']}")
        print(f"Total Entities: {data['total_entities']}")
        print("\nTop Entities:")
        for entity in data['top_entities']:
            print(f"- {entity['text']} ({entity['type']}): {entity['count']} occurrences")

except Exception as e:
    print(f"An error occurred during processing: {str(e)}")
    raise 