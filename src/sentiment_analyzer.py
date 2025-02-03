import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, collect_list, struct, count, when, input_file_name, regexp_extract, array, size
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType
from google.cloud import storage
from collections import Counter
from datetime import datetime

# Create Spark session with Spark NLP and GCS connector
spark = sparknlp.start()

# Print versions
print(f"Spark NLP Version: {sparknlp.version()}")
print(f"Spark Version: {spark.version}")

# Create the pipeline components
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

# Add UniversalSentenceEncoder for embeddings
use_embeddings = UniversalSentenceEncoder.pretrained() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence_embeddings")

# Load the sentimentdl_use_twitter model
sentiment_model = SentimentDLModel.pretrained("sentimentdl_use_twitter") \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCol("sentiment")

# Create and fit the pipeline
pipeline = Pipeline(stages=[
    document_assembler,
    use_embeddings,
    sentiment_model
])

print("Pipeline components created successfully!")

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
    return "gs://2324_fall_yzv_2024/trailer_data/comments_parquet/20250125_150835"

def analyze_sentiment_batch(comments, batch_size=100):
    """Analyze sentiment for a batch of comments."""
    results = []
    batch = []
    
    for comment in comments:
        if not comment or not comment.text:
            continue
            
        batch.append(comment)
        
        if len(batch) >= batch_size:
            try:
                # Create DataFrame from batch
                batch_data = [(c.text,) for c in batch]
                batch_df = spark.createDataFrame(batch_data, ["text"])
                
                # Apply the pipeline
                result = pipeline.fit(batch_df).transform(batch_df)
                
                # Extract sentiments
                analyzed_rows = result.select("text", "sentiment.result").collect()
                
                # Add results
                for row in analyzed_rows:
                    sentiment = row.result[0] if row.result else "neutral"
                    results.append({
                        'text': row.text,
                        'sentiment': sentiment.lower(),
                        'confidence': 1.0  # Default confidence as the model doesn't provide confidence scores
                    })
            except Exception as e:
                print(f"Error analyzing batch: {str(e)}")
            
            batch = []
    
    # Process remaining comments
    if batch:
        try:
            batch_data = [(c.text,) for c in batch]
            batch_df = spark.createDataFrame(batch_data, ["text"])
            
            result = pipeline.fit(batch_df).transform(batch_df)
            analyzed_rows = result.select("text", "sentiment.result").collect()
            
            for row in analyzed_rows:
                sentiment = row.result[0] if row.result else "neutral"
                results.append({
                    'text': row.text,
                    'sentiment': sentiment.lower(),
                    'confidence': 1.0
                })
        except Exception as e:
            print(f"Error analyzing final batch: {str(e)}")
    
    return results

def get_sentiment_stats(sentiments):
    """Calculate sentiment statistics."""
    sentiment_counts = Counter(item['sentiment'] for item in sentiments)
    total = len(sentiments)
    
    return {
        'total_analyzed': total,
        'sentiment_distribution': {
            sentiment: {
                'count': count,
                'percentage': (count / total) * 100 if total > 0 else 0
            }
            for sentiment, count in sentiment_counts.items()
        },
        'average_confidence': sum(item['confidence'] for item in sentiments) / total if total > 0 else 0
    }

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
    video_sentiments = {}
    
    # Group comments by video_title and process in smaller chunks
    for video_title in comments_df.select("video_title").distinct().collect():
        video_title = video_title.video_title
        
        # Get comments for this video
        video_comments = comments_df.filter(col("video_title") == video_title)
        total_comments = video_comments.count()
        
        print(f"\nAnalyzing sentiments for: {video_title}")
        print(f"Total comments to process: {total_comments}")
        
        # Process in chunks of 1000 comments
        chunk_size = 1000
        total_processed = 0
        all_sentiments = []
        
        for chunk in range(0, total_comments, chunk_size):
            chunk_comments = video_comments.limit(chunk_size).collect()
            chunk_sentiments = analyze_sentiment_batch(chunk_comments, batch_size=100)
            all_sentiments.extend(chunk_sentiments)
            
            total_processed += len(chunk_sentiments)
            print(f"Processed {total_processed}/{total_comments} comments...")
        
        # Calculate statistics
        sentiment_stats = get_sentiment_stats(all_sentiments)
        
        # Store results
        video_sentiments[video_title] = {
            'total_comments': total_comments,
            'sentiment_stats': sentiment_stats,
            'sentiment_examples': {
                'positive': next((item['text'] for item in all_sentiments if item['sentiment'] == 'positive'), None),
                'negative': next((item['text'] for item in all_sentiments if item['sentiment'] == 'negative'), None),
                'neutral': next((item['text'] for item in all_sentiments if item['sentiment'] == 'neutral'), None)
            }
        }
        
        print(f"Completed analysis for {video_title}")
        print(f"Sentiment distribution:", sentiment_stats['sentiment_distribution'])
    
    # Save the final results
    final_results = {
        'video_sentiments': video_sentiments,
        'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_videos_analyzed': len(video_sentiments)
    }
    
    # Upload results to GCS
    print("\nSaving results...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{GCS_RESULTS_PATH}/sentiment_analysis_{timestamp}.json"
    
    # Convert to JSON string
    results_json = json.dumps(final_results, ensure_ascii=False, indent=2)
    
    # Write to GCS
    blob = bucket.blob(f"trailer_data/results/sentiment_analysis_{timestamp}.json")
    blob.upload_from_string(results_json, content_type='application/json')
    
    print(f"\nAnalysis completed! Results saved to: {output_path}")
    
    # Print summary
    print("\nFinal Sentiment Analysis Summary:")
    for video, data in video_sentiments.items():
        print(f"\n{video}:")
        print(f"Total Comments: {data['total_comments']}")
        print(f"Analyzed Comments: {data['sentiment_stats']['total_analyzed']}")
        print("\nSentiment Distribution:")
        for sentiment, stats in data['sentiment_stats']['sentiment_distribution'].items():
            print(f"- {sentiment}: {stats['count']} ({stats['percentage']:.1f}%)")
        print(f"Average Confidence: {data['sentiment_stats']['average_confidence']:.2f}")

except Exception as e:
    print(f"An error occurred during processing: {str(e)}")
    raise
