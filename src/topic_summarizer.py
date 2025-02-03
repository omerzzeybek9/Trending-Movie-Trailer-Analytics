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
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA

# Create Spark session with Spark NLP
spark = sparknlp.start()

# Print versions
print(f"Spark NLP Version: {sparknlp.version()}")
print(f"Spark Version: {spark.version}")

# Create the pipeline components
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document") \
    .setCleanupMode("shrink")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normalized")

stopwords_cleaner = StopWordsCleaner() \
    .setInputCols("normalized") \
    .setOutputCol("cleanTokens") \
    .setCaseSensitive(False)

finisher = Finisher() \
    .setInputCols(["cleanTokens"]) \
    .setOutputCols(["tokens"]) \
    .setOutputAsArray(True) \
    .setCleanAnnotations(False)

# Create and fit the NLP pipeline
nlp_pipeline = Pipeline(stages=[
    document_assembler,
    tokenizer,
    normalizer,
    stopwords_cleaner,
    finisher
])

print("Pipeline components created successfully!")

# GCS Configuration
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

def extract_topics(comments_df, num_topics=5, max_iterations=10, vocab_size=500, min_df=5.0):
    """Extract topics from comments using LDA."""
    try:
        # Process through NLP pipeline
        print("Processing text through NLP pipeline...")
        processed_df = nlp_pipeline.fit(comments_df).transform(comments_df)
        
        # Filter out empty token arrays and select only non-empty ones
        tokens_df = processed_df.select('tokens').filter(size(col('tokens')) > 0)
        
        if tokens_df.count() == 0:
            print("No valid tokens found after processing")
            return None

        print("Converting tokens to vectors...")
        # Convert tokens to vectors
        cv = CountVectorizer(inputCol="tokens", outputCol="features", 
                           vocabSize=vocab_size, minDF=min_df)
        cv_model = cv.fit(tokens_df)
        vectorized_tokens = cv_model.transform(tokens_df)
        
        if vectorized_tokens.count() == 0:
            print("No valid vectors created after CountVectorizer")
            return None

        print(f"Training LDA model with {num_topics} topics...")
        # Apply LDA with optimized parameters
        lda = LDA(k=num_topics, 
                 maxIter=max_iterations,
                 optimizer="online",
                 learningOffset=1024.0,
                 learningDecay=0.51,
                 subsamplingRate=0.05,
                 optimizeDocConcentration=True)
        
        lda_model = lda.fit(vectorized_tokens)

        # Extract topics
        vocab = cv_model.vocabulary
        topics = lda_model.describeTopics(maxTermsPerTopic=10)  # Get top 10 words per topic

        # Convert topic terms to words
        def get_words(idx_list):
            return [vocab[idx] for idx in idx_list]

        get_words_udf = udf(get_words, ArrayType(StringType()))
        topics_df = topics.withColumn("words", get_words_udf(topics.termIndices))

        # Calculate model metrics
        log_likelihood = lda_model.logLikelihood(vectorized_tokens)
        log_perplexity = lda_model.logPerplexity(vectorized_tokens)

        print("Topic extraction completed successfully")
        return {
            'topics': topics_df.select("topic", "words").collect(),
            'vocabulary_size': len(vocab),
            'model_metrics': {
                'log_likelihood': float(log_likelihood),
                'log_perplexity': float(log_perplexity)
            }
        }

    except Exception as e:
        print(f"Error in topic extraction: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

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
    video_topics = {}
    
    # Group comments by video_title and process
    for video_title in comments_df.select("video_title").distinct().collect():
        video_title = video_title.video_title
        
        # Get comments for this video
        video_comments = comments_df.filter(col("video_title") == video_title)
        total_comments = video_comments.count()
        
        print(f"\nExtracting topics for: {video_title}")
        print(f"Total comments to process: {total_comments}")
        
        # Extract topics
        topics_result = extract_topics(video_comments)
        
        if topics_result:
            # Convert topics to serializable format
            topics_data = [{
                'topic_id': topic.topic,
                'words': topic.words,
            } for topic in topics_result['topics']]
            
            video_topics[video_title] = {
                'total_comments': total_comments,
                'topics': topics_data,
                'vocabulary_size': topics_result['vocabulary_size'],
                'model_metrics': topics_result['model_metrics']
            }
            
            print(f"Completed topic extraction for {video_title}")
            print("Top words for each topic:")
            for topic in topics_data:
                print(f"Topic {topic['topic_id']}: {', '.join(topic['words'][:5])}")
    
    # Save the final results
    final_results = {
        'video_topics': video_topics,
        'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_videos_analyzed': len(video_topics)
    }
    
    # Upload results to GCS
    print("\nSaving results...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{GCS_RESULTS_PATH}/topic_analysis_{timestamp}.json"
    
    # Convert to JSON string
    results_json = json.dumps(final_results, ensure_ascii=False, indent=2)
    
    # Write to GCS
    blob = bucket.blob(f"trailer_data/results/topic_analysis_{timestamp}.json")
    blob.upload_from_string(results_json, content_type='application/json')
    
    print(f"\nAnalysis completed! Results saved to: {output_path}")
    
    # Print summary
    print("\nFinal Topic Analysis Summary:")
    for video, data in video_topics.items():
        print(f"\n{video}:")
        print(f"Total Comments: {data['total_comments']}")
        print(f"Number of Topics: {len(data['topics'])}")
        print(f"Vocabulary Size: {data['vocabulary_size']}")
        print("\nTop Topics:")
        for topic in data['topics'][:5]:  # Show top 5 topics
            print(f"Topic {topic['topic_id']}: {', '.join(topic['words'][:5])}")

except Exception as e:
    print(f"An error occurred during processing: {str(e)}")
    import traceback
    traceback.print_exc()
    raise
