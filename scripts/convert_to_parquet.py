import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_latest_directory(base_dir):
    """Find the latest timestamp directory in the given base directory."""
    if not os.path.exists(base_dir):
        return None
    
    dirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if not dirs:
        return None
    
    # Sort directories by name (timestamp) in descending order
    latest_dir = sorted(dirs, reverse=True)[0]
    return os.path.join(base_dir, latest_dir)

def init_spark():
    """Initialize and return a Spark session."""
    return SparkSession.builder \
        .appName("CommentsToParquet") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def define_schema():
    """Define the schema for the comments data."""
    return StructType([
        StructField("author", StringType(), True),
        StructField("text", StringType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("published_at", StringType(), True)
    ])

def convert_to_parquet(input_dir, output_dir):
    """Convert JSON files to Parquet format using PySpark."""
    spark = init_spark()
    schema = define_schema()
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each JSON file in the input directory
    for json_file in os.listdir(input_dir):
        if not json_file.endswith('.json'):
            continue
            
        input_path = os.path.join(input_dir, json_file)
        output_path = os.path.join(output_dir, json_file.replace('.json', '.parquet'))
        
        try:
            # Read JSON file as array
            df = spark.read.option("multiline", "true").schema(schema).json(input_path)
            
            # Write as Parquet
            df.write.mode('overwrite').parquet(output_path)
            print(f"Converted {json_file} to Parquet format")
            print(f"Number of comments converted: {df.count()}")
            
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
            continue
    
    spark.stop()

def main():
    # Set up directories
    comments_base_dir = os.path.join("trailer_data", "comments")
    parquet_base_dir = os.path.join("trailer_data", "comments_parquet")
    
    # Get latest comments directory
    latest_comments_dir = get_latest_directory(comments_base_dir)
    
    if not latest_comments_dir:
        print("No comments directory found. Please run get_comments.py first.")
        exit(1)
    
    # Create new directory for Parquet files with same timestamp
    timestamp = os.path.basename(latest_comments_dir)
    output_dir = os.path.join(parquet_base_dir, timestamp)
    
    print(f"Converting comments from: {latest_comments_dir}")
    print(f"Saving Parquet files to: {output_dir}")
    
    # Convert files
    convert_to_parquet(latest_comments_dir, output_dir)
    print("Conversion completed successfully")

if __name__ == "__main__":
    main() 