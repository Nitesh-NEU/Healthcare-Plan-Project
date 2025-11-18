"""
Spark Job: Healthcare Plan Analytics and Aggregation
Processes large volumes of healthcare plan data and generates analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("Healthcare Plan Analytics") \
        .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/medicalPlans.plans") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/medicalPlans.analytics") \
        .config("spark.jars.packages", 
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
                "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_plans_from_mongodb(spark):
    """Read healthcare plans from MongoDB"""
    print("Reading plans from MongoDB...")
    
    df = spark.read \
        .format("mongodb") \
        .option("connection.uri", "mongodb://mongodb:27017/") \
        .option("database", "medicalPlans") \
        .option("collection", "plans") \
        .load()
    
    print(f"Loaded {df.count()} plans")
    return df

def analyze_cost_trends(df):
    """Analyze cost trends across plan types"""
    print("Analyzing cost trends...")
    
    # Extract plan cost shares
    cost_analysis = df.select(
        col("objectId").alias("plan_id"),
        col("planType").alias("plan_type"),
        col("_org").alias("organization"),
        col("creationDate").alias("creation_date"),
        col("planCostShares.deductible").alias("deductible"),
        col("planCostShares.copay").alias("copay")
    )
    
    # Calculate total costs
    cost_analysis = cost_analysis.withColumn(
        "total_cost", 
        col("deductible") + col("copay")
    )
    
    # Aggregate by plan type
    aggregated = cost_analysis.groupBy("plan_type") \
        .agg(
            count("*").alias("plan_count"),
            avg("deductible").alias("avg_deductible"),
            avg("copay").alias("avg_copay"),
            avg("total_cost").alias("avg_total_cost"),
            min("total_cost").alias("min_total_cost"),
            max("total_cost").alias("max_total_cost"),
            stddev("total_cost").alias("stddev_total_cost")
        )
    
    return aggregated

def analyze_service_patterns(df):
    """Analyze patterns in linked services"""
    print("Analyzing service patterns...")
    
    # Explode linked services
    services_df = df.select(
        col("objectId").alias("plan_id"),
        col("planType").alias("plan_type"),
        explode("linkedPlanServices").alias("service")
    )
    
    # Extract service details
    service_analysis = services_df.select(
        col("plan_id"),
        col("plan_type"),
        col("service.linkedService.name").alias("service_name"),
        col("service.linkedService.objectType").alias("service_type"),
        col("service.planserviceCostShares.copay").alias("service_copay"),
        col("service.planserviceCostShares.deductible").alias("service_deductible")
    )
    
    # Service popularity
    service_popularity = service_analysis.groupBy("service_name") \
        .agg(
            count("*").alias("frequency"),
            avg("service_copay").alias("avg_copay"),
            avg("service_deductible").alias("avg_deductible")
        ) \
        .orderBy(col("frequency").desc())
    
    return service_popularity

def detect_anomalies(df):
    """Detect anomalous costs using statistical methods"""
    print("Detecting cost anomalies...")
    
    # Extract costs
    costs_df = df.select(
        col("objectId").alias("plan_id"),
        col("planType").alias("plan_type"),
        (col("planCostShares.deductible") + col("planCostShares.copay")).alias("total_cost")
    )
    
    # Calculate statistics by plan type
    stats = costs_df.groupBy("plan_type") \
        .agg(
            avg("total_cost").alias("mean_cost"),
            stddev("total_cost").alias("stddev_cost")
        )
    
    # Join back and identify anomalies (beyond 2 standard deviations)
    anomalies = costs_df.join(stats, "plan_type")
    
    anomalies = anomalies.withColumn(
        "z_score",
        (col("total_cost") - col("mean_cost")) / col("stddev_cost")
    )
    
    anomalies = anomalies.filter(abs(col("z_score")) > 2) \
        .select("plan_id", "plan_type", "total_cost", "mean_cost", "z_score") \
        .orderBy(abs(col("z_score")).desc())
    
    return anomalies

def calculate_monthly_metrics(df):
    """Calculate monthly aggregated metrics"""
    print("Calculating monthly metrics...")
    
    # Parse creation date and extract month/year
    monthly_df = df.select(
        col("objectId").alias("plan_id"),
        col("planType").alias("plan_type"),
        to_date(col("creationDate")).alias("creation_date"),
        (col("planCostShares.deductible") + col("planCostShares.copay")).alias("total_cost")
    )
    
    monthly_df = monthly_df.withColumn("year", year("creation_date")) \
                           .withColumn("month", month("creation_date"))
    
    # Aggregate by year, month, and plan type
    monthly_metrics = monthly_df.groupBy("year", "month", "plan_type") \
        .agg(
            count("*").alias("plans_created"),
            avg("total_cost").alias("avg_total_cost"),
            sum("total_cost").alias("total_revenue")
        ) \
        .orderBy("year", "month", "plan_type")
    
    return monthly_metrics

def write_to_postgres(df, table_name):
    """Write DataFrame to PostgreSQL"""
    print(f"Writing to PostgreSQL table: {table_name}")
    
    jdbc_url = "jdbc:postgresql://postgres-warehouse:5432/healthcare_dw"
    properties = {
        "user": "dataeng",
        "password": "dataeng123",
        "driver": "org.postgresql.Driver"
    }
    
    df.write \
        .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
    
    print(f"Successfully wrote {df.count()} rows to {table_name}")

def write_to_mongodb(df, collection_name):
    """Write DataFrame back to MongoDB"""
    print(f"Writing to MongoDB collection: {collection_name}")
    
    df.write \
        .format("mongodb") \
        .option("connection.uri", "mongodb://mongodb:27017/") \
        .option("database", "medicalPlans") \
        .option("collection", collection_name) \
        .mode("overwrite") \
        .save()
    
    print(f"Successfully wrote to {collection_name}")

def main():
    """Main execution function"""
    print("Starting Healthcare Plan Analytics Spark Job...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read data
        plans_df = read_plans_from_mongodb(spark)
        
        # Perform analyses
        cost_trends = analyze_cost_trends(plans_df)
        service_patterns = analyze_service_patterns(plans_df)
        anomalies = detect_anomalies(plans_df)
        monthly_metrics = calculate_monthly_metrics(plans_df)
        
        # Show results
        print("\n=== COST TRENDS BY PLAN TYPE ===")
        cost_trends.show(truncate=False)
        
        print("\n=== TOP 10 SERVICE PATTERNS ===")
        service_patterns.show(10, truncate=False)
        
        print("\n=== COST ANOMALIES ===")
        anomalies.show(truncate=False)
        
        print("\n=== MONTHLY METRICS ===")
        monthly_metrics.show(truncate=False)
        
        # Write results to MongoDB for API access
        write_to_mongodb(cost_trends, "analytics_cost_trends")
        write_to_mongodb(service_patterns, "analytics_service_patterns")
        write_to_mongodb(anomalies, "analytics_anomalies")
        write_to_mongodb(monthly_metrics, "analytics_monthly_metrics")
        
        print("\nSpark job completed successfully!")
        
    except Exception as e:
        print(f"Error in Spark job: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
