from pyspark.sql.types import StructType
from pyspark.sql.functions import col, struct, array, lower, to_timestamp

def lower_request(df):
    """
    Cast meeting_request.purpose/description to lowercase
    """
    df = df.withColumn(
        "meeting_request",
        struct(
            col("meeting_request.purpose"),
            col("meeting_request.description"),
            lower(col("meeting_request.purpose")).alias("purpose_lowercase"),
            lower(col("meeting_request.description")).alias("description_lowercase")
        )
    )
    return df

def cast_timestamp(df):
    """
    Cast timestamp to timestamp type
    """
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    return df

def preprocessing_pipeline(df):
    df = lower_request(df)
    df = cast_timestamp(df)
    return df