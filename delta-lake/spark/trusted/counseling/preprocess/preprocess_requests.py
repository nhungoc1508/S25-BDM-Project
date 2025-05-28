from pyspark.sql.types import StructType
from pyspark.sql.functions import col, struct, array, lower, to_timestamp

import ftfy
import unicodedata
import re

def normalize_text(text):
    if not isinstance(text, str):
        return text
    # Fix mojibake, HTML entities, and broken unicode
    text = ftfy.fix_text(text)
    text = unicodedata.normalize('NFKC', text)
    # Remove control/invisible characters
    text = re.sub(r'[\u0000-\u001F\u200b-\u200f\u202a-\u202e\u2060-\u206f]', '', text)
    return text

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

def normalize_request(df):
    """
    Normalize meeting_request.purpose/description to remove non-standard characters
    """
    df = df.withColumn(
        "meeting_request",
        struct(
            col("meeting_request.purpose"),
            col("meeting_request.description"),
            normalize_text(col("meeting_request.purpose_lowercase")).alias("purpose_lowercase_norm"),
            normalize_text(col("meeting_request.description_lowercase")).alias("description_lowercase_norm")
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
    df = normalize_request(df)
    df = cast_timestamp(df)
    return df