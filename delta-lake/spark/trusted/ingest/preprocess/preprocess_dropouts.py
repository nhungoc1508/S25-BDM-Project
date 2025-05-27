import pandas as pd
import re

def essential_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the essential columns for dropouts.
    """
    cols = ['dropout_id', 'student_id', 'dropout_year']
    df = df[cols]
    df.to_csv('essential_columns_dropouts.csv', index=False)
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    print(f"Removed {before - len(df)} duplicate rows")
    return df

def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast dropout_id to int and ensure student_id and dropout_year are strings.
    """
    df['dropout_id']  = pd.to_numeric(df['dropout_id'], errors='coerce').astype('int')
    df['student_id']  = df['student_id'].astype(str)
    df['dropout_year']= df['dropout_year'].astype(str)
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows missing critical fields.
    """
    critical = ['dropout_id', 'student_id']
    before = len(df)
    df = df.dropna(subset=critical)
    print(f"Dropped {before - len(df)} rows missing critical data")
    df.to_csv('missing_values_dropouts.csv', index=False)
    return df

def validate_year_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure dropout_year follows 'YYYY - YYYY' pattern.
    """
    pattern = re.compile(r'^\d{4}\s*-\s*\d{4}$')
    before = len(df)
    df = df[df['dropout_year'].str.match(pattern, na=False)]
    print(f"Removed {before - len(df)} rows with invalid dropout_year format")
    return df

def preprocessing_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = essential_columns(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    df = validate_year_format(df)
    return df
