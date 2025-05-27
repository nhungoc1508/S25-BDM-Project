import pandas as pd
import re

def essential_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the essential columns for graduations.
    """
    cols = ['graduation_id', 'student_id', 'dept_code', 'graduation_year']
    df = df[cols]
    df.to_csv('essential_columns_graduations.csv', index=False)
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    print(f"Removed {before - len(df)} duplicate rows")
    return df

def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast graduation_id to int, and ensure other fields are strings.
    """
    df['graduation_id']   = pd.to_numeric(df['graduation_id'], errors='coerce').astype('int')
    df['student_id']      = df['student_id'].astype(str)
    df['dept_code']       = df['dept_code'].astype(str)
    df['graduation_year'] = df['graduation_year'].astype(str)
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows missing critical fields.
    """
    critical = ['graduation_id', 'student_id', 'dept_code']
    before = len(df)
    df = df.dropna(subset=critical)
    print(f"Dropped {before - len(df)} rows missing critical data")
    df.to_csv('missing_values_graduations.csv', index=False)
    return df

def validate_year_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure graduation_year follows 'YYYY - YYYY' pattern.
    """
    pattern = re.compile(r'^\d{4}\s*-\s*\d{4}$')
    before = len(df)
    df = df[df['graduation_year'].str.match(pattern, na=False)]
    print(f"Removed {before - len(df)} rows with invalid graduation_year format")
    return df

def preprocessing_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = essential_columns(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    df = validate_year_format(df)
    return df
