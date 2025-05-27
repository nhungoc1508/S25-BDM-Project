import pandas as pd
import re

def essential_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the essential columns for leave_of_absence.
    """
    cols = ['loa_id', 'student_id', 'loa_year', 'return_year']
    df = df[cols]
    df.to_csv('essential_columns_leave_of_absence.csv', index=False)
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    print(f"Removed {before - len(df)} duplicate rows")
    return df

def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast loa_id to nullable int, and ensure other fields are strings.
    """
    df['loa_id']      = pd.to_numeric(df['loa_id'], errors='coerce').astype('int')
    df['student_id']  = df['student_id'].astype(str)
    df['loa_year']    = df['loa_year'].astype(str)
    df['return_year'] = df['return_year'].astype(str)
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows missing critical fields.
    """
    critical = ['loa_id', 'student_id']
    before = len(df)
    df = df.dropna(subset=critical)
    print(f"Dropped {before - len(df)} rows missing critical data")
    df.to_csv('missing_values_leave_of_absence.csv', index=False)
    return df

def validate_year_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure loa_year and return_year follow 'YYYY - YYYY' pattern.
    """
    pattern = re.compile(r'^\d{4}\s*-\s*\d{4}$')
    before = len(df)
    df = df[df['loa_year'].str.match(pattern, na=False) & 
            df['return_year'].str.match(pattern, na=False)]
    print(f"Removed {before - len(df)} rows with invalid year format")
    return df

def preprocessing_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = essential_columns(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    df = validate_year_format(df)
    return df
