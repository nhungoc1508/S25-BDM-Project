import pandas as pd
import re

def essential_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the essential columns for student_enrollment.
    """
    cols = ['student_id', 'academic_year', 'year', 'major']
    df = df[cols]
    df.to_csv('essential_columns_student_enrollment.csv', index=False)
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    print(f"Removed {before - len(df)} duplicate rows")
    return df

def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast year to nullable Int and ensure others are strings.
    """
    df['student_id']     = df['student_id'].astype(str)
    df['academic_year']  = df['academic_year'].astype(str)
    df['year']           = pd.to_numeric(df['year'], errors='coerce').astype('int')
    df['major']          = df['major'].astype(str)
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows missing critical fields.
    """
    critical = ['student_id', 'academic_year', 'year', 'major']
    before = len(df)
    df = df.dropna(subset=critical)
    print(f"Dropped {before - len(df)} rows missing critical data")
    df.to_csv('missing_values_student_enrollment.csv', index=False)
    return df

def validate_year_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure academic_year follows 'YYYY - YYYY' and year is positive.
    """
    pattern = re.compile(r'^\d{4}\s*-\s*\d{4}$')
    before = len(df)
    df = df[df['academic_year'].str.match(pattern, na=False)]
    df = df[df['year'] > 0]
    print(f"Removed {before - len(df)} rows with invalid academic_year or year")
    return df

def preprocessing_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = essential_columns(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    df = validate_year_format(df)
    return df
