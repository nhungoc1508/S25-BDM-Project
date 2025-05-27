import pandas as pd

def essential_columns(df):
    """
    Keep only essential columns.
    """
    essential_cols = ['academic_year_id', 'start_year', 'end_year', 'start_date', 'end_date']
    df = df[essential_cols]
    df.to_csv('essential_columns_academic_years.csv', index=False)
    return df

def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates()
    print(f"Removed {before - len(df)} duplicate rows")
    return df

def cast_data_types(df):
    """
    Cast columns to appropriate types.
    """
    df['academic_year_id'] = df['academic_year_id'].astype(str)
    df['start_year'] = pd.to_numeric(df['start_year'], errors='coerce').astype('int')  # Nullable int
    df['end_year'] = pd.to_numeric(df['end_year'], errors='coerce').astype('int')
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    return df

def handle_missing_values(df):
    """
    Drop rows missing critical columns.
    """
    critical_cols = ['academic_year_id', 'start_year', 'end_year']
    before = len(df)
    df = df.dropna(subset=critical_cols)
    print(f"Dropped {before - len(df)} rows missing critical data")
    df.to_csv('missing_values_academic_years.csv', index=False)
    return df

def validate_years(df):
    """
    Ensure start_year <= end_year and dates make sense.
    """
    before = len(df)
    df = df[df['start_year'] <= df['end_year']]

    # Optionally validate start_date and end_date logical order
    df = df[(df['start_date'].isna()) | (df['end_date'].isna()) | (df['start_date'] <= df['end_date'])]
    
    print(f"Removed {before - len(df)} rows with invalid year/date ranges")
    return df

def preprocessing_pipeline(df):
    df = essential_columns(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    df = validate_years(df)
    return df
