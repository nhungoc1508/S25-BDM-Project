import pandas as pd

def essential_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only the essential columns for degree_names.
    """
    cols = ['degree_id', 'dept_code', 'department', 'major', 'degree_name']
    df = df[cols]
    # Optional: persist a snapshot
    df.to_csv('essential_columns_degree_names.csv', index=False)
    return df

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    print(f"Removed {before - len(df)} duplicate rows")
    return df

def cast_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure correct dtypes: degree_id as int, others as string.
    """
    df['degree_id'] = pd.to_numeric(df['degree_id'], errors='coerce').astype('int')
    for col in ['dept_code', 'department', 'major', 'degree_name']:
        df[col] = df[col].astype(str)
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows missing critical columns.
    """
    critical = ['degree_id', 'dept_code', 'degree_name']
    before = len(df)
    df = df.dropna(subset=critical)
    print(f"Dropped {before - len(df)} rows missing critical data")
    df.to_csv('missing_values_degree_names.csv', index=False)
    return df

def validate_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure dept_code and degree_name are non-empty strings.
    """
    before = len(df)
    df = df[df['dept_code'].str.strip() != ""]
    df = df[df['degree_name'].str.strip() != ""]
    print(f"Removed {before - len(df)} rows with blank codes or names")
    return df

def preprocessing_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = essential_columns(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    df = validate_codes(df)
    return df
