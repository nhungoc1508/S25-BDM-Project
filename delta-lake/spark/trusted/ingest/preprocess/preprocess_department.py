import pandas as pd
def essential_column(df):
    """
    Keep only essential columns in the DataFrame.
    """
    essential_columns = ['dept_code', 'division', 'name', 'office_number', 'building_code', 'blurb']
    df[essential_columns].to_csv('essential_columns_courses.csv')
    return df[essential_columns]

def remove_duplicates(df):
    return df.drop_duplicates()


def cast_data_types(df):
    """
    Cast data types appropriately.
    """
    df['dept_code'] = df['dept_code'].astype(str)
    df['division'] = df['division'].astype(str)
    df['name'] = df['name'].astype(str)
    df['office_number'] = df['office_number'].astype(int)
    df['building_code'] = df['building_code'].astype(str)
    df['blurb'] = df['blurb'].astype(str)
    return df

def handle_missing_values(df):
    """
    Handle missing values in critical columns.
    """
    critical_columns = ['dept_code', 'division', 'name']
    before_count = len(df)
    df = df.dropna(subset=critical_columns)
    print(f"Removed {before_count - len(df)} rows with missing values in critical columns")
    df.to_csv('missing_values.csv')
    return df


def remove_invalid_values(df):
    """
    Remove rows with invalid values in critical columns.
    """
    before_count = len(df)
    df = df[df['dept_code'].notna()]
    df = df[df['division'].notna()]
    df = df[df['name'].notna()]
    print(f"Removed {before_count - len(df)} rows with invalid values in critical columns")
    return df


def preprocessing_pipeline(df):
    """
    Run the preprocessing pipeline.
    """
    df = essential_column(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    df = remove_invalid_values(df)
    return df