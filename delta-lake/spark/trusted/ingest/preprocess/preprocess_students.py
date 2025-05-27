import pandas as pd
def essential_column(df):
    """
    Keep only essential columns in the DataFrame.
    """
    essential_columns = ['student_id', 'name', 'phone', 'address',  'gender', 'email']
    # df[essential_columns].to_csv('essential_columns_courses.csv')
    return df[essential_columns]

def remove_duplicates(df):
    return df.drop_duplicates()


def cast_data_types(df):
    """
    Cast data types appropriately.
    """
    df['student_id'] = df['student_id'].astype(str)
    df['name'] = df['name'].astype(str)
    df['phone'] = df['phone'].astype(str)
    df['address'] = df['address'].astype(str)
    df['gender'] = df['gender'].astype(str)
    df['email'] = df['email'].astype(str)
    return df

def handle_missing_values(df):
    """
    Handle missing values in critical columns.
    """
    critical_columns = ['student_id', 'name', 'email']
    before_count = len(df)
    df = df.dropna(subset=critical_columns)
    print(f"Removed {before_count - len(df)} rows with missing values in critical columns")
    # df.to_csv('missing_values.csv')
    return df


def preprocessing_pipeline(df):
    """
    Run the preprocessing pipeline.
    """
    df = essential_column(df)
    df = remove_duplicates(df)
    df = cast_data_types(df)
    df = handle_missing_values(df)
    return df