import pandas as pd
def essential_column(df):
    """
    Keep only essential columns in the DataFrame.
    """
    essential_columns = ['evaluation_id', 'student_id', 'course_code', 'evaluation', 'weight', 'score']
    df[essential_columns].to_csv('essential_columns.csv')
    return df[essential_columns]

def remove_duplicates(df):
    return df.drop_duplicates()


def cast_data_types(df):
    """
    Cast data types appropriately.
    """
    df['evaluation_id'] = pd.to_numeric(df['evaluation_id'], errors='coerce')
    df['student_id'] = df['student_id'].astype(str)
    df['course_code'] = df['course_code'].astype(str)
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
    df['score'] = pd.to_numeric(df['score'], errors='coerce')
    return df

def handle_missing_values(df):
    """
    Handle missing values in critical columns.
    """
    critical_columns = ['evaluation_id', 'student_id', 'course_code']
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
    df = df[df['weight'] > 0]
    df = df[df['score'] > 0]
    df = df[df['evaluation_id'].notna()]
    df = df[df['student_id'].notna()]
    df = df[df['course_code'].notna()]
    print(f"Removed {before_count - len(df)} rows with invalid values in critical columns")
    return df


def preprocessing_pipeline(df):
    """
    Run the preprocessing pipeline.
    """
    df = essential_column(df)
    df = cast_data_types(df)
    # df = remove_duplicates(df)
    # df = handle_missing_values(df)
    # df = remove_invalid_values(df)
    return df