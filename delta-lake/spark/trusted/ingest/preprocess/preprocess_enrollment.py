import pandas as pd
def essential_column(df):
    """
    Keep only essential columns in the DataFrame.
    """
    essential_columns = ['student_id', 'course_code', 'semester', 'is_taking', 'grade']
    # df[essential_columns].to_csv('essential_columns_courses.csv')
    return df[essential_columns]

def remove_duplicates(df):
    return df.drop_duplicates()


def cast_data_types(df):
    """
    Cast data types appropriately.
    """
    df['student_id'] = df['student_id'].astype(str)
    df['course_code'] = df['course_code'].astype(str)
    df['semester'] = df['semester'].astype(str)
    df['is_taking'] = df['is_taking'].astype(bool)
    df['grade'] = pd.to_numeric(df['grade'], errors='coerce')
    return df

def handle_missing_values(df):
    """
    Handle missing values in critical columns.
    """
    critical_columns = ['student_id', 'course_code', 'semester', 'is_taking']
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