import pandas as pd
def essential_column(df):
    """
    Keep only essential columns in the DataFrame.
    """
    essential_columns = ['course_code', 'title', 'description', 'dept_code', 'credits','pre_reqs', 'core_area', 'inquiry_area', 'recommendation']
    df[essential_columns].to_csv('essential_columns_courses.csv')
    return df[essential_columns]

def remove_duplicates(df):
    return df.drop_duplicates()


def cast_data_types(df):
    """
    Cast data types appropriately.
    """
    df['course_code'] = df['course_code'].astype(str)
    df['title'] = df['title'].astype(str)
    df['description'] = df['description'].astype(str)
    return df

def handle_missing_values(df):
    """
    Handle missing values in critical columns.
    """
    critical_columns = ['course_code', 'title']
    before_count = len(df)
    df = df.dropna(subset=critical_columns)
    print(f"Removed {before_count - len(df)} rows with missing values in critical columns")
    df.to_csv('missing_values.csv')
    return df


def preprocessing_pipeline(df):
    """
    Run the preprocessing pipeline.
    """
    df = essential_column(df)
    df = cast_data_types(df)
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    return df