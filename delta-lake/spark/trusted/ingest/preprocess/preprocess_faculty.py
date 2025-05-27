import pandas as pd
def essential_column(df):
    """
    Keep only essential columns in the DataFrame.
    """
    essential_columns = ['instructor_code', 'first_name', 'last_name', 'dept_code', 'phone', 'email', 'office_number', 'building_code', 'status', 'terminal_degree', 'institution']
    # df[essential_columns].to_csv('essential_columns_courses.csv')
    return df[essential_columns]

def remove_duplicates(df):
    return df.drop_duplicates()


def cast_data_types(df):
    """
    Cast data types appropriately.
    """
    df['instructor_code'] = df['instructor_code'].astype(str)
    df['first_name'] = df['first_name'].astype(str)
    df['last_name'] = df['last_name'].astype(str)
    df['dept_code'] = df['dept_code'].astype(str)
    df['phone'] = df['phone'].astype(str)
    df['email'] = df['email'].astype(str)
    df['office_number'] = df['office_number'].astype(str)
    df['building_code'] = df['building_code'].astype(str)
    df['status'] = df['status'].astype(str)
    df['terminal_degree'] = df['terminal_degree'].astype(str)
    df['institution'] = df['institution'].astype(str)
    return df

def handle_missing_values(df):
    """
    Handle missing values in critical columns.
    """
    critical_columns = ['instructor_code', 'first_name', 'last_name', 'dept_code', 'email']
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