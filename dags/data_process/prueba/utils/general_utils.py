import pandas as pd
import requests
import os


## download csv from url
def download_csv_url(url_csv, folder_dir, file_name = None, format_file = 'csv'):
    """Download csv from url."""
    # Download file
    if file_name == None:
        file_name = 'data_raw.csv'
    else:
        file_name = file_name + '.' + format_file
    
    file_path = os.path.join(folder_dir, file_name)
    r = requests.get(url_csv, stream=True)
    with open(file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

def read_csv(file_path:str, sep = ',', encoding = 'utf-8', header = 0)-> pd.DataFrame:
    """Read csv."""
    df = pd.read_csv(file_path, sep = sep, encoding = encoding, header = header)
    return df

def clean_data(df:pd.DataFrame, subset_columns:list) -> pd.DataFrame:
    """Clean data."""
    # DRop NaN for subset columns
    df = df.dropna(axis=0, subset=subset_columns)
    return df

def clean_transform_data(df:pd.DataFrame, subset_columns:list, path_dest:str) -> pd.DataFrame:
    """Transform data."""
    # Clean data
    df = clean_data(df, subset_columns)
    # Groupby
    df = df.groupby(['WEATHER']).agg(
        collision_count = pd.NamedAgg(
            column='WEATHER', 
            aggfunc='count'
            )
        ).reset_index()
    # Save data
    df.to_csv(path_dest, index=False)
