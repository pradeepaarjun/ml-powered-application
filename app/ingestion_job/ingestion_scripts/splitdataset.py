import os
import pandas as pd
import numpy as np
 
 
def introduce_data_issues(df):
    print(df)
    df.loc[df.sample(frac=0.1).index, 'duration'] = df.loc[df.sample(frac=0.1).index, 'duration'] * 1.1
    df.loc[df.sample(frac=0.1).index, 'days_left'] = df.loc[df.sample(frac=0.1).index, 'days_left'] + np.random.normal(0, 5, size=len(df.sample(frac=0.1)))
    df.loc[df.sample(frac=0.1).index, 'airline'] = 'UnknownAirline'
    df.loc[df.sample(frac=0.1).index, 'duration'] = df.loc[df.sample(frac=0.1).index, 'duration'] * -1
    df.loc[df.sample(frac=0.1).index, 'days_left'] = df.loc[df.sample(frac=0.1).index, 'days_left'] * -1
    airline_mapping = {'SpiceJet': 1, 'AirAsia': 2, 'Vistara': 3, 'GO_FIRST': 4, 'Indigo': 5, 'Air_India': 6}  
    df.loc[df.sample(frac=0.1).index, 'airline'] = df.loc[df.sample(frac=0.1).index, 'airline'].map(airline_mapping)
    df = pd.concat([df, df.sample(frac=0.05)])
    return df
 
 
def split_dataset(main_dataset_path, raw_data_folder_path, num_files):
    os.makedirs(raw_data_folder_path, exist_ok=True)
    df = pd.read_csv(main_dataset_path)
    rows_per_file = len(df) // num_files
    for i in range(num_files):
        start_index = i * rows_per_file
        end_index = start_index + rows_per_file if i < num_files - 1 else None
        file_path = os.path.join(raw_data_folder_path, f"data_{i+1}.csv")
        df.iloc[start_index:end_index].to_csv(file_path, index=False)
 

 
if __name__ == "__main__":
    df = pd.read_csv('../../../data/feature_dataset.csv')
    df_with_issues = introduce_data_issues(df)
    df_with_issues.to_csv('../../../data/Dataset_with_issues.csv', index=False)
    main_dataset_path = '../../../data/Dataset_with_issues.csv'
    raw_data_folder_path = '../../../airflow-spike/data/raw-data'
    num_files = 20
    split_dataset(main_dataset_path, raw_data_folder_path, num_files)
    #introduce_greater()