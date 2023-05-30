import pandas as pd


def save_processed_data(df, folder_name: str, name: str):
    """
    folder_name: [KYOTO, OMNI, INPE]
    name:
    """
    processed_data = pd.HDFStore(path=f'{folder_name}/processed_data/{name}.h5')
    processed_data.append('df', df)
    processed_data.close()
    print(f'Seus dados foram salvos em: processed_data/{name}.h5\n')


def load_processed_data(folder_name:str, name: str):
    """
    folder_name: [KYOTO, OMNI, INPE]
    name:
    """
    processed_data = pd.HDFStore(path=f'{folder_name}/processed_data/{name}.h5')
    df = processed_data['df']
    processed_data.close()
    return df
