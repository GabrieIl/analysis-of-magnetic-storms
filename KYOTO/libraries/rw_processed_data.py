import pandas as pd


def save_processed_data(df, name: str):
    processed_data = pd.HDFStore(path=f'processed_data/{name}.h5')
    processed_data.append('df', df)
    processed_data.close()
    print(f'Seus dados foram salvos em: processed_data/{name}.h5\n')


def load_processed_data(df, name: str):
    processed_data = pd.HDFStore(path=f'processed_data/{name}.h5')
    df = processed_data['df']
    processed_data.close()
    return df
