import re
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup as soup
from typing import List, Union, Dict



class DstExtraction:
    def __init__(self, periods: List[str]) -> None:
        """
        periods List[str]
            A extração função com base no ano e mês.
            Exemplo:
                [
                    '2003-11',
                    '2002-07',
                    '2000-03',
                    '2007'      -> neste caso será extraído os dados do ano inteiro
                ]
        """

        self.BASE_URL_DST_FINAL = 'https://wdc.kugi.kyoto-u.ac.jp/dst_final/'
        self.__periods = self.__validate_periods(periods)
        self.__df = self.__extract_data()


    def __validate_periods(self, periods: List[str]):
        new_periods = list()
        for period in periods:
            # Exatamente YYYY 
            if len(period) == 4:
                [new_periods.append(f'{period}-0{i}' if i < 10 else f'{period}-{i}') for i in range(1, 13)]
            else:
                _, month = period.split('-')
                if 1 <= int(month) <= 12:
                    new_periods.append(period)
                else:
                    raise Exception('O valor para mês de extração deve ser entre 1 e 12.')
        return list(set(new_periods))


    @property
    def periods_extraction(self):
        return self.__periods


    @property
    def df(self):
        return self.__df


    def __extract_data(self):
        monthly_dfs = list()
        for period in self.__periods:
            year, month = period.split('-')
            month_data = self.__get_month_data(year=year, month=month)
            cleaned_data = self.__clean_month_data_text(data_text=month_data)
            raw_data = self.__clean_multiples_lines(lines=cleaned_data)
            current_df = self.__generate_df(raw_data=raw_data, year=year, month=month)
            monthly_dfs.append(current_df)
        
        grouped_df = pd.concat(monthly_dfs)
        grouped_df.reset_index(drop=True, inplace=True)
        return grouped_df
    
    
    def __get_month_data(self, year: str, month: str) -> Union[str, Exception]:
        """
        year [str]: YYYYY
        month [str]: DD
        """
        url = f"{self.BASE_URL_DST_FINAL}/{year}{month}/index.html"
        try:
            ans = requests.get(url=url)
            data = soup(ans.text, "html.parser")
            text_from_html = data.findAll("pre")[0].text
            return text_from_html
        except Exception as error:
            raise error


    def __clean_month_data_text(self, data_text: str) -> List[str]:
        """
        data_text [str]
        """
        raw_data = list()
        _ = [raw_data.append(i) for i in data_text.split('\n') if i != '']
        return raw_data[6:]


    def __clean_single_line(self, line: str) -> List[int]:
        """
        line [str]
        """
        # seleciona somente os valores de dst dentro da lista
        # quebra o texto em 3 blocos com 33 caracteres
        split_three_blocks = re.findall('.................................', line[2:])
        # remove o primeiro caracter de cada bloco
        clean_blocks = list()
        _ = [clean_blocks.append(i[1:]) for i in split_three_blocks]
        # separa os blocos em conjuntos de 4 caracteres
        # converte valores de string para inteiro
        separated_values = [int(i) for i in re.findall('....', ''.join(clean_blocks))]
        return separated_values


    def __clean_multiples_lines(self, lines: List[str]):
        lines_ok = list()
        _ = [lines_ok.append(self.__clean_single_line(i)) for i in lines]
        return lines_ok


    def __generate_df(self, raw_data: List[List[int]], year: str, month: str) -> pd.DataFrame:
        """
        """
        # Corrige data: 24h00 -> 00h00
        for i, day_indexex in enumerate(raw_data):
            raw_data[i].insert(0, day_indexex.pop())
    
        df = pd.DataFrame(raw_data, columns=list(range(0, 24)))
        df.index = df.index+1
        df['date'] = [pd.to_datetime(f"{year}-{month}-{day}", format='%Y-%m-%d') for day in df.index]
        df['dst_min'] = [df.loc[i, np.array(range(0, 24))].min() for i in range(1, len(df)+1)]
        return df
    

    def make_classification(self, classification_rules: Dict[str, List[int]], dropna: bool = True):
        """
        df [DataFrame]
        classification_rules [Dict[str, List[int]]]
            example:
                {
                    'fraca':            np.array(range(-31, -51, -1)),
                    'moderada':         np.array(range(-51, -101, -1)),
                    'intensa':          np.array(range(-101, -251, -1)),
                    'super_intensa':    np.array(range(-251, -1001, -1)),
                }
        """
        self.__df['classification'] = np.nan
        for i in range(len(self.__df)):
            for category, index_range in classification_rules.items(): 
                if self.__df.loc[i, 'dst_min'] in index_range:
                    self.__df.loc[i, 'classification'] = category
                    break
        
        self.__df = self.__df.dropna() if dropna else self.__df
    

    def remove_storms_by_date(self, dates: List[str]) -> None:
        """
        dates: list[str]
            format: YYYY-MM-DD
        """
        format_dates = [pd.to_datetime(date, format='%Y-%m-%d') for date in dates]
        boolean_mask = [date not in format_dates for date in self.df['date']]
        final_mask = pd.Series(boolean_mask, name='date', index=list(range(1, len(self.df)+1)))
        filtered_df = self.__df[final_mask]
        filtered_df.reset_index(drop=True, inplace=True)
        self.__df = filtered_df


    def plot_dst_graph(self):
        # alta resolução
        axis_x = [(i+1)/self.__df.columns.size for i in range(self.__df.columns.size*len(self.__df))]

        elements = [self.__df.iloc[i].to_list() for i in range(len(self.__df))]
        axis_y = list()
        for element in elements:
            axis_y += element

        plt.plot(axis_x, axis_y)

        # baixa resolução
        # df.min(axis=1).plot()
