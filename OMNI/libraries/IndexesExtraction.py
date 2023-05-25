import os
import datetime
import numpy as np
import pandas as pd
from typing import List, Union, Dict
from calendar import monthrange


class IndexesExtraction:
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

        self.columns = [
            "DOY",
            "hour",
            "Dst_index",
            "Kp_index",
            "B_scalar",
            "Bz_GSM",
        ]

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
            cleaned_data = self.__clean_month_data(data_text=month_data)
            trusted_data = self.__set_data_type(raw_data=cleaned_data, year=year)
            trusted_daily_data = self.__set_daily_data(trusted_data=trusted_data)
            current_df = self.__generate_df(trusted_data=trusted_daily_data)
            monthly_dfs.append(current_df)
        
        grouped_df = pd.concat(monthly_dfs)
        grouped_df.reset_index(drop=True, inplace=True)
        return grouped_df
    

    def __get_month_data(self, year: str, month: str) -> Union[str, Exception]:
        """
        year [str]: YYYYY
        month [str]: DD

        vars=40 [Dst-index, nT]
        vars=38 [Kp*10 index]
        vars=8  [Scalar B, nT]
        vars=16 [BZ, nT (GSM)]
        """
        _, end_day = monthrange(year=int(year), month=int(month))
        url = f'curl -d  "activity=retrieve&res=hour&spacecraft=omni2&start_date={year}{month}01&end_date={year}{month}{end_day}&vars=40&vars=38&vars=8&vars=16" https://omniweb.gsfc.nasa.gov/cgi/nx1.cgi'
        try:
            resp = os.popen(url).read()
            raw_data = list()
            _ = [raw_data.append(i) for i in resp.split('\n') if i != '']
            return raw_data[10:-15]
        except Exception as error:
            raise error
    

    def __clean_month_data(self, data_text: List[str]):
        """
        columns
            example:
                [
                    "DOY",
                    "hour",
                    "Dst_index",
                    "Kp_index",
                    "B_scalar",
                    "Bz_GSM"
                ]
        """
        columns = {key: list() for key in self.columns}
        for line in data_text:
            line_values = [i for i in line.split(' ') if i != ''][1:]
            for i, key in enumerate(columns.keys()):
                columns[key].append(line_values[i])

        return columns
    

    def convert_doy_to_datetime(self, year: str, doy: str):
        return datetime.datetime(
            year=int(year), 
            month=1, 
            day=1, 
        ) + datetime.timedelta(int(doy) - 1)


    def __set_data_type(self, raw_data: Dict[str, list], year: str):
        columns = {
            'date': [self.convert_doy_to_datetime(year, doy) for doy in raw_data['DOY']],
            "hour": list(map(int, raw_data['hour'])),
            "Dst_index": list(map(float, raw_data["Dst_index"])),
            "Kp_index": list(map(float, raw_data["Kp_index"])),
            "B_scalar": list(map(float, raw_data["B_scalar"])),
            "Bz_GSM": list(map(float, raw_data["Bz_GSM"])),
        }
        return columns
    

    def __set_daily_data(self, trusted_data: Dict[str, list]):
        size_data = len(trusted_data['hour'])
        step_hour = 24
        columns = {
            'date': list(set(trusted_data['date'])),
            "Dst_index": [min(trusted_data["Dst_index"][i:i+step_hour]) for i in range(0, size_data, step_hour)],
            "Kp_index": [max(trusted_data["Kp_index"][i:i+step_hour]) for i in range(0, size_data, step_hour)],
            "B_scalar": [max(trusted_data["B_scalar"][i:i+step_hour]) for i in range(0, size_data, step_hour)],
            "Bz_GSM": [min(trusted_data["Bz_GSM"][i:i+step_hour]) for i in range(0, size_data, step_hour)],
        }
        return columns
    

    def __generate_df(self, trusted_data: Dict[str, list]):
        df = pd.DataFrame(trusted_data).sort_values(by=['date'])
        df.reset_index(drop=True, inplace=True)
        return df
    

    def make_classification(
            self, 
            classification_rules: Dict[str, List[int]], 
            classification_by_column: str,
            dropna: bool = True
        ) -> None:
        """
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
                if self.__df.loc[i, classification_by_column] in index_range:
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
