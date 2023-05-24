import requests
import numpy as np
import pandas as pd
from typing import List, Union, Dict
from calendar import monthrange




class KpExtraction:
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

        self.BASE_URL_KP_FINAL = 'https://kp.gfz-potsdam.de/app/json'
        self.__periods = self.__validate_periods(periods)
        self.__df = self.__extract_data()


    def __validate_periods(self, periods: List[str]):
        new_periods = list()
        for period in periods:
            # Exatamente YYYY 
            if len(period) == 4:
                [new_periods.append(f'{period}-0{i}' if i < 10 else f'{period}-{i}') for i in range(1, 13)]
            else:
                year, month = period.split('-')
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
        monthly_kps = list()
        for period in self.__periods:
            year, month = period.split('-')
            month_data_kp = self.__get_month_data(year=year, month=month)
            trusted_data = self.__clean_month_data_kp(raw_data=month_data_kp)
            current_df = self.__generate_df(trusted_data=trusted_data)
            monthly_kps.append(current_df)

        grouped_df = pd.concat(monthly_kps)
        grouped_df.reset_index(drop=True, inplace=True)
        return grouped_df
    

    def __get_month_data(self, year: str, month: str) -> Union[str, Exception]:
        """
        year [str]: YYYYY
        month [str]: DD
        """
        _, end_day = monthrange(year=int(year), month=int(month))
        url = f"{self.BASE_URL_KP_FINAL}/?start={year}-{month}-01T00%3A00%3A00Z&end={year}-{month}-{end_day}T23%3A59%3A59Z&index=Kp&status=def"
        try:
            resp = requests.get(url=url)
            ans = resp.json()
            return ans
        except Exception as error:
            raise error
    

    def __clean_month_data_kp(self, raw_data: Dict[str, str]) -> Dict:
        trusted_data = {
            'datetime': pd.to_datetime(raw_data['datetime']),
            'Kp': pd.to_numeric(raw_data['Kp'])
        }
        return trusted_data


    def __generate_df(self, trusted_data: Dict) -> pd.DataFrame:
        """
        """

        processed_data = list()
        for i in range(0, len(trusted_data['Kp']), 8):
            processed_data.append({
                'date': trusted_data['datetime'][i].date(),
                '0': trusted_data['Kp'][i+0],
                # '1': round((0.75*trusted_data['Kp'][i+0] + 0.25*trusted_data['Kp'][i+1])/2, 4), #
                # '2': round((0.25*trusted_data['Kp'][i+0] + 0.75*trusted_data['Kp'][i+1])/2, 4), #
                '3': trusted_data['Kp'][i+1],
                # '4': round((0.75*trusted_data['Kp'][i+1] + 0.25*trusted_data['Kp'][i+2])/2, 4), #
                # '5': round((0.25*trusted_data['Kp'][i+1] + 0.75*trusted_data['Kp'][i+2])/2, 4), #
                '6': trusted_data['Kp'][i+2],
                # '7': round((0.75*trusted_data['Kp'][i+2] + 0.25*trusted_data['Kp'][i+3])/2, 4), #
                # '8': round((0.25*trusted_data['Kp'][i+2] + 0.75*trusted_data['Kp'][i+3])/2, 4), #
                '9': trusted_data['Kp'][i+3],
                # '10': round((0.75*trusted_data['Kp'][i+3] + 0.25*trusted_data['Kp'][i+4])/2, 4), #
                # '11': round((0.25*trusted_data['Kp'][i+3] + 0.75*trusted_data['Kp'][i+4])/2, 4), #
                '12': trusted_data['Kp'][i+4],
                # '13': round((0.75*trusted_data['Kp'][i+4] + 0.25*trusted_data['Kp'][i+5])/2, 4), #
                # '14': round((0.25*trusted_data['Kp'][i+4] + 0.75*trusted_data['Kp'][i+5])/2, 4), #
                '15': trusted_data['Kp'][i+5],
                # '16': round((0.75*trusted_data['Kp'][i+5] + 0.25*trusted_data['Kp'][i+6])/2, 4), #
                # '17': round((0.25*trusted_data['Kp'][i+5] + 0.75*trusted_data['Kp'][i+6])/2, 4), #
                '18': trusted_data['Kp'][i+6],
                # '19': round((0.75*trusted_data['Kp'][i+6] + 0.25*trusted_data['Kp'][i+7])/2, 4), #
                # '20': round((0.25*trusted_data['Kp'][i+6] + 0.75*trusted_data['Kp'][i+7])/2, 4), #
                '21': trusted_data['Kp'][i+7],
                # '22': trusted_data['Kp'][i+7], # *
                # '23': trusted_data['Kp'][i+7], # *
            })
        return pd.DataFrame(processed_data)
