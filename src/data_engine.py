__version__ = ''
__author__ = 'Patryk Bajgot'

import pandas as pd
import re


class DataReader:

    skills_section_str = 'skills'
    location_section_str = 'location'
    job_title_section_str = 'job_title'
    company_section_str = 'company'
    posted_date_str = 'posted_date'

    __data = None

    def __init__(self, file: str = None, different_dataframe: pd.DataFrame = None):
        if file:
            cleaner = DataCleanerPandas(filename=file)
        elif different_dataframe is not None and not different_dataframe.empty:
            cleaner = DataCleanerPandas(different_dataframe=different_dataframe)
        elif different_dataframe.empty:
            raise Exception("Empty dataframe.")
        else:
            raise Exception('No selected data.')

        cleaner.cleanup_data()
        self.__data = cleaner.return_cleaned_data()

    # region Public methods

    def calculate_median(self):
        """
        Calculates salary median.
        :return: salary median
        """
        return self.__data['salary'].median()

    def prepare_ml_data(self):
        """
        Startup func. Calls methods to prepare data for ML
        :return: Nothing
        """
        data_section_names = [
            self.job_title_section_str,
            self.company_section_str,
            self.skills_section_str,
            self.location_section_str,
            self.posted_date_str
        ]
        for section in data_section_names:
            self.__data[section] = self.__data[section].str.lower().str.strip()
        self.__add_above_median_column()

    def return_ml_data(self, target_section, drop_section: list):
        """
        Returns data for ML func.
        :param target_section: target section
        :param drop_section: sections to drop
        :return: target and features
        """
        for section in drop_section:
            if section not in self.__data.columns:
                raise Exception(f'Column not found: {section}')
        features = self.__data.drop(columns=drop_section)
        target = self.__data[target_section]
        return features, target

    # endregion
    # region Private methods

    def __add_above_median_column(self):
        """
        Creates new column in data class variable.
        :return: Nothing
        """
        self.__data['above_median'] = (self.__data['salary'] > self.calculate_median()).astype(int)

    # endregion


class DataCleanerPandas:

    __loaded_data = None
    __correct_records = None
    __posted_date_str = 'posted_date'
    __salary_str = 'salary'

    def __init__(self, filename: str = None, different_dataframe: pd.DataFrame = None):
        if filename:
            try:
                self.__loaded_data = pd.read_csv(filename)
            except:
                raise Exception(f'Cannot find file in given path: {filename}')
        elif different_dataframe is not None and not different_dataframe.empty:
            self.__loaded_data = different_dataframe
        elif different_dataframe.empty:
            raise Exception('Empty dataframe.')
        else:
            raise Exception('No selected data.')

    # region Public methods

    def cleanup_data(self):
        """
        Startup func. Runs methods to clean all data in file.
        :return: Nothing
        """
        if self.__loaded_data is not None:
            self.__loaded_data = self.__loaded_data.dropna().reset_index(drop=True)
            for index in self.__loaded_data.index:
                if self.__cleanup_invalid_date(index):
                    continue
                if self.__cleanup_salary(index):
                    continue

    def return_cleaned_data(self):
        """
        Returns pandas obj wit loaded data.
        :return: pandas obj
        """
        return self.__loaded_data

    # endregion
    # region Private methods

    def __cleanup_invalid_date(self, index):
        """
        Checks and cleans invalid posted_date.
        :param index: Current index in dataframe
        :return: True if index was dropped
        """
        date_value = self.__loaded_data.loc[index, self.__posted_date_str]

        if pd.isna(pd.to_datetime(date_value, errors='coerce')):
            self.__loaded_data.drop(index, inplace=True)
            self.__loaded_data.reset_index(drop=True)
            return True
        else:
            self.__loaded_data.loc[index, self.__posted_date_str] = pd.to_datetime(date_value).strftime('%d-%m-%Y')
        return False

    def __cleanup_salary(self, index):
        """
        Checks and repair invalid salary.
        :param index: Current index in dataframe
        :return: True if index was dropped
        """
        temp_value = self.__loaded_data.loc[index, self.__salary_str]
        try:
            self.__loaded_data.loc[index, self.__salary_str] = pd.to_numeric(
                self.__loaded_data.loc[index, self.__salary_str]
            )
            if self.__loaded_data.loc[index, self.__salary_str] < 0:
                self.__loaded_data.drop(index, inplace=True)
                self.__loaded_data = self.__loaded_data.reset_index(drop=True)
                return True
            return False
        except:
            self.__loaded_data.loc[index, self.__salary_str] = pd.to_numeric(
                self.__replace_invalid_marks(temp_value)
            )
            return False

    @staticmethod
    def __replace_invalid_marks(data: str):
        """
        Collects all numbers from given data and converts them into string.
        :param data: string
        :return: str with only numbers
        """
        return ''.join(re.findall(r'\d', data))

    # endregion
