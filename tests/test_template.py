__version__ = ''
__author__ = 'Patryk Bajgot'

import pandas as pd
import os


class NameClass:

    test_csv = 'dummy.csv'
    test_big_csv = 'big_dummy.csv'
    correct_file = "data/job_data.csv"
    file_str_test = 'file'
    dataframe_str_test = 'dataframe'
    empty_df_str_test = 'empty_df'
    invalid_df_str_test = 'invalid_df'
    big_data_str_test = 'big_data'
    job_title_str = 'job_title'
    company_str = 'company'
    location_str = 'location'
    salary_str = 'salary'
    skills_str = 'skills'
    posted_date_str = 'posted_date'
    job_titles = ['Maintenance engineer', 'Gaffer', 'Nurse, learning disability', 'Programmer', 'Tester']
    companies = ['Akapulko SA', 'Griffer', 'Lockhead MLH', 'PRRT Sp. Z O.O.', 'Tiger GMBH']
    locations = ['New York', 'San Francisco', 'London', 'Gdansk', 'Berlin']
    correct_salary = ['$1000', '$2000', '3000', '$10 000', '$20.000']
    skills = ['Python, SQL', 'Java', 'Teamplay', 'C#, C++, JS', 'JS']
    posted_date = ['2022-01-01', '2023-01-01', '2022/01/01', '2023.01.01', '2023.05.29']


def temp_file_creator(data: pd.DataFrame):
    """
    Creates temp file from given dataframe
    :param data: pandas dataframe object
    :return: absolute path to new file
    """
    data.to_csv(NameClass.test_csv)
    return os.path.abspath(NameClass.test_csv)


def temp_file_destroyer(file_path: str):
    """
    Removes given file.
    :param file_path: absolute path to file
    :return: Nothing
    """
    os.remove(file_path)
