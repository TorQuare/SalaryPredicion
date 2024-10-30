__version__ = ''
__author__ = 'Patryk Bajgot'

import pandas as pd
import pytest
from tests.test_template import NameClass as Nc


@pytest.fixture(scope='function')
def json_data():
    return {
            "job_title": "Data Scientist",
            "company": "XYZ Corp",
            "location": "New York",
            "salary": "120000",
            "skills": "Python, Machine Learning, Data Science",
            "posted_date": "2023-08-15"
        }


@pytest.fixture(scope='function')
def sample_big_data(sample_data):
    return pd.DataFrame(
        {
            Nc.job_title_str: expand_data(Nc.job_titles),
            Nc.company_str: expand_data(Nc.companies),
            Nc.location_str: expand_data(Nc.locations),
            Nc.salary_str: expand_data(Nc.correct_salary),
            Nc.skills_str: expand_data(Nc.skills),
            Nc.posted_date_str: expand_data(Nc.posted_date)
        }
    )


@pytest.fixture(scope='function')
def sample_big_data_suffix(sample_data):
    return pd.DataFrame(
        {
            Nc.job_title_str: expand_data(Nc.job_titles, suffix="_no_"),
            Nc.company_str: expand_data(Nc.companies, suffix="_no_"),
            Nc.location_str: expand_data(Nc.locations),
            Nc.salary_str: expand_data(Nc.correct_salary),
            Nc.skills_str: expand_data(Nc.skills),
            Nc.posted_date_str: expand_data(Nc.posted_date)
        }
    )


@pytest.fixture(scope='function')
def sample_data():
    return pd.DataFrame(
        {
            'job_title': ['Maintenance engineer', 'Gaffer', 'Nurse, learning disability'],
            'company': ['Akapulko SA', 'Griffer', 'Lockhead MLH'],
            'location': ['New York', 'San Francisco', 'London'],
            'salary': ['$1000', '$2000', '3000'],
            'skills': ['Python, SQL', 'Java', ''],
            'posted_date': ['2022-01-01', '2023-01-01', 'not a date']
        }
    )


@pytest.fixture(scope='function')
def empty_data():
    return pd.DataFrame({})


@pytest.fixture(scope='function')
def invalid_data():
    return pd.DataFrame(
        {
            'job_title': ['Maintenance engineer', 'Gaffer', 'Nurse, learning disability', 'Programmer'],
            'company': ['Akapulko SA', 'Griffer', 'Lockhead MLH', 'PRRT Sp. Z O.O.'],
            'location': ['New York', 'San Francisco', 'London', 'Gdansk'],
            'salary': ['$10 000', '$20.000', '30,000', '-2000'],
            'skills': ['Python, SQL', 'Java', '', 'C#'],
            'posted_date': ['2022/01/01', '2023.01.01', 'last year', 'NaN']
        }
    )


def expand_data(data, iterations: int = 10, suffix: str = None):
    """
    Expands given data x iterations
    :param data:
    :param iterations:
    :param suffix:
    :return:
    """
    if suffix is not None:
        temp_data = []
        for iteration in range(iterations):
            for value in data:
                temp_data.append(value+suffix+str(iteration))
        return temp_data
    return data*iterations
