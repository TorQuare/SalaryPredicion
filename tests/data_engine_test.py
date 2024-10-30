__version__ = ''
__author__ = 'Patryk Bajgot'

import pytest
from src.data_engine import DataReader, DataCleanerPandas
from tests.test_template import NameClass as Nc
from tests.test_template import temp_file_destroyer, temp_file_creator


@pytest.mark.parametrize(
    "loader_type, data_source, expected",
    [
        (Nc.file_str_test, Nc.test_csv, [1000, 2000, 3000]),
        (Nc.dataframe_str_test, None, [1000, 2000, 3000]),
        (Nc.empty_df_str_test, None, None),
        (Nc.invalid_df_str_test, None, [10000, 20000, 30000]),
        (Nc.big_data_str_test, None, [1000, 2000, 3000, 10000, 20000] * 10)
    ]
)
def test_cleanup_salary(sample_data, empty_data, invalid_data, sample_big_data, loader_type, data_source, expected):
    """
    Checks correctness of cleanup salary func.
    :param sample_data, empty_data, invalid_data, sample_big_data: fixture
    :param loader_type: describes how data is loaded to tested func
    :param data_source: selects file to load data
    :param expected: expected values
    :return: Nothing
    """
    if loader_type == Nc.invalid_df_str_test:
        data_file = invalid_data
    elif loader_type == Nc.big_data_str_test:
        data_file = sample_big_data
    elif loader_type == Nc.file_str_test:
        file_path = temp_file_creator(sample_data)
        data_file = sample_data
    else:
        data_file = sample_data

    if loader_type == Nc.empty_df_str_test:
        with pytest.raises(Exception, match='Empty dataframe'):
            DataCleanerPandas(different_dataframe=empty_data)
    else:
        cleaner = prepare_class_object(
            sample_data=sample_data,
            invalid_data=invalid_data,
            sample_big_data=sample_big_data,
            cleaner_class_bool=True,
            loader_type=loader_type,
            data_source=data_source
        )
        if loader_type == Nc.file_str_test:
            cleaner._DataCleanerPandas__loaded_data = sample_data
            temp_file_destroyer(file_path)

        for item in range(len(data_file[Nc.salary_str])):
            cleaner._DataCleanerPandas__cleanup_salary(item)

        assert (cleaner._DataCleanerPandas__loaded_data[Nc.salary_str] == expected).all()


@pytest.mark.parametrize(
    "loader_type, data_source, expected_shape",
    [
        (Nc.file_str_test, Nc.test_csv, 2),
        (Nc.dataframe_str_test, None, 2),
        (Nc.empty_df_str_test, None, None),
        (Nc.invalid_df_str_test, None, 2),
        (Nc.big_data_str_test, None, 50)
    ]
)
def test_cleanup_invalid_date(
        sample_data,
        empty_data,
        invalid_data,
        sample_big_data,
        loader_type,
        data_source,
        expected_shape
):
    """
    Checks correctness of cleanup posting date func.
    :param sample_data, empty_data, invalid_data, sample_big_data: fixture
    :param loader_type: describes how data is loaded to tested func
    :param data_source: selects file to load data
    :param expected_shape: expected value
    :return: Nothing
    """
    if loader_type == Nc.invalid_df_str_test:
        data_file = invalid_data
    elif loader_type == Nc.big_data_str_test:
        data_file = sample_big_data
    elif loader_type == Nc.file_str_test:
        file_path = temp_file_creator(sample_data)
        data_file = sample_data
    else:
        data_file = sample_data

    if loader_type == Nc.empty_df_str_test:
        with pytest.raises(Exception, match='Empty dataframe'):
            DataCleanerPandas(different_dataframe=empty_data)
    else:
        cleaner = prepare_class_object(
            sample_data=sample_data,
            invalid_data=invalid_data,
            sample_big_data=sample_big_data,
            cleaner_class_bool=True,
            loader_type=loader_type,
            data_source=data_source
        )
        if loader_type == Nc.file_str_test:
            cleaner._DataCleanerPandas__loaded_data = sample_data
            temp_file_destroyer(file_path)

        for item in range(len(data_file[Nc.posted_date_str])):
            cleaner._DataCleanerPandas__cleanup_invalid_date(item)

        assert cleaner._DataCleanerPandas__loaded_data.shape[0] == expected_shape


@pytest.mark.parametrize(
    "loader_type, data_source, expected_values, expected_shape",
    [
        (Nc.file_str_test, Nc.test_csv, [1000, 2000], 2),
        (Nc.dataframe_str_test, None, [1000, 2000], 2),
        (Nc.empty_df_str_test, None, None, None),
        (Nc.invalid_df_str_test, None, [10000, 20000], 2),
        (Nc.big_data_str_test, None, [1000, 2000, 3000, 10000, 20000] * 10, 50)
    ]
)
def test_cleanup_data(
        sample_data,
        empty_data,
        invalid_data,
        sample_big_data,
        loader_type,
        data_source,
        expected_values,
        expected_shape
):
    """
    Checks correctness of cleanup func.
    :param sample_data, empty_data, invalid_data, sample_big_data: fixture
    :param loader_type: describes how data is loaded to tested func
    :param data_source: selects file to load data
    :param expected_values: expected values
    :param expected_shape: expected shape
    :return: Nothing
    """
    if loader_type == Nc.file_str_test:
        file_path = temp_file_creator(sample_data)

    if loader_type == Nc.empty_df_str_test:
        with pytest.raises(Exception, match='Empty dataframe'):
            DataCleanerPandas(different_dataframe=empty_data)
    else:
        cleaner = prepare_class_object(
            sample_data=sample_data,
            invalid_data=invalid_data,
            sample_big_data=sample_big_data,
            cleaner_class_bool=True,
            loader_type=loader_type,
            data_source=data_source
        )
        if loader_type == Nc.file_str_test:
            cleaner._DataCleanerPandas__loaded_data = sample_data
            temp_file_destroyer(file_path)

        cleaner.cleanup_data()

        assert cleaner._DataCleanerPandas__loaded_data.shape[0] == expected_shape
        assert (cleaner._DataCleanerPandas__loaded_data[Nc.salary_str] == expected_values).all()


@pytest.mark.parametrize(
    "loader_type, data_source, expected",
    [
        (Nc.file_str_test, Nc.test_csv, 1500),
        (Nc.dataframe_str_test, None, 1500),
        (Nc.empty_df_str_test, None, None),
        (Nc.invalid_df_str_test, None, 15000),
        (Nc.big_data_str_test, None, 3000)
    ]
)
def test_calculate_median(sample_data, empty_data, invalid_data, sample_big_data, loader_type, data_source, expected):
    """
    Checks correctness of calculating median func.
    :param sample_data, empty_data, invalid_data, sample_big_data: fixture
    :param loader_type: describes how data is loaded to tested func
    :param data_source: selects file to load data
    :param expected: expected values
    :return: Nothing
    """
    if loader_type == Nc.file_str_test:
        file_path = temp_file_creator(sample_data)

    if loader_type == Nc.empty_df_str_test:
        with pytest.raises(Exception, match='Empty dataframe'):
            DataReader(different_dataframe=empty_data)
    else:
        reader = prepare_class_object(
            sample_data=sample_data,
            invalid_data=invalid_data,
            sample_big_data=sample_big_data,
            cleaner_class_bool=False,
            loader_type=loader_type,
            data_source=data_source
        )

        median_salary = reader.calculate_median()
        assert median_salary == expected

    if loader_type == Nc.file_str_test:
        temp_file_destroyer(file_path)


@pytest.mark.parametrize(
    "loader_type, data_source",
    [
        (Nc.file_str_test, Nc.test_csv),
        (Nc.dataframe_str_test, None),
        (Nc.empty_df_str_test, None),
        (Nc.invalid_df_str_test, None),
        (Nc.big_data_str_test, None)
    ]
)
def test_prepare_ml_data(
        sample_data,
        empty_data,
        invalid_data,
        sample_big_data,
        loader_type,
        data_source
):
    """
    Checks correctness of prepare ML data func.
    :param sample_data, empty_data, invalid_data, sample_big_data: fixture
    :param loader_type: describes how data is loaded to tested func
    :param data_source: selects file to load data
    :return: Nothing
    """
    if loader_type == Nc.file_str_test:
        file_path = temp_file_creator(sample_data)

    if loader_type == Nc.empty_df_str_test:
        with pytest.raises(Exception, match='Empty dataframe'):
            DataReader(different_dataframe=empty_data)
    else:
        reader = prepare_class_object(
            sample_data=sample_data,
            invalid_data=invalid_data,
            sample_big_data=sample_big_data,
            cleaner_class_bool=False,
            loader_type=loader_type,
            data_source=data_source
        )
        reader.prepare_ml_data()

        columns = reader._DataReader__data.columns

        assert Nc.skills_str in columns
        assert Nc.location_str in columns
        assert Nc.job_title_str in columns
        assert Nc.company_str in columns
        assert Nc.posted_date_str in columns
        assert 'above_median' in columns

    if loader_type == Nc.file_str_test:
        temp_file_destroyer(file_path)


def prepare_class_object(
        sample_data,
        invalid_data,
        sample_big_data,
        cleaner_class_bool: bool,
        loader_type,
        data_source
):
    """
    Prepares classes from DataEngine to tests.
    :param sample_data: fixture
    :param invalid_data: fixture
    :param sample_big_data: fixture
    :param cleaner_class_bool: selects which object from DataEngine should be created
    :param loader_type: selects which data should be used
    :param data_source: specific data to load
    :return: class object
    """
    data_file = None

    if loader_type == Nc.invalid_df_str_test:
        data_file = invalid_data
    elif loader_type == Nc.dataframe_str_test:
        data_file = sample_data
    elif loader_type == Nc.file_str_test:
        data_file = data_source
    elif loader_type == Nc.big_data_str_test:
        data_file = sample_big_data

    if cleaner_class_bool:
        if loader_type == Nc.file_str_test:
            return DataCleanerPandas(filename=data_file)
        else:
            return DataCleanerPandas(different_dataframe=data_file)
    else:
        if loader_type == Nc.file_str_test:
            return DataReader(file=data_file)
        else:
            return DataReader(different_dataframe=data_file)
