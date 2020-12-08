import pandas as pd


class DataSet(object):
    """
    This class is to define common functions for dataset processing

    Attributes:
        csv_file_path: str, path/url for csv file
    """

    def __init__(self):
        """
        The constructor for DataSet class
        """
        pass

    def feature_selection(self, df: pd.DataFrame, feature_list: list) -> \
            pd.DataFrame:
        """
        Function to return dataframe with given list of features

        Parameters:
        ----------
        df: pd.Datafame object common columns with feature_list
        feature_list: List of columns to select from provided dataframe

        Returns:
        df: pd.DataFrame object with columns as provided feature_list

        """
        return df[feature_list]

    def save_dataframe_as_csv(self, df: pd.DataFrame, csv_file_path: str):
        """
        Function to save dataframe df as csv file in local file system

        Parameters:
        ----------
        df: pd.DataFrame object with data
        path: path of the output csv file
        """
        df.to_csv(csv_file_path)
