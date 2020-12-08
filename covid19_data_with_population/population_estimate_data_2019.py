import pandas as pd

from .dataset import DataSet
from .exceptions import InputError

POPULATION_ESTIMATE_DATA_REQUIRED_COLUMNS = ["STATE", "COUNTY",
                                             "POPESTIMATE2019"]


class PopulationEstimateData2019(DataSet):
    """
    This class is for processing Population Estimate Data 2019 

    Attributes:
        csv_file_path: str, path/url for Population Estimate Data 2019 
            For example default path used is:
            "https://www2.census.gov/programs-surveys/popest/datasets/
            2010-2019/counties/totals/co-est2019-alldata.csv"
            The csv file should have atleast following columns:
            "STATE", "COUNTY","STNAME","CTYNAME","POPESTIMATE2019"
    """

    def __init__(self, csv_file_path):
        """
        The constructor for PopulationEstimateData2019 class

        Parameters:
        ----------
        csv_file_path: str, path/url for Population Estimate Data 2019. 
            For example default path used is:
            "https://www2.census.gov/programs-surveys/popest/datasets/
            2010-2019/counties/totals/co-est2019-alldata.csv"
            The csv file should have atleast following columns:
            "STATE", "COUNTY","STNAME","CTYNAME","POPESTIMATE2019"
        """
        # -> Using dtype as object to avoid data type format change due to
        # auto inferring the data type
        # -> Using encoding as "ISO-8859-1" because of source file is
        # present in "ISO-8859-1" encoding
        self.df = pd.read_csv(csv_file_path,
                              dtype=object,
                              encoding="ISO-8859-1")
        for c in POPULATION_ESTIMATE_DATA_REQUIRED_COLUMNS:
            if c not in self.df.columns:
                raise InputError(PopulationEstimateData2019(csv_file_path),
                                 f"Column {c} of type string not found in source \
                        dataset {csv_file_path}")

    def generate_fips_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to create fips column by combining "STATE" and "COUNTY" 
        columns

        Parameters:
        ----------
        df: pd.DataFrame object having Population Estimate Data 2019 with 
            columns:
                "STATE":string
                "COUNTY":string
                "POPESTIMATE2019":string

        Returns:
        df: pd.DataFrame object having Population Estimate Data 2019 with 
            an extra column having fips code
                "STATE":string
                "COUNTY":string
                "POPESTIMATE2019":string
                "fips": string
        """
        df["fips"] = df["STATE"].str.strip() + df["COUNTY"].str.strip()
        return df

    def typecast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to typecast columns for Population Estimate Data 2019

        Parameters:
        ----------
        df: pd.DataFrame object having Population Estimate Data 2019 with 
            columns:
                "STATE":string
                "COUNTY":string
                "POPESTIMATE2019":string
                "fips":string

        Returns:
        -------
        df: pd.DataFrame object having Population Estimate Data 2019 with 
            columns:
                "STATE":string
                "COUNTY":string
                "POPESTIMATE2019":integer
                "fips":string
        """
        df = df.astype(dtype={"POPESTIMATE2019": "int"})
        return df

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to preprocess the Population Estimate Data 2019

        Explanation:
            generate fips code: combine "STATE" and "COUNTY" columns
                to generate fips code
            typecast POPESTIMATE2019: typecast "POPESTIMATE2019" as integer

        Parameters:
        ----------
        df: pd.DataFrame object having Population Estimate Data 2019 with 
            columns:
                "STATE":string
                "COUNTY":string
                "POPESTIMATE2019":string

        Returns:
        -------
        df: pd.DataFrame object having Population Estimate Data 2019 with 
            columns:
                "STATE":string
                "COUNTY":string
                "POPESTIMATE2019":integer
                "fips":string
        """
        df = self.generate_fips_code(df)
        df = self.typecast_columns(df)
        return df
