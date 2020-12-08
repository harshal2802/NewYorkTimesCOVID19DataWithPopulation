import pandas as pd

from .dataset import DataSet
from .exceptions import InputError

NEWYORK_TIMES_COVID19_DATA_REQUIRED_COLUMNS = ["fips", "date", "cases",
                                               "deaths", "county", "state"]


class NewYorkTimesCovid19Data(DataSet):
    """
    This class is for processing New York Times COVID-19 Data

    Attributes:
        csv_file_path: str, path/url for New York Times COVID-19 Data. 
            For example default path used is:
            "https://raw.githubusercontent.com/nytimes/covid-19-data/
            master/us-counties.csv"
            The csv file should have following columns:
            fips, date, county, state, cases, deaths
    """

    def __init__(self, csv_file_path: str):
        """
        The constructor for NewYorkTimesCovid19Data class

        Parameters:
        ----------
        csv_file_path: str, path/url for New York Times COVID-19 Data. 
            For example default path used is:
            "https://raw.githubusercontent.com/nytimes/covid-19-data/
            master/us-counties.csv"
        """
        # -> Using dtype as object to avoid data type format change due to
        # auto inferring the data type
        self.df = pd.read_csv(csv_file_path, dtype=object)
        for c in NEWYORK_TIMES_COVID19_DATA_REQUIRED_COLUMNS:
            if c not in self.df.columns:
                raise InputError(NewYorkTimesCovid19Data(csv_file_path),
                                 f"Column {c} of type string not found in source \
                        dataset {csv_file_path}")

    def update_df_with_geographic_exceptions(self, df: pd.DataFrame)\
            -> pd.DataFrame:
        """
        Function to update dataframe using Geographic Exceptions mentioned
        here: https://github.com/nytimes/covid-19-data#geographic-exceptions

        Parameters:
        ----------
        df: pd.Datafame object with New York Times COVID-19 Data

        Returns:
        df: pd.DataFrame object with updated with Geographic Exceptions logic

        """
        #### This is a function placeholder for future implementation ########
        # Geographic Exceptions cases:
        # 1. New York: All cases for the five boroughs of New York City
        # (New York, Kings, Queens, Bronx and Richmond counties) are
        # assigned to a single area called New York City. The number
        # of deaths in New York City also includes probable deaths
        # reported by the New York City health department. Deaths are
        # reported by county of residence, except for certain periods
        # described below:
        # :: county:New York City,state:New York :: 280 records
        # Timelines:
        # -> Beginning of outbreak to April 5: The New York State
        # Department of Health did not report the number of deaths
        # in each county. Our deaths numbers come from individual
        # county health departments and press releases and from the
        # N.Y.C. Health Department.
        # -> April 6 and 7: New York State began to report deaths
        # for each county. We recorded whichever number was more
        # current: either the state’s or the county’s. New York City
        # shows a large increase in deaths because the state health
        # department’s figures at that time were ahead of the city
        # health department’s.
        # -> April 8 to Aug. 5: Deaths are recorded for each county
        # based on place of death data from New York State.
        # -> April 17 and 18: The state did not report data on new
        # deaths in counties on April 17 or 18.
        # -> June 30: The New York City health department announced
        # the deaths of an additional 692 New York City residents,
        # most of which had taken place outside the city more than
        # three weeks earlier.
        # -> Beginning Aug. 6: Deaths are reported for each county
        # based on place of residence data from New York State.
        # 2. Kansas City, Mo.: Four counties (Cass, Clay, Jackson
        # and Platte) overlap the municipality of Kansas City, Mo.
        # The cases and deaths that we show for these four counties
        # are only for the portions exclusive of Kansas City. Cases
        # and deaths for Kansas City are reported as their own line.
        # :: county:Kansas City,state:Missouri   :: 261 records
        # 3. Joplin, Mo.: Starting June 25, cases and deaths for Joplin
        # are reported separately from Jasper and Newton counties.
        # The cases and deaths reported for those counties are only
        # for the portions exclusive of Joplin. Joplin cases and deaths
        # previously appeared in the counts for those counties or
        # as Unknown.:: county:Joplin,state:Missouri        :: 164 records
        # 4. Alameda County, Calif.: Counts for Alameda County include
        # cases and deaths from Berkeley and the Grand Princess
        # cruise ship. :: add population of Alameda County and Berkeley
        # county and we can neglect the population affected by cruise ship
        # for better stats
        # 5. Douglas County, Neb.: Counts for Douglas County include
        # cases brought to the state from the Diamond Princess
        # cruise ship. :: we can neglect the population affected
        # by cruise ship
        # 6. Guam: Counts for Guam include cases reported from
        # the USS Theodore Roosevelt. :: We can neglect population affected
        # by USS Theodore Roosevelt
        # 7. Puerto Rico: Data for Puerto Rico's county-equivalent
        # municipios are available starting on May 5. This data was
        # not available at the beginning of the outbreak and so all
        # cases and deaths were assigned to Unknown. Puerto Rico
        # does not report deaths at the municipio level.
        # :: deaths not populated
        # county:Unknown,state: <52 different state values>::6886records
        return df

    def preprocess_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to handle missing values in relavent columns ["fips",
        "date","cases","deaths"]
        Logic:
            fips: If the value for the "fips" column is missing 
            we can not create a join with the population data, 
            so this value is necessary. Hence we will drop all the 
            records related to the missing value of the "fips" column
            date: If the value for the "date" column is missing, 
            we can not find the unique group ( "fips" and "date" ) 
            for this record. Hence we will drop all the records related 
            to the missing value of the "date" column.
            cases: Remove all the records which do not have any/NaN values
            in cases field.
            deaths: Remove all the records which do not have any/NaN values
            in deaths field.
        Parameters:
        -----------
            df: pd.DataFrame object having all the required columns :: "fips",
                "date","cases","deaths"
        Returns:
        -------
            df: pd.DataFrame with the above logic applied for missing values

        """
        df = df.dropna(subset=["fips", "date", "cases", "deaths"])
        return df

    def preprocess_typecast_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to typecast the "date" as datetime, "cases" as integer and 
            "deaths" as integer

        This Function is required to convert the columns to the required 
        data type

        Parameters:
        ----------
        df: pd.DataFrame object having all the required 
            columns: 
                "fips":string
                "cases": string
                "deaths": string
                "date": string
        Return:
        ------
        df: pd.DataFrame object with updated data type for 
            columns:
                "fips":string
                "cases": integer
                "deaths": integer
                "date": datetime64[ns]
        """
        df = df.astype(dtype={"cases": int, "deaths": int,
                              "date": "datetime64[ns]"})
        return df

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to apply all the preprocessing procedure to New York 
        Covid19 data.

        This function is required to execute the preprocessing operations 
        in the required order

        Parameters:
        ----------
        df: pd.Datafame object with New York Times COVID-19 Data having
            columns:
                fips: string
                date: string
                county: string
                state: string
                cases: string
                deaths: string 
        Returns:
        -------
        df: pd.Datafame object with preprocessed New York Times COVID-19 
            Data having columns:
                fips: string
                date: datetime64[ns]
                county: string
                state: string
                cases: integer
                deaths: integer 
        """
        # Apply the geographic exceptions mentioned on the github repository
        df = self.update_df_with_geographic_exceptions(self.df)
        feature_list = ["fips", "date", "cases", "deaths"]
        df = self.feature_selection(df, feature_list)
        df = self.preprocess_missing_values(df)
        df = self.preprocess_typecast_columns(df)
        return df

    def combine_with_population_data(self, df_covid19: pd.DataFrame,
                                     df_population: pd.DataFrame) -> pd.DataFrame:
        """
        Function to combine covid19 data with population data using
        left join on "fips" column

        Explanation: We are applying the left join because we need 
        latest population estimate data from df_population. The combined 
        dataframe can later be used for generating the stats

        Parameter:
        ---------
        df_covid: pd.DataFrame object with processed New York Times 
            COVID-19 Data having columns:
                "fips": string
                "date": datetime64[ns]
                "cases": integer
                "deaths": integer

        df_population: pd.DataFrame object with Population Estimate 
            Data 2019 having columns:
                "fips": string
                "POPESTIMATE2019": integer
        Returns:
        -------
        df_combined: pd.DataFrame object with combined data having 
            columns:
                "fips": string
                "date": datetime64[ns]
                "cases": integer
                "deaths": integer
                "POPESTIMATE2019": integer
        """
        df_combined = df_covid19.merge(df_population, on="fips", how="left")
        feature_list = ["fips", "date", "cases", "deaths", "POPESTIMATE2019"]
        return self.feature_selection(df_combined, feature_list)

    def generate_stats_for_each_fips_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to generate statistics for filtered dataframe by fips code

        Explanation:
            cumulative_cases_to_date: cumulative sum of cases on combined 
                dataframe filtered by fips code and sorted by date value 
                from older newer
            cumulative_deaths_to_date: cumulative sum of deaths on combined
                dataframe filtered by fips code and sorted by date value 
                inorder of older to newer
            population: Updated population by taking into account
                the cumulative deaths to the date
        Parameters:
        ----------
        df: pd.DataFrame object with data filtered by fips code from 
            combined dataframe having following columns:
                "date": datetime64[ns]
                "cases": integer
                "deaths": integer
                "POPESTIMATE2019": integer
        Returns:
        ----------
        df: pd.DataFrame object with generated stats on the data filtered 
            by fips code from combined dataframe having following columns:
                "date": datetime64[ns]
                "population":integer
                "daily_cases":integer
                "daily_deaths":integer
                "cumulative_cases_to_date":integer
                "cumulative_deaths_to_date":integer
        """
        # Sort the dataframe for given fips code to generate cumulative sum
        # on daily cases and daily deaths count
        df = df.sort_values(by="date")
        df.index = df.date
        # calculate cumulative cases to date
        df["cumulative_cases_to_date"] = df["cases"].cumsum()
        # calculate cumulative deaths to date
        df["cumulative_deaths_to_date"] = df["deaths"].cumsum()
        # calculate updated population to date
        df["population"] = (df["POPESTIMATE2019"]
                            - df["cumulative_deaths_to_date"])
        # rename columns
        df.rename(columns={"cases": "daily_cases",
                           "deaths": "daily_deaths"}, inplace=True)
        # select features
        feature_list = ["population", "daily_cases",
                        "daily_deaths", "cumulative_cases_to_date",
                        "cumulative_deaths_to_date"]
        df = self.feature_selection(df, feature_list)
        return df

    def generate_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to generate statistics defined in 
        "generate_stats_for_each_fips_code" function for each
        fips code in combined dataframe

        Parameters:
        ----------
        df: pd.DataFrame object with combined data having 
            columns:
                "fips": string
                "date": datetime64[ns]
                "cases": integer
                "deaths": integer
                "POPESTIMATE2019": integer

        Returns:
        -------
        df: pd.DataFrame object with generated statistics on combined
            dataframe having following columns:
                "fips": string
                "date": datetime64[ns]
                "population":integer
                "daily_cases":integer
                "daily_deaths":integer
                "cumulative_cases_to_date":integer
                "cumulative_deaths_to_date":integer
        """
        df = df.groupby("fips").apply(lambda df_tmp:
                                      self.generate_stats_for_each_fips_code(df_tmp))
        feature_list = ["population", "daily_cases",
                        "daily_deaths", "cumulative_cases_to_date",
                        "cumulative_deaths_to_date"]
        df = self.feature_selection(df, feature_list)
        return df

    def sanity_check_prepared_data(self, df_prepared):
        """
        Function to test the prepared dataframe

        """
        print("Applying Sanity check on the output dataframe::")
        # 1. Check all the expected columns are available in the dataframe
        desired_columns = ["fips", "date", "population", "daily_cases",
                           "daily_deaths", "cumulative_cases_to_date",
                           "cumulative_deaths_to_date"]
        df_prepared = df_prepared.reset_index()
        for c in desired_columns:
            if c not in df_prepared.columns:
                raise Exception(
                    f"Desired Column{c} is not present in processed dataframe")

        # 2. Check if all the fips available in final dataframe as compare to original data.
        fips_in_original_data = self.df.fips.dropna().unique()
        print("Missing fips code as compare to original dataset",
              set(fips_in_original_data)-set(df_prepared.fips.unique()))
        # 3. Check for all the fips value the data is available except for the counties for which population
        #    data is missing, (69110', '69120', '78010', '78020', '78030')
        print("fips code related to null value of population",
              df_prepared[df_prepared.population.isna()].fips.unique())
        print("fips code related to null value of daily_cases",
              df_prepared[df_prepared.daily_cases.isna()].fips.unique())
        print("fips code related to null value of daily_deaths",
              df_prepared[df_prepared.daily_deaths.isna()].fips.unique())
        # 4. Check for all "fips" value the date column should be increasing order with the cumulative
        #    values(cumulative_cases_to_date, cumulative_deaths_to_date) are also in non-decreasing order
        df_fips_status = df_prepared.groupby("fips")\
            .apply(lambda df_tmp: (df_tmp.date.is_monotonic
                                   and df_tmp.cumulative_cases_to_date.is_monotonic
                                   and df_tmp.cumulative_deaths_to_date.is_monotonic))
        print("fips values for which columns:['date','cumulative_cases_to_date','cumulative_deaths_to_date'] is not monotonic",
              df_fips_status[df_fips_status == False].index)
