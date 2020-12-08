import argparse
from .newyork_times_covid19_data import NewYorkTimesCovid19Data
from .population_estimate_data_2019 import PopulationEstimateData2019


def main():
    """
    Function to execute all the required steps in a sequence to generate
    desired output data file
    """
    # 1. Get the command line arguments
    parser = argparse.ArgumentParser(
        description="Prepare Covid19 cases summary with population for New York Times COVID-19 Data")

    parser.add_argument('--covid19_csv_path',
                        type=str,
                        default="https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv",
                        help="URL/Path for latest data from New York Times COVID-19 Data")

    parser.add_argument('--population_csv_path', type=str,
                        default="https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv",
                        help="URL/Path for 2019 Population Estimate Data")

    parser.add_argument('--output_file_path',
                        type=str,
                        default="./aggregated_covid19_data_with_population.csv",
                        help="Path of output csv file")

    parser.add_argument('--apply_sanity_check_on_output_data',
                        type=bool,
                        default=False,
                        help="Path of output csv file")

    args = parser.parse_args()
    print(f"Reading New York Times COVID-19 Data from {args.covid19_csv_path}")
    print(
        f"Reading 2019 Population Estimate Data from {args.population_csv_path}")
    print(f"Output file path for processed file is: {args.output_file_path}")
    # -------------------------------------------------------------------------
    # 2. Get and preprocess Raw data
    # Get the newyork times covid 19 data
    newyork_times_covid19_data = NewYorkTimesCovid19Data(
        args.covid19_csv_path)
    # preprocess newyork times covid19 data
    df_covid19 = newyork_times_covid19_data.preprocess(
        newyork_times_covid19_data.df)

    # Get  Population estimate 2019 data
    population_estimate_data_2019 = PopulationEstimateData2019(
        args.population_csv_path)
    # preprocess Population estimate data 2019
    df_population = population_estimate_data_2019.preprocess(
        population_estimate_data_2019.df)
    print("Completed: Get and preprocessed Raw data")
    # -------------------------------------------------------------------------
    # 3. Combine covid19 data and population data using left join
    # Combine the df_covid19 with df_population dataframes using fips column
    df_combined = newyork_times_covid19_data.combine_with_population_data(
        df_covid19, df_population)
    print("Completed: Combine covid19 data and population data using left join")
    # -------------------------------------------------------------------------
    # 4. Generate statistics for combined dataframe
    # generate stats on combined dataframe
    df_out = newyork_times_covid19_data.generate_stats(df_combined)
    print("Completed: Generate statistics for combined dataframe")
    # -------------------------------------------------------------------------
    # 5. save generated dataframe to out path
    newyork_times_covid19_data.save_dataframe_as_csv(df_out,
                                                     args.output_file_path)
    print(
        f"Completed: save generated dataframe to out path: {args.output_file_path}")
    if args.apply_sanity_check_on_output_data:
        newyork_times_covid19_data.sanity_check_prepared_data(df_out)


if __name__ == '__main__':
    main()
