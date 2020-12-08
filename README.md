# NewYorkTimesCOVID19DataWithPopulation

<!-- TABLE OF CONTENTS -->

## Table of Contents

- [About the Project](#about-the-project)
  - [Built With](#built-with)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
- [Overview](#overview)
  - [Data Description](#data-description)
  - [Preprocess Population Estimate Data 2019](#preprocess-population-estimate-data-2019)
  - [Preprocess New York Times COVID-19 Data](#preprocess-new-york-times-covid-19-data)
  - [Create Combined View](#create-combined-view)
  - [Generate Statistics on combined view](#generate-statistics-on-combined-view)
- [Learnings or thoughts on data source](#learnings-or-thoughts-on-data-source)
- [Testing the output](#testing-the-output)
- [Things I would add given more time](#things-you-would-add-given-more-time)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)

<!-- ABOUT THE PROJECT -->

## About The Project

As part of COVID19 response, the Data Science team needs to prepare
daily/weekly updates of nationwide infection counts, organized by county. We use
numeric FIPS code https://en.wikipedia.org/wiki/FIPS_county_code rather than
stand and county name to serve our results

For every FIPS code and date, your end user will get: population, daily
cases, daily deaths, cumulative cases to date, and cumulative death counts to
date.

<!-- Built With -->

### Built With

This project mainly utilize below mentioned python libraries.

- [Python 3.5.2+](https://www.python.org/downloads/release/python-352/)
- [Pandas](https://pandas.pydata.org/)

<!-- GETTING STARTED -->

## Getting Started

To get a local copy up and running follow these simple example steps.

### Prerequisites

This project requires knowledge of Python 3.6, and pandas

### Installation

Installing for bash:

1. Clone this repo

```sh
$ git clone https://github.com/harshal2802/NewYorkTimesCOVID19DataWithPopulation.git
```

2. Change directory to above cloned repo

```sh
$ cd NewYorkTimesCOVID19DataWithPopulation
```

3. Install dependencies from requirements.txt

```sh
$ pip3 install -r requirements.txt
```

Installing for Docker:

1. Clone this repo

```sh
git clone https://github.com/harshal2802/NewYorkTimesCOVID19DataWithPopulation.git
```

2. Change directory to above cloned repo

```sh
cd NewYorkTimesCOVID19DataWithPopulation
```

3. Build docker container:

```
docker build -t covid19_data_with_population .
```

<!-- USAGE EXAMPLES -->

## Usage

To generate summary data file for given Chicago bird collision dataset please use below command

Running From bash::

```sh
#make sure that you are inside project("NewYorkTimesCOVID19DataWithPopulation") directory
$ python3 -m covid19_data_with_population \
  --covid19_csv_path https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv \
  --population_csv_path https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv \
  --output_file_path aggregated_covid19_data_with_population.csv
```

Running From Docker::

```sh
#make sure that you are inside project("NewYorkTimesCOVID19DataWithPopulation") directory
$ docker run -p 8080:8080 -v $PWD:/shared covid19_data_with_population \
  -m covid19_data_with_population \
  --covid19_csv_path https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv \
  --population_csv_path https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv \
  --output_file_path /shared/aggregated_covid19_data_with_population.csv
```

<!-- Overview -->

## Overview

Problem Statement is to generate the statistics on combined view of
New York Times Covid-19 data and Population Estimate Data 2019.

For every FIPS code and date on combined data, end user will get: population,
daily cases, daily deaths, cumulative cases to date, and cumulative death
counts to date.

Final Data should have following columns:

- fips: string, 5 digit code <br>
- date: string, standard date format <br>
- population: integer, updated population estimate <br>
- daily_cases: integer, daily covid-19 cases <br>
- daily_deaths: integer, daily covid-19 deaths <br>
- cumulative_cases_to_date: integer, cumulative cases to date <br>
- cumulative_deaths_to_date: integer, cumulative deaths to date <br>

<!-- Data Description -->

### Data Description

Given dataset contains mainly 2 csv file sources

1. [Population Estimate Data 2019](https://www.census.gov/data/datasets/time-series/demo/popest/2010s-counties-total.html)
   [download](https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv)
   should have following columns:

   - STATE: string, 2 digit code <br>
   - COUNTY: string, 3 digit code <br>
   - POPESTIMATE2019:integer, estimated population of 2019 <br>

2. [New York Times COVID-19 Data](https://github.com/nytimes/covid-19-data/blob/master/README.md)
   [download](https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv):
   This Data is aggregated from all the US counties on a daily basis by the New York Times
   should have following columns:
   - fips: string, 5 digit code <br>
   - date: string, standard date format <br>
   - county: string, county name <br>
   - state: string, state name <br>
   - cases: integer, number of daily cases <br>
   - deaths: integer, number of daily deaths <br>

<!-- Preprocess Population Estimate Data 2019 -->

### Preprocess Population Estimate Data 2019

To use Population estimate data 2019 in our pipeline, we need to preprocess
the raw data. We have defined all the required functions used in preprocessing
in the class named: [PopulationEstimateData2019](covid19_data_with_population/population_estimate_data_2019.py)

Here is explanation of major functions from PopulationEstimateData2019 class

1. generate_fips_code: Function to create fips column by combining "STATE"
   and "COUNTY" columns. <br>
2. typecast_columns: Function to type cast column to required format as
   follows: <br>

- POPESTIMATE2019: integer <br>
- fips: string <br>

<!-- Preprocess New York Times COVID-19 Data -->

### Preprocess New York Times COVID-19 Data

To use New York Times COVID-19 Data in our pipeline, we need to preprocess
the raw data. We have defined all the required functions used in preprocessing
in the class named: [NewYorkTimesCovid19Data](covid19_data_with_population/newyork_times_covid19_data.py)

Here is explanation of major preprocessing functions from
PopulationEstimateData2019 class

1. preprocess_missing_values: Function to handle missing values in relavent
   columns ["fips","date","cases","deaths"] <br>
   Logic:<br>
   - fips: If the value for the "fips" column is missing
     we can not create a join with the population data,
     so this value is necessary. Hence we will drop all the
     records related to the missing value of the "fips" column
   - date: If the value for the "date" column is missing,
     we can not find the unique group ( "fips" and "date" )
     for this record. Hence we will drop all the records related
     to the missing value of the "date" column.
   - cases: Remove all the records which do not have any/NaN values
     in cases field.
   - deaths: Remove all the records which do not have any/NaN values
     in deaths field. <br>
2. preprocess_typecast_columns: Function to typecast "date" as datetime,
   "cases" as integer and "deaths" as integer<br>
3. preprocess: Function to apply all the preprocessing procedure to
   New York Covid19 data. This function is required to execute the
   preprocessing operations in the required order

<!-- Create Combined View -->

### Create Combined View

To generate the combined view of preprocessed New York Times COVID-19 Data
and Population Estimate Data 2019. We will apply left join because we need
to extend the preprocessed New York Times COVID-19 Data to get population
estimate value from Population Estimate Data 2019 using "fips" as a joining
key. The combined dataframe can later be used for generating the statistics.
We will use "combine_with_population_data" function from
[NewYorkTimesCovid19Data](covid19_data_with_population/newyork_times_covid19_data.py)
class to generate combined view.

Data after applying "combine_with_population_data" function will look like:

- fips: string
- date: datetime64[ns]
- cases: integer
- deaths: integer
- POPESTIMATE2019: integer

<!-- Generate Statistics on combined view -->

### Generate Statistics on combined view

To generate the required statistics at the "fips" level, we first need to
sort the records by "date" in increasing order for each "fips" value available
in the data frame. Once the data is sorted at the "fips" level by the "date"
value. we will apply the following operations to generate
the required columns:

- cumulative_cases_to_date: cumulative sum on "cases" at the "fips" level
- cumulative_deaths_to_date: cumulative sum on "deaths at the "fips" level
- population: subtract the "cumulative_deaths_to_date" value from "POPESTIMATE2019" value to get more accurate population.
- daily_cases: rename the "cases" column as "daily_cases"
- daily_deaths: rename the "deaths" column as "daily_deaths"

Final dataframe looks like:

- fips: string
- date: datetime64[ns]
- population:integer
- daily_cases:integer
- daily_deaths:integer
- cumulative_cases_to_date:integer
- cumulative_deaths_to_date:integer

<!-- Learnings or thoughts on data source -->

## Learnings or thoughts on data source

In real world most of the datasource are not perfect and the provided data
i.e. New York Times COVID-19 Data and Population Estimate Data 2019 is also
has no exception:
Here are few observations about New York Times COVID-19 Data:

- There are 7620 records which does not have "fips" value populated.
  Most of these record belongs to county values: Joplin(164 records),
  Kansas City( 261 records),New York City( 280 records), Unknown( 6886 records )
  - The reason behind these missing fips code is changes made in reporting of these cases
    by different authorities. <br>
    For simplicity we are neglecting these records for the time being
- For Puerto Rico state: There are 16811 records for which deaths has not
  been recorded. We are removing these records as well for the calculation simplicity.
- There are Geographic Exceptions defined for the New York Times COVID-19 Dataset,
  which explains some of the missing records. We have defined a placeholder function
  "update_df_with_geographic_exceptions" as a part of NewYorkTimesCovid19Data class to
  include Geographic Exceptions logic in future.
- Population data For fips code 69110(Saipan, MP), 69120(Aguijan, MP and Tinian, MP),
  78010(St. Croix, VI), 78020( St. John, VI ), 78030( St. Thomas, VI ) is missing.
  For the time being we will report these value of population as Null.
  We can improve this data by obtaining the population of these missing fips code from
  official sources.

<!-- Testing the output -->

## Testing the output

Here are few steps to test the sanity of the processed dataframe:
check the "sanity_check_prepared_data" function in NewYorkTimesCovid19Data class

- Check all the expected columns are available in the dataframe
- Check if all the fips available in final dataframe as compare to original data.
- Check for all the fips value the data is available except for the counties
  for which population data is missing, (69110', '69120', '78010', '78020', '78030')
- Check for all "fips" value the date column should be increasing order with
  the cumulative values(cumulative_cases_to_date, cumulative_deaths_to_date)
  are also in non-decreasing order

<!-- Things I would add given more time -->

## Things I would add given more time

Given more time I would like to improve the accuracy and completness in dataset
by:

- implementing the logic to include Geographic exceptions for the New York Times
  COVID-19 Data
- getting the population data for the counties for which population is missing

<!-- LICENSE -->

## License

Distributed under the MIT License. See `LICENSE` for more information.

<!-- CONTACT -->

## Contact

Harshal Chourasiya - [Linkedin](https://www.linkedin.com/in/harshal-chourasiya-39bb0426/), [Github](https://github.com/harshal2802)

[Project Link](https://github.com/harshal2802/NewYorkTimesCOVID19DataWithPopulation)

<!-- ACKNOWLEDGEMENTS -->

## Acknowledgements

- [FIPS_county_code wikipedia](https://en.wikipedia.org/wiki/FIPS_county_code)
- [New York Times COVID-19 Data](https://github.com/nytimes/covid-19-data)
- [County Population Totals: 2010-2019](https://www.census.gov/data/datasets/time-series/demo/popest/2010s-counties-total.html)
- [Population Estimate Data 2019](https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/co-est2019-alldata.pdf)
