#!/usr/bin/python3
import argparse
import numpy as np
import os
import pandas as pd


class WeatherProcessor:
    """Process Weather Data"""

    def __init__(self, station_dir, yield_dir):
        self.station_dir = station_dir
        self.yield_dir = yield_dir

    def read_all_stations(self):
        """ Read all weather data in weather directory into dataframe """
        station_list = [station for station in os.listdir(self.station_dir) if station[-4:] == ".txt"]

        weather_list = []
        for filename in station_list:
            station_id = filename[:-4]
            filename = os.path.join(self.station_dir, filename)
            wd = self._read_weather(filename)
            wd['station_id'] = station_id
            weather_list.append(wd)

        weather_df = pd.concat(weather_list)

        self.weather_df = weather_df

    def _read_weather(self, filename):
        """Read weather data from single file into pd dataframe"""
        df = pd.read_csv(
            filename,
            sep="\t",
            na_values=[-9999],
            names=["date", "tmax", "tmin", "precip"],
        )
        df.date = pd.to_datetime(df.date, format="%Y%m%d")
        df.tmax = df.tmax / 10
        df.tmin = df.tmin / 10
        df.precip = df.precip / 10

        return df

    def read_yield(self):
        """ Reads Yield Data from yield_dir into pd dataframe """
        yield_file = os.listdir(self.yield_dir)[0]
        yield_file = os.path.join(self.yield_dir, yield_file)
        yield_df = pd.read_csv(yield_file, sep="\t", names=["year", "corn_yield"])
        self.yield_df = yield_df

    def problem1(self):
        """ Counts number of days with missing precipitation """
        wd = self.weather_df.copy()
        wd['missing_precip'] = np.where(
            (wd.tmax.notna()) & (wd.tmin.notna()) & (wd.precip.isna()),
            1,
            0
        )
        summary_df = wd.groupby(['station_id'])['missing_precip'].sum().reset_index()
        summary_df.columns = ['file', 'missing_precip']
        summary_df.file = summary_df.file.apply(lambda x: x + ".txt")
        summary_df.sort_values(['file'], ascending=True, inplace=True)

        summary_df.to_csv("./DataSciTest/answers/MissingPrcpData.out", sep="\t", index=False)

    def problem2(self):
        """ Computes summary stats for each weather station """
        wd = self.weather_df.copy()
        wd['year'] = wd.date.dt.year
        print(wd)

        summary_df = wd.groupby(['station_id', 'year']).agg(
            avg_max = ('tmax', np.mean),
            avg_min = ('tmin', np.mean),
            total_precip = ('precip', np.sum)
        ).reset_index()

        summary_df.rename(columns={'station_id': 'filename'}, inplace=True)
        summary_df.filename = summary_df.filename.apply(lambda x: x + ".txt")

        summary_df.avg_max = np.round(summary_df.avg_max, 2)
        summary_df.avg_min = np.round(summary_df.avg_min, 2)
        summary_df.total_precip = summary_df.total_precip / 10 # mm to cm
        summary_df.total_precip = np.round(summary_df.total_precip, 2)

        summary_df.sort_values(['filename', 'year'], ascending=True, inplace=True)
        summary_df.fillna(-9999, inplace=True)
        summary_df.to_csv('./DataSciTest/answers/YearlyAverages.out', sep='\t', index=False)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("station_dir", help="directory with weather station files")
    parser.add_argument("yield_dir", help="directory with yield data files")
    parser.parse_args()
    args = parser.parse_args()

    wp = WeatherProcessor(station_dir=args.station_dir, yield_dir=args.yield_dir)
    print(args.station_dir)
    print(args.yield_dir)
    wp.read_all_stations()
    wp.read_yield()

    print(wp.weather_df.head())
    print(wp.yield_df.head())
    wp.problem1()
    wp.problem2()


if __name__ == "__main__":
    main()
