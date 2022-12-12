#!/usr/bin/python3
import argparse
import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


class WeatherProcessor:
    """Process Weather Data"""

    def __init__(self, station_dir, yield_dir):
        self.station_dir = station_dir
        self.yield_dir = yield_dir

    def read_all_stations(self):
        """Read all weather data in weather directory into dataframe"""
        station_list = [
            station
            for station in os.listdir(self.station_dir)
            if station[-4:] == ".txt"
        ]

        weather_list = []
        for filename in station_list:
            station_id = filename[:-4]
            filename = os.path.join(self.station_dir, filename)
            wd = self._read_weather(filename)
            wd["station_id"] = station_id
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
        """Reads Yield Data from yield_dir into pd dataframe"""
        yield_file = os.listdir(self.yield_dir)[0]
        yield_file = os.path.join(self.yield_dir, yield_file)
        yield_df = pd.read_csv(yield_file, sep="\t", names=["year", "corn_yield"])
        self.yield_df = yield_df

    def problem1(self):
        """Counts number of days with missing precipitation"""
        wd = self.weather_df.copy()
        wd["missing_precip"] = np.where(
            (wd.tmax.notna()) & (wd.tmin.notna()) & (wd.precip.isna()), 1, 0
        )
        summary_df = wd.groupby(["station_id"])["missing_precip"].sum().reset_index()
        summary_df.columns = ["file", "missing_precip"]
        summary_df.file = summary_df.file.apply(lambda x: x + ".txt")
        summary_df.sort_values(["file"], ascending=True, inplace=True)

        summary_df.to_csv(
            "./DataSciTest/answers/MissingPrcpData.out", sep="\t", index=False
        )

    def problem2(self):
        """Computes summary stats for each weather station"""
        wd = self.weather_df.copy()
        wd["year"] = wd.date.dt.year

        summary_df = (
            wd.groupby(["station_id", "year"])
            .agg(
                avg_tmax=("tmax", np.mean),
                avg_tmin=("tmin", np.mean),
                total_precip=("precip", np.sum),
            )
            .reset_index()
        )

        summary_df.rename(columns={"station_id": "filename"}, inplace=True)
        summary_df.filename = summary_df.filename.apply(lambda x: x + ".txt")

        summary_df.avg_tmax = np.round(summary_df.avg_tmax, 2)
        summary_df.avg_tmin = np.round(summary_df.avg_tmin, 2)
        summary_df.total_precip = summary_df.total_precip / 10  # mm to cm
        summary_df.total_precip = np.round(summary_df.total_precip, 2)

        summary_df.sort_values(["filename", "year"], ascending=True, inplace=True)
        summary_df.fillna(-9999, inplace=True)
        summary_df.to_csv(
            "./DataSciTest/answers/YearlyAverages.out", sep="\t", index=False
        )

    def problem3(self):
        """Count how many weather stations experience the most record setting year"""
        problem2_df = pd.read_csv("./DataSciTest/answers/YearlyAverages.out", sep="\t")
        records = (
            problem2_df.groupby(["filename"])
            .agg(
                record_avg_tmax=("avg_tmax", np.max),
                record_avg_tmin=("avg_tmin", np.max),
                record_total_precip=("total_precip", np.max),
            )
            .reset_index()
        )

        problem3_df = problem2_df.merge(records, how="left", on=["filename"])
        problem3_df["tmax_record_year"] = np.where(
            problem3_df.avg_tmax == problem3_df.record_avg_tmax, 1, 0
        )
        problem3_df["tmin_record_year"] = np.where(
            problem3_df.avg_tmin == problem3_df.record_avg_tmin, 1, 0
        )
        problem3_df["precip_record_year"] = np.where(
            problem3_df.avg_tmin == problem3_df.record_avg_tmin, 1, 0
        )

        histogram_df = (
            problem3_df.groupby(["year"])
            .agg(
                tmax_record_count=("tmax_record_year", np.sum),
                tmin_record_count=("tmin_record_year", np.sum),
                precip_record_count=("precip_record_year", np.sum),
            )
            .reset_index()
            .sort_values(["year"])
        )

        plot_df = pd.melt(histogram_df, id_vars=["year"], var_name="weather_type")
        g = sns.catplot(
            data=plot_df, x="year", y="value", hue="weather_type", kind="bar"
        )
        # g.set_xlabel("Year")
        # g.set_ylabel("Count of weather stations")
        # g.set_title("histogram of record breaking weather")
        plt.xticks(rotation=45)
        plt.savefig("./DataSciTest/answers/YearHistogram.png")

        histogram_df.to_csv(
            "./DataSciTest/answers/YearHistogram.out", sep="\t", index=False
        )


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
    wp.problem3()


if __name__ == "__main__":
    main()
