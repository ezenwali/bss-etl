import pandas as pd


class BikeSharingData:

    col_rename = {
        "Departure": "date_dep",
        "Return": "date_ret",
        "Bike": "bike_id",
        "Departure station": "dep_station_label",
        "Return station": "ret_station_label",
        "Membership type": "formula_label",
        "Memebership type": "formula_label",
        "Membership Type": "formula_label",
        "Formula": "formula_label",
        "Covered distance (m)": "covered_distance",
        "Duration (sec.)": "duration",
        "Electric bike": "is_ebike",
    }

    def __init__(self, data_path):

        self.filename = data_path
        self.data = self.read_and_process_file()

    def read_and_process_file(self):
        # Get the file extension (e.g., csv, xls)
        filename = self.filename
        ext = filename.split(".")[-1][:3]

        if ext == "csv":
            df = pd.read_csv(filename)
        elif ext == "xls":
            df = pd.read_excel(filename)
        else:
            print(f"Unsupported file type: {filename}")
            return pd.DataFrame()  # Return an empty DataFrame for unsupported files

        if "Electric bike" not in df.columns:
            df["Electric bike"] = pd.NA

        # Rename columns
        df.rename(columns=self.col_rename, inplace=True)
        # print(df.columns) # Return
        df["bike_id"] = pd.to_numeric(df["bike_id"], errors="coerce").astype("Int64")
        df["is_ebike"] = df["is_ebike"].astype("boolean")

        return df[list(set(list(self.col_rename.values())))]

    def __check_date_format(self, row):
        correct_format = "%Y-%m-%d %H:%M:%S"
        try:
            pd.to_datetime(row["date_dep"], format=correct_format)
            pd.to_datetime(row["date_ret"], format=correct_format)
            return False
        except ValueError:
            return True

    # TODO: complete date formatting
    def __clean_data(self):
        """
        Clean the bike-sharing data by removing duplicates and missing values,
        and ensuring correct data types.
        """

        # Apply the function to the 'date_dep' column and filter rows that don't follow the format
        invalid_rows = self.data[
            self.data.apply(lambda row: self.__check_date_format(row), axis=1)
        ]
        self.data.head()
        self.data.drop(invalid_rows.index, inplace=True)
        self.data["date_dep"] = pd.to_datetime(
            self.data["date_dep"], format="%Y-%m-%d %H:%M:%S"
        )
        self.data["date_ret"] = pd.to_datetime(
            self.data["date_ret"], format="%Y-%m-%d %H:%M:%S"
        )
