import dask.dataframe as dd
import pandas as pd
from dask import delayed
import util.mars_time as mt

from data_path_handler import L1BDataPathHandler, L22dDataPathHandler
from reader import MCSL1BReader, MCSL22dReader


class MCSL1BLoader(L1BDataPathHandler, MCSL1BReader):
    """
    Class to load L1B data (multiple files) in different ways.
    Requires path handler to generate filenames in different.
    """

    def __init__(self, mcs_data_path):
        super().__init__(mcs_data_path)

    def load(self, files, dask=False):
        if type(files) != list:
            return self.read(files)
        elif len(files) == 0:
            df = pd.DataFrame(columns=self.columns)
        else:
            if not dask:
                df = pd.concat([self.read(f) for f in sorted(files)])
            else:
                dfs = [delayed(self.read)(f) for f in sorted(files)]
                df = dd.from_delayed(dfs)
        return df

    def load_files_around_date(self, date, n=1, **kwargs):
        files, _ = self.find_files_around_date(date, n)
        return self.load(files, *kwargs)

    def load_files_around_file(self, f, n=1, **kwargs):
        files, _ = self.find_files_around_file(f, n)
        return self.load(files, *kwargs)

class MCSL22dLoader(L22dDataPathHandler, MCSL22dReader):
    def __init__(self, mcs_data_path):
        super().__init__(mcs_data_path)
    
    def load_single(self, filename, ddr):
        try:
            data = self.read(filename, ddr=ddr)
        except FileNotFoundError:
            data = self.make_empty_df()
        self.data[ddr] = data
        return data

    def reduce_to_profiles(self, data, profiles):
        return pd.merge(data, profiles, on=["Prof#", "filename"])

    def load(self, files, ddr, profiles=[]):
        if type(files) != list:
            data = self.load_single(files, ddr)
            if len(profiles)== 0:
                return data
            else:
                self.reduce_to_profiles([data], profiles)
        elif len(files) == 0:
            return self.make_empty_df(ddr)
        else:
            dfs = [self.read(f) for f in files]
            if len(profiles)>0:
                dfs = [self.reduce_to_profiles(df) for df in dfs]
            return pd.concat(dfs)

    def load_date_range(self, start_time, end_time, ddr="DDR1", profiles=[]):
        print(f"Loading L2 {ddr} data from {start_time} - {end_time}")
        files = self.DPH.find_files_from_daterange(start_time, end_time)[0]
        data = self.load_L2(files, ddr, profiles=profiles)
        return data

    def load_ls_range(
        self, my: int, start_ls: float, end_ls: float, **kwargs
    ) -> pd.DataFrame:
        """
        Load L2 data within Ls range of a given Mars Year

        Parameters
        ----------
        my: Mars Year
        start_ls/end_ls: beginning/end of Ls range

        Returns
        -------
        _: loaded L2 data
        """
        print(
            f"Determining approximate start/end dates for MY{my}, "
            f"Ls range: {start_ls} - {end_ls}"
        )
        date_start = mt.MY_Ls_to_UTC(my, start_ls)
        date_end = mt.MY_Ls_to_UTC(my, end_ls)
        return self.load_date_range(date_start, date_end, **kwargs)
