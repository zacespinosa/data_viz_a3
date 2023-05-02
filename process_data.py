import xarray as xr
import pandas as pd 

def downsample(df):
    # downsample to monthly mean skipping nans
    df = df.set_index('time')
    df = df.resample('M').mean()
    # Apply MonthBegin offset to the index
    df.index = df.index.map(pd.offsets.MonthBegin(1).rollback)

    return df

def load_raw_cfc(filename, cfc_type):
    # read text file into pandas DataFrame and
    df = pd.read_csv(filename, delimiter=r'\s+', header=67)
    # reformat to extract time and global mean
    df = pd.DataFrame({
        'time':pd.to_datetime(df[f'HATS_{cfc_type}_YYYY'].astype(str) + df[f'HATS_{cfc_type}_MM'].astype(str), format='%Y%m'),
        f'global_mean_{cfc_type}': df[f'HATS_Global_{cfc_type}'],
        f'SH_mean_{cfc_type}': df[f'HATS_SH_{cfc_type}'],
        f'NH_mean_{cfc_type}': df[f'HATS_SH_{cfc_type}'],
    })
    return downsample(df)

def load_raw_ozone(filename):
    df = pd.read_csv(filename, delimiter=r'\s+', header=1)
    # convert utc date to datetime
    time = pd.to_datetime(df['UTC_Date'], format='%Y/%m/%d')
    df = pd.DataFrame({
        'time': time,
        'Total_Ozone': df[f'Total_Ozone']
    })
    return downsample(df)

def load_raw_hfc(filename):
    df = pd.read_csv(filename, delimiter=r'\s+', header=49)
    # convert utc date to datetime
    years = df['HCFC142bcatsSPOyr']
    months = df['HCFC142bcatsSPOmon']
    # Combine years and months into a list of strings with format "YYYY-MM"
    time = [f"{y}-{m:02d}" for y, m in zip(years, months)]
    # Convert date strings to pandas datetimes
    time = pd.to_datetime(time) 

    df = pd.DataFrame({
        'time': time,
        'HFC_142b': df[f'HCFC142bcatsSPOm']
    })
    return downsample(df)

def load_raw_hfc32(filename):
    df = pd.read_csv(filename, delimiter=r'\s+', header=24)
    # convert utc date to datetime
    time = pd.to_datetime(df['yyyymmdd'], format='%Y%m%d')
    df = pd.DataFrame({
        'time': time,
        'HFC-32_C': df[f'HFC-32_C']
    })
    return downsample(df)


def main():
    cfc_11 = load_raw_cfc("data/CFC-11-global.txt", cfc_type='F11')
    cfc_12 = load_raw_cfc("data/CFC-12-global.txt", cfc_type='F12')
    o3_spo = load_raw_ozone("data/O3-dobson_toSPO.txt")
    hcfc_142 = load_raw_hfc("data/HCFC-142-South-Pole.txt")
    hfc32 = load_raw_hfc32("data/HFC-32_M2&PR1_MS_flask.txt")

    ds = xr.merge([
        cfc_11.to_xarray(), 
        cfc_12.to_xarray(), 
        o3_spo.to_xarray(), 
        hcfc_142.to_xarray(), 
        hfc32.to_xarray()
    ])
    ds.to_netcdf("data/processed_data.nc")


if __name__ == '__main__':
    main()

