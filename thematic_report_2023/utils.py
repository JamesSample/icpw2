import numpy as np
import pandas as pd
from sqlalchemy import types


def read_data_template(file_path):
    """Read the ICPW template. An example of the template is here:
           ../data/icpw_input_template_chem_v0-3.xlsx
    Args
        file_path:  Raw str. Path to Excel template
        sheet_name: Str. Name of sheet to read
    Returns
        Dataframe.
    """
    df = pd.read_excel(
        file_path,
        sheet_name="Data",
        skiprows=1,
        header=[0, 1],
        parse_dates=[2],
        date_parser=lambda x: pd.to_datetime(x, format="%Y.%m.%d"),
    )
    df = merge_multi_header(df)

    return df


def merge_multi_header(df):
    """Merge the parameter and unit rows of the template into a single header.
    Args
        df: Raw dataframe read from the template
    Returns
        Dataframe with single, tidied header.
    """
    df.columns = [f"{i[0]}_{i[1]}" for i in df.columns]
    df.columns = [i.replace("_-", "") for i in df.columns]

    return df


def map_method_ids(df):
    """Change columns from template parameter names & units to RESA2 method IDs.

    Args
        df:  Dataframe. Read from template

    Returns
        Dataframe.
    """
    meth_map = {
        "Code": "code",
        "Date": "date",
        "pH": 10268,
        "Cond25_mS/m at 25C": 10260,
        "NH4-N_µgN/L": 10264,
        "Ca_mg/L": 10251,
        "Mg_mg/L": 10261,
        "Na_mg/L": 10263,
        "K_mg/L": 10258,
        "Alk_µeq/L": 10298,
        "SO4_mg/L": 10271,
        "NO3-N_µgN/L": 10265,
        "Cl_mg/L": 10253,
        "F_µg/L": 11121,
        "TOTP_µgP/L": 10275,
        "TOTN_µgN/L": 10274,
        "ORTP_µgP/L": 10279,
        "OKS_mgO/L": 10277,
        "SiO2_mgSiO2/L": 10270,
        "DOC_mgC/L": 10294,
        "TOC_mgC/L": 10273,
        "PERM_mgO/L": 10267,
        "TAl_µg/L": 10249,
        "RAl_µg/L": 10269,
        "ILAl_µg/L": 10257,
        "LAl_µg/L": 10292,
        "Fe_Total_µg/L": 10256,
        "Mn_Total_µg/L": 10262,
        "Cd_Total_µg/L": 10252,
        "Zn_Total_µg/L": 10276,
        "Cu_Total_µg/L": 10254,
        "Ni_Total_µg/L": 10281,
        "Pb_Total_µg/L": 10266,
        "Cr_Total_µg/L": 10285,
        "As_Total_µg/L": 10293,
        "Hg_Total_ng/L": 10921,
        "Fe_Filt_µg/L": 11122,
        "Mn_Filt_µg/L": 11123,
        "Cd_Filt_µg/L": 11124,
        "Zn_Filt_µg/L": 11125,
        "Cu_Filt_µg/L": 11126,
        "Ni_Filt_µg/L": 11127,
        "Pb_Filt_µg/L": 11128,
        "Cr_Filt_µg/L": 11129,
        "As_Filt_µg/L": 11130,
        "Hg_Filt_ng/L": 11131,
        "COLOUR_mgPt/L": 10278,
        "TURB_FTU": 10284,
        "TEMP_C": 10272,
        "RUNOFF_m3/s": 10288,
    }
    df.rename(meth_map, axis="columns", inplace=True)

    return df


def map_station_ids(df, eng):
    """Convert stations codes to RESA2 IDs.

    Args
        df:  Dataframe. Read from template
        eng: Obj. Active database connection object

    Returns
        Dataframe.
    """
    sql = "SELECT station_code AS code, station_id FROM resa2.stations"
    stn_df = pd.read_sql(sql, eng)
    df = pd.merge(df, stn_df, how="left", on="code")

    assert (
        pd.isna(df["station_id"]).sum() == 0
    ), "Some station codes cannot be matched to IDs."

    del df["code"]

    return df


def wide_to_long(df):
    """Convert from wide to long format.

    Args
        df:  Dataframe. Read from template

    Returns
        Dataframe.
    """
    df = pd.melt(df, id_vars=["station_id", "date"], var_name="method_id")
    df.dropna(subset="value", inplace=True)
    df["method_id"] = df["method_id"].astype(int)

    return df


def extract_lod_flags(df):
    """Extract LOD flags ('<' or '>') as a separate column and convert
    the value column to float.

    Args
        df:  Dataframe. Read from template

    Returns
        Dataframe.
    """

    def f(row):
        if "<" in str(row["value"]):
            val = "<"
        elif ">" in str(row["value"]):
            val = ">"
        else:
            val = np.nan
        return val

    df["flag1"] = df.apply(f, axis=1)
    df["value"] = (
        df["value"].astype(str).str.extract("([-+]?\d*\.\d+|\d+)", expand=True)
    )
    df["value"] = df["value"].astype(float)

    return df


def remove_duplicates(df, how="mean"):
    """Remove duplicated values for station_code-date combinations. Either averages
    or drops the duplicates (i.e. keeps only the first). Default is to average.

    Note that when averaging is performed, LOD flags are handled arbitrarily in cases
    where some duplicates are above the LOD and some are below.

    IMPORTANT: Duplicates should be assessed and corrected in the app first, if
    possible.

    Args
        df:  Dataframe. Read from template
        how: Str. Either 'mean' or 'drop'.

    Returns
        Dataframe.
    """
    assert how in ("mean", "drop"), "'how' must be either 'mean' or 'drop'."

    if how == "mean":
        df = (
            df.groupby(["station_id", "date", "method_id"])
            .aggregate({"value": "mean", "flag1": "first"})
            .reset_index()
        )
    else:
        # Drop
        df = df.drop_duplicates(
            subset=["station_id", "date", "method_id"], keep="first"
        )

    return df


def upload_samples(df, eng, dry_run=True):
    """Compiles a table of water samples to be added and uploads them to
    'resa2.water_samples'. All samples are assumed to be collected at the surface
    (i.e. depth1 = depth2 = 0 m).

    Args
        df:      Dataframe. Read from template
        eng:     Obj. Active database connection object
        dry_run: Bool. Default True. If True, performs most of the processing without
                 adding anything to the database.

    Returns
        Tuple of dataframes (ws_df, df). 'df' has RESA2 sample IDs appended and
        samples are added to the database.
    """
    ws_df = df[["station_id", "date"]].drop_duplicates().reset_index(drop=True)
    ws_df["depth1"] = 0
    ws_df["depth2"] = 0

    if not dry_run:
        ws_df.to_sql(
            name="water_samples",
            schema="resa2",
            con=eng,
            if_exists="append",
            index=False,
        )

    # Get the sample IDs back from the db
    stn_ids = ws_df["station_id"].unique()
    if len(stn_ids) == 1:
        sql = (
            'SELECT water_sample_id, station_id, sample_date AS "date" '
            "FROM resa2.water_samples "
            "WHERE station_id = %s" % stn_ids[0]
        )
    else:
        stn_ids = str(tuple(stn_ids))
        sql = (
            'SELECT water_sample_id, station_id, sample_date AS "date" '
            "FROM resa2.water_samples "
            "WHERE station_id IN %s" % stn_ids
        )
    ws_df = pd.read_sql_query(sql, eng)

    # Add IDs to df
    df = pd.merge(
        df,
        ws_df,
        how="left",
        on=["station_id", "date"],
    )

    ws_df = (
        df[["water_sample_id", "station_id", "date"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if not dry_run:
        assert pd.isna(df["water_sample_id"]).sum() == 0

    return (ws_df, df)


def upload_chemistry(df, eng, dry_run=True):
    """Upload chemistry data.

    Args
        df:      Dataframe. Read from template
        eng:     Obj. Active database connection object
        dry_run: Bool. Default True. If True, performs most of the processing without
                 adding anything to the database.

    Returns
        Dataframe. Data are uploaded to RESA2.
    """
    if not dry_run:
        assert pd.isna(df["method_id"]).sum() == 0
        assert pd.isna(df["water_sample_id"]).sum() == 0
        assert pd.isna(df["value"]).sum() == 0

    del df["station_id"], df["date"]

    # Improve performance by explicitly setting dtypes. See
    # https://stackoverflow.com/a/42769557/505698
    dtypes = {
        c: types.VARCHAR(df[c].str.len().max())
        for c in df.columns[df.dtypes == "object"].tolist()
    }

    if not dry_run:
        df.to_sql(
            name="water_chemistry_values2",
            schema="resa2",
            con=eng,
            if_exists="append",
            index=False,
            dtype=dtypes,
        )

    return df


def process_template(xl_path, eng, dups="mean", dry_run=True):
    """Main function for processing the ICPW data template.

    NOTE: Be sure to check the template using the ICPW QC app and fix any
    issues hightlighted before using this function.

    Args
        xl_path: Str. Path to template
        eng:     Obj. Active database connection object
        dups:    Str. How to handle duplicates. Either 'mean' or 'drop'. See docsring
                 of 'remove_duplicates' for details
        dry_run: Bool. Default True. If True, performs most of the processing without
                 adding anything to the database.

    Returns
        Tuple of dataframes (water_samples, chem_values).
    """
    df = read_data_template(xl_path)
    del df["Name"]
    df = map_method_ids(df)
    df = map_station_ids(df, eng)
    df = wide_to_long(df)
    df = extract_lod_flags(df)
    df = remove_duplicates(df, how=dups)
    ws_df, df = upload_samples(df, eng, dry_run=dry_run)
    df = upload_chemistry(df, eng, dry_run=dry_run)

    return (ws_df, df)
