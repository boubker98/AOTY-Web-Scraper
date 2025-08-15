import pandas as pd
from prefect import task

months = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


@task(persist_result=False)
def transform_and_clean(messy_albums):
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(messy_albums)

    # Clean title and artist
    df["title"] = df["title"].str.strip().str.title()
    df["artist"] = df["artist"].str.strip().str.title()

    # Clean and parse release_date
    df["release_date"] = (
        df["release_date"]
        .astype(str)  # ensure all values are strings
        .str.replace(r"/\s*Release Date", "", regex=True)  # remove release text
        .str.replace("\u00a0", " ", regex=False)  # non-breaking spaces → regular
        .str.replace(
            r"([a-zA-Z]+)(\d{1,2})", r"\1 \2", regex=True
        )  # add missing space after month
        .str.replace(r",(\d{4})", r", \1", regex=True)  # ensure comma-space before year
        .str.strip()
    )
    # Use flexible parsing to avoid NaT for messy formats
    df["release_date"] = pd.to_datetime(
        df["release_date"], format="%B %d, %Y", errors="coerce"
    )
    # Clean label (case-insensitive)
    df["label"] = (
        df["label"].str.replace(r"/\s*Label", "", regex=True, case=False).str.strip()
    )

    # Clean genres (handle NaN safely)
    df["genres"] = df["genres"].apply(
        lambda x: (
            [g.strip() for g in str(x).split(",") if g.strip()] if pd.notna(x) else []
        )
    )

    return df
