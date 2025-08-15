import pandas as pd
import asyncio
from get_links import get_links
from get_data import get_data
from utils import init_driver, get_connection, setup_logger
from fetch_albums import fetch_albums_data
from transform import transform_and_clean
from config import BASE_URL, PAGE_COUNT, HEADLESS
from prefect import flow


@flow(name="Main", persist_result=False)
def main():
    # * DONE: pull links asynchronously
    # TODO: Set up logger properly and establish a logging history
    logger = setup_logger()

    # Driver and DuckDB Initialisation
    driver = init_driver(headless=HEADLESS)
    con = get_connection("aoty.duckdb")

    # Data ingestion and transform
    links = asyncio.run(get_links(BASE_URL, 1))
    album_data_list, reviews_list = fetch_albums_data(links, driver)
    cleaned_albums_data = transform_and_clean(pd.DataFrame(album_data_list))

    # TODO: Make the table saving a bit more verbose, designate the schema and be more specific
    con.execute("CREATE OR REPLACE TABLE albums AS SELECT * FROM cleaned_albums_data")
    con.execute("CREATE OR REPLACE TABLE reviews AS SELECT * FROM reviews_list")

    # Saving data locally to check
    cleaned_albums_data.to_csv("all_album_data.csv", index=False)
    reviews_list.to_csv("all_reviews_data.csv", index=False)

    # Wrapping up
    driver.quit()
    con.close()


if __name__ == "__main__":
    main()
