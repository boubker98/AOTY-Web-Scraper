import pandas as pd
import asyncio
from get_links import get_links
from get_data import get_data
from utils import init_driver, get_connection
from fetch_albums import fetch_albums_data
from transform import transform_and_clean
from config import BASE_URL, HEADLESS
from prefect import flow, get_run_logger
import datetime
import click


@click.command()
@click.option("--year", default=2025, help="Year to scrape album data for.")
@click.option("--deploy", is_flag=True, help="Deploy the flow.")
def cli(year, deploy):
    if deploy:
        main(year).serve("main-deploy")
    else:
        main(year)


@flow(name="Main", persist_result=False)
def main(year):

    # * DONE: pull links asynchronously
    # * TODO: Set up logger properly and establish a logging history
    logger = get_run_logger()
    logger.info(f"Starting the scraping process for year: {year}")
    logger.info(f"Base URL: {BASE_URL}/{year}/")
    # Driver and DuckDB Initialisation
    driver = init_driver(headless=HEADLESS)
    con = get_connection("aoty.duckdb")

    # Data ingestion and transform
    links = asyncio.run(get_links(f"{BASE_URL}/{year}/"))
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
    cli()
