import time
from get_data import get_data
import pandas as pd
from prefect import task, get_run_logger


@task(persist_result=False)
def fetch_albums_data(albums, driver):
    logger = get_run_logger()
    album_data_list = []
    reviews_list = []

    # Initialize empty DataFrames to avoid UnboundLocalError
    all_album_data = pd.DataFrame()
    all_reviews = pd.DataFrame()

    for index, album in enumerate(albums):
        try:
            reviews, album_data = get_data(album["link"], driver)

            # Add album_id to album data and reviews
            album_data["album_id"] = [index]
            for review in reviews:
                review["album_id"] = index

            # Append data to lists
            album_data_list.append(pd.DataFrame(album_data))
            reviews_list.append(pd.DataFrame(reviews))

        except Exception as e:
            logger.error(f"Error processing {album['link']}: {e}")

        finally:
            # Add a delay to avoid overwhelming the server
            time.sleep(3)

    # Concatenate all album data and reviews after the loop
    if album_data_list:
        all_album_data = pd.concat(album_data_list, ignore_index=True)
    if reviews_list:
        all_reviews = pd.concat(reviews_list, ignore_index=True)

    return all_album_data, all_reviews
