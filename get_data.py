import time
import pandas as pd
from bs4 import BeautifulSoup
from prefect import task


@task(retries=3, retry_delay_seconds=3, persist_result=False)
def get_data(
    album_link,
    driver,
):
    reviews = []
    album_data = []
    try:
        driver.get(album_link)
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, "html.parser")

        album_title = soup.find("h1", class_="albumTitle")
        album_title = album_title.get_text(strip=True) if album_title else None

        artist = soup.find("div", class_="artist")
        artist = artist.get_text(strip=True) if artist else None

        release_row = soup.find("div", class_="detailRow")
        release_date = release_row.get_text(strip=True) if release_row else None

        label_row = soup.find("div", class_="detailRow")
        label = None
        for row in soup.find_all("div", class_="detailRow"):
            if "Label" in row.get_text():
                label = row.get_text(strip=True)
                break

        genre_row = None
        for row in soup.find_all("div", class_="detailRow"):
            if "Genre" in row.get_text():
                genre_row = row
                break
        genres = []
        if genre_row:
            for a in genre_row.find_all("a"):
                genres.append(a.get_text(strip=True))

        cover_img = soup.find("div", class_="albumTopBox cover")
        cover_url = cover_img.find("img")["src"] if cover_img else None

        critic_score = soup.find("div", class_="albumCriticScore")
        critic_score = critic_score.get_text(strip=True) if critic_score else None

        user_score = soup.find("div", class_="albumUserScore")
        user_score = user_score.get_text(strip=True) if user_score else None

        album_data = pd.DataFrame(
            [
                {
                    "title": album_title,
                    "artist": artist,
                    "release_date": release_date,
                    "label": label,
                    "genres": ", ".join(genres),
                    "cover_url": cover_url,
                    "critic_score": critic_score,
                    "user_score": user_score,
                }
            ]
        )

        for review_row in soup.find_all("div", class_="albumReviewRow"):
            pub = review_row.find("div", class_="publication")
            pub = pub.get_text(strip=True) if pub else "AOTY"

            author = review_row.find("div", class_="author")
            if not author:
                user_div = review_row.find("div", class_="userReviewName")
                author = user_div.get_text(strip=True) if user_div else None
            else:
                author = author.get_text(strip=True)

            rating_block = review_row.find("div", class_="ratingBlock")
            score = review_row.find("div", class_="albumReviewRating")
            score = score.get_text(strip=True) if score else None

            rating = None
            if rating_block:
                rating = rating_block.find("div", class_="rating")
                rating = rating.get_text(strip=True) if rating else None
            else:
                rating = score

            text = review_row.find("div", class_="albumReviewText")
            text = text.get_text(strip=True) if text else None

            link = review_row.find("div", class_="albumReviewLinks")
            review_link = None
            if link:
                a = link.find("a", href=True)
                review_link = a["href"] if a else None

            reviews.append(
                {
                    "publication": pub,
                    "author": author,
                    "rating": rating,
                    "text": text,
                    "review_link": review_link,
                }
            )

        print(f"Processed album: {album_title}")

    except Exception as e:
        print(f"Error processing {album_link}: {e}")
    finally:
        time.sleep(2)
        return reviews, album_data
