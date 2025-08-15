# Album Data Scraper
This project is a web scraping pipeline designed to extract album data and reviews from [Album of the Year](https://www.albumoftheyear.org). The pipeline uses Selenium for dynamic web scraping, BeautifulSoup for HTML parsing, and DuckDB for storing the extracted data.

---

## Features
- **Dynamic Web Scraping**: Uses Selenium to handle JavaScript-rendered pages.
- **Data Extraction**: Extracts album details (title, artist, release date, genres, etc.) and reviews (publication, author, rating, etc.).
- **Data Storage**: Saves the extracted data to DuckDB and CSV files.
- **Error Handling**: Implements retries and logging for robust scraping.
- **Modular Design**: The codebase is organized into reusable modules for better maintainability.

---
## Installation
### Prerequisites
- Python 3.8 or higher
- Google Chrome browser
- ChromeDriver (compatible with your Chrome version)
### Setup
1. Clone the repository:

```bash
git clone https://github.com/your-repo/album-data-scraper.git
cd album-data-scraper
```
2. Create a virtual environment and activate it:

```python
python3 -m venv .venv
source .venv/bin/activate
```
3. Install dependencies:
```python
pip install -r requirements.txt
```
> Ensure ChromeDriver is installed and added to your PATH.
### Usage
1. Update the configuration in config.py:
	- Set the `BASE_URL` to the desired starting page.
	- Adjust `PAGE_COUNT` to specify the number of pages to scrape.
2. Run the script:
```python
python main.py
```

Output:
- Album data will be saved to data/all_album_data.csv.
- Reviews will be saved to data/all_reviews.csv.
-  Both datasets will also be stored in the aoty.duckdb database.