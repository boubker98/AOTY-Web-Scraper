# Album Data Scraper
This project is a web scraping pipeline designed to extract album data and reviews from [Album of the Year](https://www.albumoftheyear.org). The pipeline uses Selenium for dynamic web scraping, BeautifulSoup for HTML parsing, and DuckDB for storing the extracted data, click for CLI argument handling and Prefect for end-to-end orchestration.

---

## Features
- **Dynamic Web Scraping**: Handles JavaScript-rendered pages using Selenium.
- **Comprehensive Data Extraction**: Captures album details (title, artist, release date, genres, etc.) and reviews (publication, author, rating, etc.).
- **Flexible Data Storage**: Saves results in DuckDB and CSV formats.
- **Robust Error Handling**: Retries, logging, and graceful failure recovery.
- **Modular Codebase**: Organized into reusable, maintainable modules.
- **Pipeline Orchestration**: Automate end-to-end workflows by running a local Prefect server and scheduling flows.


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
	- Set the `year` to the desired starting page.
2. Run the script:
```python
python main.py --year `year you want to scrape albums for`
```

Output:
- Album data will be saved to data/all_album_data.csv.
- Reviews will be saved to data/all_reviews.csv.
-  Both datasets will also be stored in the aoty.duckdb database.