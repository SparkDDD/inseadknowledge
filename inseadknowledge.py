import os
import cloudscraper # Import cloudscraper
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urljoin, urlparse
import logging

# --- Configuration ---
# It's CRITICAL to load sensitive information like API keys from environment variables
# when running on platforms like GitHub Actions, not hardcode them.
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tbl7VYAVYoO9ySh0u"

# Airtable Field IDs (good practice to use these if they are stable)
FIELD_CATEGORY = "fldAHCnq8IqIaRbHD"
FIELD_TITLE = "fldlUm4FqOpdD2RCj"
FIELD_PUBLICATION_DATE = "fldNiEGA9hBHpW4ah"
FIELD_AUTHOR = "fldw2dqoOXGxkkmgQ"
FIELD_SUMMARY = "fldK0gBQFV5DPgQn9"
FIELD_ARTICLE_URL = "fld6Uhrx1CzOWEZZT"
FIELD_IMAGE_URL = "fldnGyY3zX7aDhG6d"

BASE_URL = "https://knowledge.insead.edu"

# Logging setup
logging.basicConfig(
    filename='insead_scrape.log',
    filemode='w',
    level=logging.INFO, # Set to INFO for less verbose logs unless debugging
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Initialize cloudscraper once globally
scraper = cloudscraper.create_scraper()

# --- Helper Functions ---

def normalize_url(url):
    """Normalizes a URL by parsing and reconstructing it without query parameters or fragments."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_publication_date(article_url):
    """
    Visits an individual article page to extract the publication date.
    Uses cloudscraper to handle potential Cloudflare protection on article pages.
    """
    try:
        logging.debug(f"Visiting article page to extract date: {article_url}")
        # Use scraper for individual article pages too
        res = scraper.get(article_url, timeout=15)
        res.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        
        soup = BeautifulSoup(res.content, "html.parser")
        date_tag = soup.select_one("a.link.link--date") # Selector for the date tag

        if date_tag:
            return date_tag.get_text(strip=True)
        
        logging.warning(f"Publication date not found for {article_url}")
        return None
    except Exception as e:
        logging.error(f"Error extracting date from {article_url}: {e}")
        # Optionally, save the error page for debugging if a date isn't found due to a scrape issue
        # with open(f"error_date_page_{urlparse(article_url).hostname}.html", "w", encoding="utf-8") as f:
        #     f.write(res.text)
        return None

# --- Main Scraper Logic ---

def main():
    logging.info("INSEAD Knowledge Scraper Started.")

    if not AIRTABLE_API_KEY:
        logging.error("AIRTABLE_API_KEY environment variable not set. Exiting.")
        print("❌ Error: AIRTABLE_API_KEY not set. Please configure your environment variables.")
        return

    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    # Fetch existing article URLs from Airtable to avoid duplicates
    existing_urls = set()
    try:
        logging.info("Fetching existing article URLs from Airtable...")
        for record in table.all():
            url = record.get("fields", {}).get("Article URL")
            if url:
                existing_urls.add(normalize_url(url))
        logging.info(f"Found {len(existing_urls)} existing articles in Airtable.")
    except Exception as e:
        logging.error(f"Error loading existing records from Airtable: {e}")
        print(f"❌ Error loading existing records: {e}. Check Airtable config/permissions.")
        return

    # Scrape INSEAD homepage using cloudscraper
    try:
        logging.info(f"Attempting to fetch homepage: {BASE_URL}")
        # Use scraper for the main page
        response = scraper.get(BASE_URL, timeout=30) # Increased timeout for main page
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Using the selectors identified from previous successful run
        article_cards = soup.select("div.card-object")
        logging.info(f"Found {len(article_cards)} article cards on the homepage.")

    except Exception as e:
        logging.error(f"Error fetching or parsing homepage: {e}")
        print(f"❌ Error fetching homepage: {e}. Check internet connection or website status.")
        return

    added_count = 0
    skipped_duplicates_count = 0

    for card in article_cards:
        try:
            # The actual article details are often within a 'div.list-object' inside the 'card-object'
            list_object_element = card.select_one("div.list-object")
            if not list_object_element:
                logging.debug("Skipping card: No 'list-object' found within 'card-object'.")
                continue

            link_tag = list_object_element.select_one("a.list-object__heading-link")
            if not link_tag or not link_tag.has_attr("href"):
                logging.debug("Skipping card: No valid link tag with href found.")
                continue

            article_url = normalize_url(urljoin(BASE_URL, link_tag['href']))
            
            # Check for duplicates before processing further
            if article_url in existing_urls:
                logging.debug(f"Skipping duplicate: {article_url}")
                skipped_duplicates_count += 1
                continue

            title = link_tag.get_text(strip=True)
            
            category_tag = list_object_element.select_one(".list-object__category a")
            category = category_tag.get_text(strip=True) if category_tag else "N/A"

            summary_tag = list_object_element.select_one(".list-object__description")
            summary = summary_tag.get_text(strip=True) if summary_tag else "N/A"

            author_tag = list_object_element.select_one(".list-object__author")
            author_text = author_tag.get_text(strip=True).replace("By ", "").strip() if author_tag else "N/A"
            
            image_figure = card.select_one(".card-object__figure")
            image_tag = None
            if image_figure:
                image_tag = image_figure.select_one("picture img")
                if not image_tag: # Fallback to direct img if picture img not found
                    image_tag = image_figure.select_one("img")

            image_url = ""
            if image_tag:
                image_src = image_tag.get("src") or image_tag.get("data-src") # Check both src and data-src
                if image_src:
                    image_url = urljoin(BASE_URL, image_src)

            # Extract publication date by visiting the individual article page
            pub_date = extract_publication_date(article_url)

            # Prepare record for Airtable
            record_fields = {
                FIELD_TITLE: title,
                FIELD_ARTICLE_URL: article_url,
                FIELD_IMAGE_URL: image_url,
                FIELD_CATEGORY: category,
                FIELD_SUMMARY: summary,
                FIELD_AUTHOR: author_text,
            }
            if pub_date:
                record_fields[FIELD_PUBLICATION_DATE] = pub_date

            # Add record to Airtable
            table.create(record_fields)
            existing_urls.add(article_url) # Add to set to prevent re-adding in current run
            logging.info(f"✅ ADDED: '{title}' by {author_text}")
            added_count += 1

        except Exception as e:
            logging.error(f"❌ Failed to process an article card (URL: {article_url if 'article_url' in locals() else 'N/A'}): {e}", exc_info=True)
            # exc_info=True adds stack trace to the log for better debugging

    logging.info(f"Scraper Finished. {added_count} new article(s) added. {skipped_duplicates_count} duplicates skipped.")
    print(f"✅ Done. {added_count} new articles added. {skipped_duplicates_count} duplicates skipped. See insead_scrape.log for details.")

if __name__ == "__main__":
    main()
