import os
import cloudscraper
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime

# --- Configuration ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tbl7VYAVYoO9ySh0u"

# Airtable Field IDs
FIELD_CATEGORY = "fldAHCnq8IqIaRbHD"
FIELD_TITLE = "fldlUm4FqOpdD2RCj"
FIELD_PUBLICATION_DATE = "fldNiEGA9hBHpW4ah"
FIELD_AUTHOR = "fldw2dqoOXGxkkmgQ"
FIELD_SUMMARY = "fldK0gBQFV5DPgQn9"
FIELD_ARTICLE_URL = "fld6Uhrx1CzOWEZZT"
FIELD_IMAGE_URL = "fldnGyY3zX7aDhG6d"

BASE_URL = "https://knowledge.insead.edu"

# Logging setup - Temporarily set to DEBUG to get more detailed output
logging.basicConfig(
    filename='insead_scrape.log',
    filemode='w',
    level=logging.DEBUG, # <--- Changed to DEBUG for detailed diagnosis
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
    Visits an individual article page to extract the publication date and format it to ISO 8601.
    Uses cloudscraper to handle potential Cloudflare protection on article pages.
    """
    try:
        logging.debug(f"Attempting to fetch article page for date: {article_url}") # Debugging fetch attempt
        res = scraper.get(article_url, timeout=15)
        
        if res.status_code != 200:
            logging.error(f"Failed to load article page {article_url} for date (Status: {res.status_code}).")
            # Optional: Save HTML of problematic page for manual inspection
            # with open(f"failed_date_page_{urlparse(article_url).path.replace('/', '_').strip('.html')}.html", "w", encoding="utf-8") as f:
            #     f.write(res.text)
            return None
        
        logging.debug(f"Successfully fetched article page {article_url} (Status: {res.status_code}). Parsing HTML for date.")
        soup = BeautifulSoup(res.content, "html.parser")
        
        # Selector for the date tag
        date_tag = soup.select_one("a.link.link--date") 

        if date_tag:
            date_str = date_tag.get_text(strip=True)
            logging.debug(f"Found potential date string: '{date_str}' for {article_url}")
            try:
                # Parse the date string (e.g., "02 Jun 2025")
                date_object = datetime.strptime(date_str, "%d %b %Y")
                # Format to ISO 8601 (YYYY-MM-DD)
                iso_date = date_object.strftime("%Y-%m-%d")
                logging.debug(f"Successfully parsed and formatted date: '{date_str}' -> '{iso_date}'")
                return iso_date
            except ValueError as ve:
                logging.error(f"Failed to parse date string '{date_str}' from {article_url} into '%d %b %Y' format: {ve}", exc_info=True)
                return None
        else:
            logging.warning(f"Publication date tag 'a.link.link--date' NOT found for {article_url}. HTML structure might have changed or element is missing.")
            # Optional: Save HTML of page where date tag wasn't found
            # with open(f"no_date_tag_found_{urlparse(article_url).path.replace('/', '_').strip('.html')}.html", "w", encoding="utf-8") as f:
            #     f.write(res.text)
            return None
    except Exception as e:
        logging.error(f"General error during date extraction for {article_url}: {e}", exc_info=True)
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
        logging.error(f"Error loading existing records from Airtable: {e}", exc_info=True)
        print(f"❌ Error loading existing records: {e}. Check Airtable config/permissions.")
        return

    # Scrape INSEAD homepage using cloudscraper
    article_cards = [] 
    try:
        logging.info(f"Attempting to fetch homepage: {BASE_URL}")
        response = scraper.get(BASE_URL, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        article_cards = soup.select("div.card-object")
        logging.info(f"Found {len(article_cards)} article cards on the homepage.")

    except Exception as e:
        logging.error(f"Error fetching or parsing homepage: {e}", exc_info=True)
        print(f"❌ Error fetching homepage: {e}. Check internet connection or website status.")
        return

    added_count = 0
    skipped_duplicates_count = 0

    for card in article_cards:
        current_article_url = "N/A" 
        try:
            list_object_element = card.select_one("div.list-object")
            if not list_object_element:
                logging.debug("Skipping card: No 'list-object' found within 'card-object'.")
                continue

            link_tag = list_object_element.select_one("a.list-object__heading-link")
            if not link_tag or not link_tag.has_attr("href"):
                logging.debug("Skipping card: No valid link tag with href found.")
                continue

            current_article_url = normalize_url(urljoin(BASE_URL, link_tag['href']))
            
            if current_article_url in existing_urls:
                logging.debug(f"Skipping duplicate: {current_article_url}")
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
                if not image_tag:
                    image_tag = image_figure.select_one("img")

            image_url = ""
            if image_tag:
                image_src = image_tag.get("src") or image_tag.get("data-src")
                if image_src:
                    image_url = urljoin(BASE_URL, image_src)

            # --- Extract publication date by visiting the individual article page ---
            pub_date = extract_publication_date(current_article_url)

            # Prepare record for Airtable
            record_fields = {
                FIELD_TITLE: title,
                FIELD_ARTICLE_URL: current_article_url,
                FIELD_IMAGE_URL: image_url,
                FIELD_CATEGORY: category,
                FIELD_SUMMARY: summary,
                FIELD_AUTHOR: author_text,
            }
            if pub_date:
                record_fields[FIELD_PUBLICATION_DATE] = pub_date

            # Add record to Airtable
            table.create(record_fields)
            existing_urls.add(current_article_url)
            logging.info(f"✅ ADDED: '{title}' by {author_text} (Date: {pub_date if pub_date else 'N/A'})")
            added_count += 1

        except Exception as e:
            logging.error(f"❌ Failed to process article card (URL: {current_article_url}): {e}", exc_info=True)

    logging.info(f"Scraper Finished. {added_count} new article(s) added. {skipped_duplicates_count} duplicates skipped.")
    print(f"✅ Done. {added_count} new articles added. {skipped_duplicates_count} duplicates skipped. See insead_scrape.log for details.")

if __name__ == "__main__":
    main()
