import subprocess
import logging
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright

# Automatically install the Chromium browser if missing
subprocess.run(["playwright", "install", "chromium"])

# Airtable config
AIRTABLE_API_KEY = "patQklX1y11lFtFFY.74b2fc99a09edbf052f3ff8fcf378c3c3b09397f0683dd171b968ad747a4035b"
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tbl7VYAVYoO9ySh0u"

FIELD_CATEGORY = "fldAHCnq8IqIaRbHD"
FIELD_TITLE = "fldlUm4FqOpdD2RCj"
FIELD_PUBLICATION_DATE = "fldNiEGA9hBHpW4ah"
FIELD_AUTHOR = "fldw2dqoOXGxkkmgQ"
FIELD_SUMMARY = "fldK0gBQFV5DPgQn9"
FIELD_ARTICLE_URL = "fld6Uhrx1CzOWEZZT"
FIELD_IMAGE_URL = "fldnGyY3zX7aDhG6d"

logging.basicConfig(
    filename="insead_scrape.log",
    filemode="w",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def normalize_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_publication_date(page, url):
    try:
        page.goto(url)
        page.wait_for_timeout(2000)
        soup = BeautifulSoup(page.content(), "html.parser")
        date_tag = soup.select_one("a.link.link--date")
        return date_tag.get_text(strip=True) if date_tag else None
    except Exception as e:
        logging.error(f"❌ Failed to extract date from {url}: {e}")
        return None

def main():
    logging.info("Script started.")
    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    existing_urls = set()
    for record in table.all():
        url = record.get("fields", {}).get("Article URL")
        if url:
            existing_urls.add(normalize_url(url))
    logging.info(f"Found {len(existing_urls)} existing articles.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://knowledge.insead.edu/")
        page.wait_for_timeout(4000)

        soup = BeautifulSoup(page.content(), "html.parser")
        articles = soup.select("article.list-object")
        logging.info(f"Found {len(articles)} article blocks.")

        added = 0
        for article in articles:
          try:
             link_tag = article.select_one("a.list-object__heading-link")
             if not link_tag:
               logging.info("SKIPPED: No link tag found.")
               continue

             article_url = normalize_url(urljoin("https://knowledge.insead.edu/", link_tag["href"]))
             logging.info(f"FOUND ARTICLE: {article_url}")

            if article_url in existing_urls:
              logging.info(f"SKIPPED (already exists): {article_url}")
              continue

                title = link_tag.get_text(strip=True)
                category = article.select_one(".list-object__category")
                summary = article.select_one(".list-object__description")
                author = article.select_one(".list-object__author")
                img_tag = article.select_one("picture img")

                image_url = img_tag.get("src") or img_tag.get("data-src") if img_tag else ""

                pub_date = extract_publication_date(page, article_url)

                record = {
                    FIELD_TITLE: title,
                    FIELD_ARTICLE_URL: article_url,
                    FIELD_IMAGE_URL: image_url,
                    FIELD_CATEGORY: category.get_text(strip=True) if category else "",
                    FIELD_SUMMARY: summary.get_text(strip=True) if summary else "",
                    FIELD_AUTHOR: author.get_text(strip=True) if author else "",
                }
                if pub_date:
                    record[FIELD_PUBLICATION_DATE] = pub_date

                table.create(record)
                logging.info(f"✅ ADDED: {title}")
                added += 1
            except Exception as e:
                logging.error(f"❌ Failed to process article: {e}")

        browser.close()
        logging.info(f"✅ Finished. {added} new article(s) added.")
        print(f"✅ Done. {added} new article(s) added.")

if __name__ == "__main__":
    main()
