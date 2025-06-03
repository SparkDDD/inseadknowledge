import subprocess
import logging
import os
import time
from pyairtable import Api
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright

# Install Chromium
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

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

logging.basicConfig(
    filename="insead_scrape.log",
    filemode="w",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def normalize_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_publication_date(context, url):
    try:
        page = context.new_page()
        page.goto(url, timeout=10000)
        page.wait_for_timeout(2000)
        date = page.locator("a.link.link--date").inner_text(timeout=2000)
        page.close()
        return date
    except Exception as e:
        logging.warning(f"❌ Failed to extract pub date from {url}: {e}")
        return None

def safe_airtable_insert(table, record, retries=3):
    for i in range(retries):
        try:
            return table.create(record)
        except Exception as e:
            logging.warning(f"⚠️ Airtable insert failed (try {i+1}): {e}")
            time.sleep(2)
    logging.error("❌ Final Airtable insert failed.")
    return None

def main():
    logging.info("🔄 Script started.")
    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    # Collect existing URLs
    existing_urls = set()
    for record in table.all():
        url = record.get("fields", {}).get("Article URL")
        if url:
            existing_urls.add(normalize_url(url))
    logging.info(f"📌 Loaded {len(existing_urls)} existing records.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto("https://knowledge.insead.edu/")
        page.wait_for_load_state("networkidle")
        time.sleep(2)

        # Accept cookies
        try:
            if page.locator("button:has-text('Accept all cookies')").is_visible():
                page.click("button:has-text('Accept all cookies')")
                page.wait_for_timeout(1000)
                logging.info("✅ Accepted cookies.")
        except Exception as e:
            logging.warning(f"⚠️ Cookie banner handling failed: {e}")

        # Save debug
        html = page.content()
        with open("debug.html", "w", encoding="utf-8") as f:
            f.write(html)

        # Parse articles using Playwright
        articles = page.locator("article.list-object")
        count = articles.count()
        logging.info(f"🔍 Found {count} article blocks.")

        added = 0
        skipped = 0
        for i in range(count):
            try:
                article = articles.nth(i)
                link = article.locator("a.list-object__heading-link")
                article_url = normalize_url(urljoin("https://knowledge.insead.edu/", link.get_attribute("href")))
                title = link.inner_text().strip()

                if article_url in existing_urls:
                    logging.info(f"⏭️ Skipped existing: {article_url}")
                    skipped += 1
                    continue

                category = article.locator(".list-object__category").inner_text(timeout=1000) or ""
                summary = article.locator(".list-object__description").inner_text(timeout=1000) or ""
                author = article.locator(".list-object__author").inner_text(timeout=1000) or ""
                img_tag = article.locator("picture img")
                image_url = img_tag.get_attribute("src") or img_tag.get_attribute("data-src") or ""

                pub_date = extract_publication_date(context, article_url)

                record = {
                    FIELD_TITLE: title,
                    FIELD_ARTICLE_URL: article_url,
                    FIELD_IMAGE_URL: image_url,
                    FIELD_CATEGORY: category,
                    FIELD_SUMMARY: summary,
                    FIELD_AUTHOR: author,
                }
                if pub_date:
                    record[FIELD_PUBLICATION_DATE] = pub_date

                logging.info(f"📦 Record:\n{record}")

                if not DRY_RUN:
                    result = safe_airtable_insert(table, record)
                    if result:
                        logging.info(f"✅ Added: {title}")
                        added += 1
                else:
                    print(f"[DRY RUN] Would insert: {record}")
                    added += 1

            except Exception as e:
                logging.error(f"❌ Failed to process article: {e}")

        browser.close()
        logging.info(f"🏁 Done. Added: {added}, Skipped: {skipped}, Total: {count}")
        print(f"✅ Done. Added: {added}, Skipped: {skipped}, Total: {count}")

if __name__ == "__main__":
    main()
