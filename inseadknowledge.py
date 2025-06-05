import os
import cloudscraper
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime
import time  # For adding delays between requests
import json  # For parsing JSON responses
import re  # For regular expressions

# --- Configuration ---
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = "appoz4aD0Hjolycwd"
TABLE_ID = "tbl7VYAVYoO9ySh0u"

# Airtable Field Names (using common names instead of specific IDs, as IDs might be dynamic or incorrect)
FIELD_CATEGORY = "Category"
FIELD_TITLE = "Title"
FIELD_PUBLICATION_DATE = "Publication Date"
FIELD_AUTHOR = "Author"
FIELD_SUMMARY = "Summary"
FIELD_ARTICLE_URL = "Article URL"
FIELD_IMAGE_URL = "Image URL"

BASE_URL = "https://knowledge.insead.edu"
AJAX_ENDPOINT = "https://knowledge.insead.edu/views/ajax"  # The base URL for AJAX calls

# Logging setup - Temporarily set to DEBUG for detailed diagnosis
logging.basicConfig(filename='insead_scrape.log',
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] %(message)s')

# Initialize cloudscraper once globally
scraper = cloudscraper.create_scraper()

# --- Helper Functions ---


def normalize_url(url):
    """Normalizes a URL by parsing and reconstructing it without query parameters or fragments."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")


def extract_publication_date(article_url):
    """
    Visits an individual article page to extract the publication date, prioritizing meta tags.
    Formats the date to ISO 8601 (YYYY-MM-DD).
    """
    try:
        logging.debug(
            f"Attempting to fetch article page for date: {article_url}")
        res = scraper.get(article_url, timeout=15)

        if res.status_code != 200:
            logging.error(
                f"Failed to load article page {article_url} for date (Status: {res.status_code})."
            )
            return None

        logging.debug(
            f"Successfully fetched article page {article_url} (Status: {res.status_code}). Parsing HTML for date."
        )
        soup = BeautifulSoup(res.content, "html.parser")

        # --- PRIORITY 1: Extract from <meta property="article:published_time"> tag ---
        meta_pub_date_tag = soup.find("meta",
                                      property="article:published_time")
        if meta_pub_date_tag and meta_pub_date_tag.has_attr("content"):
            date_iso_str = meta_pub_date_tag["content"]
            try:
                # Parse ISO 8601 string (like "2025-06-02T09:00:00+0800")
                # and format to `%Y-%m-%d`
                date_object = datetime.fromisoformat(
                    date_iso_str.replace('Z', '+00:00'))
                iso_date = date_object.strftime("%Y-%m-%d")
                logging.debug(
                    f"Found and formatted date from meta tag: '{date_iso_str}' -> '{iso_date}'"
                )
                return iso_date
            except ValueError as ve:
                logging.error(
                    f"Failed to parse ISO date string '{date_iso_str}' from meta tag for {article_url}: {ve}",
                    exc_info=True)
        else:
            logging.debug(
                f"Meta tag 'article:published_time' not found or content attribute missing for {article_url}. Falling back to link tag."
            )

        # --- PRIORITY 2: Fallback to existing 'a.link.link--date' selector ---
        date_tag = soup.select_one("a.link.link--date")
        if date_tag:
            date_str = date_tag.get_text(strip=True)
            logging.debug(
                f"Found potential date string from link tag: '{date_str}' for {article_url}"
            )
            try:
                date_object = datetime.strptime(date_str, "%d %b %Y")
                iso_date = date_object.strftime("%Y-%m-%d")
                logging.debug(
                    f"Successfully parsed and formatted date from link tag: '{date_str}' -> '{iso_date}')"
                )
                return iso_date
            except ValueError as ve:
                logging.error(
                    f"Failed to parse date string '{date_str}' from link tag for {article_url} into '%d %b %Y' format: {ve}",
                    exc_info=True)
                return None
        else:
            logging.warning(
                f"Publication date tag 'a.link.link--date' NOT found for {article_url}. HTML structure might have changed or element is missing."
            )
            return None
    except Exception as e:
        logging.error(
            f"General error during date extraction for {article_url}: {e}",
            exc_info=True)
        return None


def process_and_add_articles(article_cards, existing_urls, table,
                             added_count_ref, skipped_duplicates_count_ref):
    """
    Processes a list of BeautifulSoup article card elements and adds them to Airtable.
    Updates the counts of added and skipped articles via mutable references.
    """
    for card in article_cards:
        current_article_url = "N/A"
        try:
            list_object_element = card.select_one("div.list-object")
            if not list_object_element:
                logging.debug(
                    "Skipping card: No 'list-object' found within 'card-object'."
                )
                continue

            link_tag = list_object_element.select_one(
                "a.list-object__heading-link")
            if not link_tag or not link_tag.has_attr("href"):
                logging.debug(
                    "Skipping card: No valid link tag with href found.")
                continue

            current_article_url = normalize_url(
                urljoin(BASE_URL, link_tag['href']))

            if current_article_url in existing_urls:
                logging.debug(f"Skipping duplicate: {current_article_url}")
                skipped_duplicates_count_ref[0] += 1
                continue

            title = link_tag.get_text(strip=True)

            category_tag = list_object_element.select_one(
                ".list-object__category a")
            category = category_tag.get_text(
                strip=True) if category_tag else "N/A"

            summary_tag = list_object_element.select_one(
                ".list-object__description")
            summary = summary_tag.get_text(
                strip=True) if summary_tag else "N/A"

            author_tag = list_object_element.select_one(".list-object__author")
            author_text = author_tag.get_text(
                strip=True).replace("By ", "").strip() if author_tag else "N/A"

            # --- Improved image extraction ---
            image_url = ""
            image_figure = card.select_one(".card-object__figure")
            if image_figure:
                picture_tag = image_figure.select_one("picture")
                if picture_tag:
                    source_tag = picture_tag.select_one("source")
                    img_tag = picture_tag.select_one("img")

                    image_src = ""
                    if source_tag and source_tag.has_attr("srcset"):
                        image_src = source_tag["srcset"]
                    elif img_tag:
                        image_src = img_tag.get("src") or img_tag.get(
                            "data-src")

                    if image_src:
                        image_url = urljoin(BASE_URL, image_src)
                        logging.debug(f"Extracted image URL: {image_url}")

            pub_date = extract_publication_date(current_article_url)

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

            table.create(record_fields)
            existing_urls.add(current_article_url)
            logging.info(
                f"✅ ADDED: '{title}' by {author_text} (Date: {pub_date if pub_date else 'N/A'})"
            )
            added_count_ref[0] += 1

        except Exception as e:
            logging.error(
                f"❌ Failed to process article card (URL: {current_article_url}): {e}",
                exc_info=True)


# --- Main Scraper Logic ---


def main():
    logging.info("INSEAD Knowledge Scraper Started.")

    if not AIRTABLE_API_KEY:
        logging.error(
            "AIRTABLE_API_KEY environment variable not set. Exiting.")
        print(
            "❌ Error: AIRTABLE_API_KEY not set. Please configure your environment variables."
        )
        return

    api = Api(AIRTABLE_API_KEY)
    table = api.table(BASE_ID, TABLE_ID)

    existing_urls = set()
    try:
        logging.info("Fetching existing article URLs from Airtable...")
        for record in table.all():
            url = record.get("fields", {}).get(FIELD_ARTICLE_URL)
            if url:
                existing_urls.add(normalize_url(url))
        logging.info(
            f"Found {len(existing_urls)} existing articles in Airtable.")
    except Exception as e:
        logging.error(f"Error loading existing records from Airtable: {e}",
                      exc_info=True)
        print(
            f"❌ Error loading existing records: {e}. Check Airtable config/permissions."
        )
        return

    # Use mutable lists to pass counts by reference
    added_count = [0]
    skipped_duplicates_count = [0]

    ajax_libraries_param = ""
    view_dom_id = "2bcf87ffae10d903e48004546039697aebd0e6dc08d71cbbb4b8e009c1559405"  # Default/Hardcoded Value

    # --- Fetch initial homepage and extract ajax_page_state[libraries] & view_dom_id ---
    logging.info(f"Fetching initial homepage: {BASE_URL}")
    try:
        response = scraper.get(BASE_URL, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        initial_cards = soup.select("div.card-object")
        logging.info(
            f"Fetched {len(initial_cards)} articles from initial load.")

        # Process and add initial cards immediately
        process_and_add_articles(initial_cards, existing_urls, table,
                                 added_count, skipped_duplicates_count)

        # --- Extract ajax_page_state[libraries] and view_dom_id from Drupal.settings JSON ---
        drupal_settings_json = None
        script_tags = soup.find_all("script",
                                    type="application/json",
                                    string=re.compile(r"Drupal\.settings"))
        for script_tag in script_tags:
            try:
                script_content = script_tag.string
                if script_content:
                    drupal_settings_json = json.loads(script_content)
                    break  # Found and parsed Drupal.settings
            except json.JSONDecodeError:
                logging.debug(
                    "Script tag content was not valid JSON for Drupal.settings."
                )
                continue
            except Exception as e:
                logging.debug(
                    f"Error parsing script tag for Drupal.settings: {e}")
                continue

        if drupal_settings_json:
            ajax_page_state = drupal_settings_json.get('ajaxPageState', {})
            ajax_libraries_param = ajax_page_state.get('libraries', '')
            if ajax_libraries_param:
                logging.info(
                    f"Extracted ajax_page_state[libraries] from JSON: {ajax_libraries_param[:50]}..."
                )
            else:
                logging.warning(
                    "ajax_page_state[libraries] not found in Drupal.settings JSON."
                )

            views_ajax_settings = drupal_settings_json.get('views',
                                                           {}).get('ajax', {})
            for view_id, view_data in views_ajax_settings.items():
                if 'dom_id' in view_data:
                    view_dom_id = view_data['dom_id']
                    logging.info(
                        f"Extracted dynamic view_dom_id from JSON: {view_dom_id}"
                    )
                    break
            if view_dom_id == "2bcf87ffae10d903e48004546039697aebd0e6dc08d71cbbb4b8e009c1559405":
                logging.warning(
                    "Dynamic view_dom_id not found in Drupal.settings JSON. Using hardcoded value."
                )
        else:
            logging.warning(
                "Drupal.settings JSON not found. Attempting regex fallback for AJAX parameters."
            )
            # Fallback to regex if JSON parsing fails or Drupal.settings not found
            html_content_str = response.content.decode('utf-8')
            match_libs = re.search(r'"ajaxPageState":{"libraries":"(.*?)"',
                                   html_content_str)
            if match_libs:
                ajax_libraries_param = match_libs.group(1)
                logging.info(
                    f"Extracted ajax_page_state[libraries] via regex: {ajax_libraries_param[:50]}..."
                )
            else:
                logging.warning(
                    "Could not extract ajax_page_state[libraries] via regex. AJAX calls might fail."
                )

            dom_id_match = re.search(r"'view_dom_id':\s*'([a-f0-9]+)'",
                                     html_content_str)
            if dom_id_match:
                view_dom_id = dom_id_match.group(1)
                logging.info(f"Extracted view_dom_id via regex: {view_dom_id}")
            else:
                logging.warning(
                    "Could not extract view_dom_id via regex. Using hardcoded value."
                )

        time.sleep(5)

    except Exception as e:
        logging.error(
            f"Error fetching initial homepage or extracting AJAX state: {e}",
            exc_info=True)
        print(
            f"❌ Error fetching initial homepage or extracting AJAX state: {e}. Exiting."
        )
        return

    page_num = 2
    max_pagination_attempts = 5
    total_articles_fetched_from_ajax = 0  # To track articles fetched from AJAX pages

    while page_num <= max_pagination_attempts:
        request_params = {
            "_wrapper_format": "drupal_ajax",
            "view_name": "topics",
            "view_display_id": "topic_block",
            "view_args": "",
            "view_path": "/node/11",
            "view_base_path": "",
            "view_dom_id": view_dom_id,
            "pager_element": "0",
            "page": page_num - 1,
            "_drupal_ajax": "1",
            "ajax_page_state[theme]": "knowledge_theme",
            "ajax_page_state[theme_token]": "",
        }

        if ajax_libraries_param:
            request_params["ajax_page_state[libraries]"] = ajax_libraries_param
        else:
            logging.warning(
                f"ajax_page_state[libraries] was empty. Proceeding without it for page {page_num}, but requests may fail."
            )

        logging.info(
            f"Fetching page {page_num} via AJAX from {AJAX_ENDPOINT} with params: {request_params}"
        )

        try:
            response = scraper.get(AJAX_ENDPOINT,
                                   params=request_params,
                                   timeout=30)
            response.raise_for_status()

            response_content = response.text

            response_json = None

            # Try to parse directly first, then fallback to textarea
            try:
                response_json = json.loads(response_content)
                logging.debug(
                    f"Successfully parsed direct JSON for page {page_num}.")
            except json.JSONDecodeError:
                logging.debug(
                    f"Direct JSON parsing failed for page {page_num}. Checking for textarea wrapper."
                )
                soup_ajax = BeautifulSoup(response_content, 'html.parser')
                textarea_tag = soup_ajax.find('textarea')
                if textarea_tag:
                    json_string = textarea_tag.text
                    try:
                        response_json = json.loads(json_string)
                        logging.debug(
                            f"Successfully parsed JSON from textarea for page {page_num}."
                        )
                    except json.JSONDecodeError as jde:
                        logging.error(
                            f"Failed to decode JSON from textarea content for page {page_num}: {jde}",
                            exc_info=True)
                        logging.error(
                            f"Textarea content (first 500 chars): {json_string[:500]}..."
                        )
                        print(
                            f"❌ Error: Textarea content for page {page_num} was not valid JSON. Halting pagination."
                        )
                        break
                else:
                    logging.error(
                        f"AJAX response for page {page_num} could not be parsed as JSON (neither direct nor from textarea). Halting."
                    )
                    break

            if response_json is None:  # Should ideally be caught by inner blocks, but as a safeguard
                logging.error(
                    f"AJAX response for page {page_num} could not be parsed as JSON. Halting."
                )
                break

            new_cards_html = ""

            for command in response_json:
                logging.debug(
                    f"Processing command: {command.get('command')}, selector: {command.get('selector')}"
                )
                if command.get("command") == "insert" and "data" in command:
                    # Prefer the selector that matches the dynamic view_dom_id
                    if command.get(
                            "selector") == ".js-view-dom-id-" + view_dom_id:
                        new_cards_html = command["data"]
                        logging.debug(
                            f"Found 'insert' command matching dynamic view_dom_id for page {page_num}."
                        )
                        break
                    # Fallback to other common selectors
                    elif command.get(
                            "selector"
                    ) == ".block-views-blocktopics-topic-block .view-content":
                        new_cards_html = command["data"]
                        logging.debug(
                            f"Found 'insert' command with .block-views-blocktopics-topic-block selector for page {page_num}."
                        )
                        break
                    elif command.get(
                            "selector"
                    ) == "#block-knowledge-theme-content-block-2":
                        new_cards_html = command["data"]
                        logging.debug(
                            f"Found 'insert' command with #block-knowledge-theme-content-block-2 selector for page {page_num}."
                        )
                        break
                    elif command.get("selector") == ".views-element-container":
                        new_cards_html = command["data"]
                        logging.debug(
                            f"Found 'insert' command with .views-element-container selector for page {page_num}."
                        )
                        break

            if not new_cards_html:
                logging.info(
                    f"No HTML content found in AJAX response commands for page {page_num}. Ending pagination."
                )
                break

            soup = BeautifulSoup(new_cards_html, "html.parser")
            new_cards = soup.select("div.card-object")

            if not new_cards:
                logging.info(
                    f"No more articles found in HTML snippet for page {page_num}. Ending pagination."
                )
                break

            total_articles_fetched_from_ajax += len(new_cards)
            logging.info(
                f"Fetched {len(new_cards)} articles for page {page_num}. Total articles found from AJAX so far: {total_articles_fetched_from_ajax}"
            )

            # Process and add newly fetched articles immediately
            process_and_add_articles(new_cards, existing_urls, table,
                                     added_count, skipped_duplicates_count)

        except Exception as e:
            logging.error(
                f"Error during AJAX request or parsing for page {page_num}: {e}",
                exc_info=True)
            print(
                f"❌ Error fetching page {page_num}: {e}. Halting pagination.")
            break

        page_num += 1
        time.sleep(5)

    logging.info(
        f"Scraper Finished. {added_count[0]} new article(s) added. {skipped_duplicates_count[0]} duplicates skipped."
    )
    print(
        f"✅ Done. {added_count[0]} new articles added. {skipped_duplicates_count[0]} duplicates skipped. See insead_scrape.log for details."
    )


if __name__ == "__main__":
    main()
