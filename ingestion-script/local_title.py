import requests
import psycopg2
import xml.etree.ElementTree as ET
import re
import time
from datetime import datetime

# =======================
# CONFIGURATION
# =======================
BASE_URL = "https://www.ecfr.gov/api/versioner/v1/full/2025-08-30/title-{}.xml"
TITLES = range(1, 51)   # Titles 1–50
REQUEST_DELAY = 1.2      # Pause between API calls

PG_CONN = {
    "host": "localhost",
    "port": 5432,
    "database": "ecfrdb",
    "user": "postgres",
    "password": "XYZ"
}

# =======================
# POSTGRES: CREATE TABLE
# =======================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ecfr_chapter_wordcount (
    id SERIAL PRIMARY KEY,
    title INTEGER NOT NULL,
    chapter_identifier TEXT,  -- Chapter number like I, II, III
    chapter_heading TEXT,
    word_count INTEGER NOT NULL,
    character_count INTEGER,
    is_reserved BOOLEAN DEFAULT FALSE,
    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_SQL = """
INSERT INTO ecfr_chapter_wordcount (title, chapter_identifier, chapter_heading, word_count, character_count, is_reserved)
VALUES (%s, %s, %s, %s, %s, %s)
RETURNING id;
"""


# =======================
# HELPERS
# =======================
def setup_postgres():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    conn.close()


def fetch_title_xml(title_number: int) -> str:
    url = BASE_URL.format(title_number)
    print(f"📥 Downloading Title {title_number} from {url}")

    resp = requests.get(url, headers={"accept": "application/xml"}, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch Title {title_number}, HTTP {resp.status_code}")

    return resp.text


def clean_text(xml_content: str) -> str:
    """Strip XML tags and return plain text."""
    # remove tags
    text = re.sub(r"<[^>]+>", " ", xml_content)

    # normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text


def word_count(text: str) -> int:
    """Return number of words in a text."""
    if not text:
        return 0
    return len(text.split())


def parse_chapters_for_wordcount(xml_text: str):
    """
    Parse XML and yield:
    (chapter_label, chapter_heading, word_count_int)
    """
    root = ET.fromstring(xml_text)
    
    # Find the title element first
    title_elem = root.find('.//DIV1[@TYPE="TITLE"]')
    if title_elem is None:
        print("Warning: Could not find TITLE element")
        return
    
    # Look for chapters (DIV3 with TYPE="CHAPTER") within the title
    for chapter in title_elem.iter():
        if chapter.tag.endswith("DIV3") and chapter.attrib.get("TYPE") == "CHAPTER":
            # Get chapter identifier (N attribute)
            chapter_label = chapter.attrib.get("N", "")
            
            # Get chapter heading
            heading_el = chapter.find("HEAD")
            chapter_heading = heading_el.text.strip() if heading_el is not None else ""
            
            # Extract all text from this chapter
            def extract_text(element):
                text = ""
                if element.text:
                    text += element.text + " "
                for child in element:
                    text += extract_text(child)
                    if child.tail:
                        text += child.tail + " "
                return text
            
            chapter_text = extract_text(chapter)
            # Clean and count words
            plain_text = clean_text(chapter_text)
            wc = word_count(plain_text)
            
            yield (chapter_label, chapter_heading, wc)


def insert_rows(title_number: int, rows):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    inserted = 0

    for chapter_label, chapter_heading, wc in rows:
        # Check if chapter is reserved
        is_reserved = "[Reserved]" in chapter_heading if chapter_heading else False
        
        # Calculate character count
        char_count = len(chapter_heading) if chapter_heading else 0
        
        cur.execute(
            INSERT_SQL,
            (title_number, chapter_label, chapter_heading, wc, char_count, is_reserved)
        )
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    return inserted


# =======================
# MAIN
# =======================
def main():
    print("🔧 Setting up PostgreSQL…")
    setup_postgres()

    for title in TITLES:
        try:
            xml_text = fetch_title_xml(title)
            chapters = list(parse_chapters_for_wordcount(xml_text))
            count = insert_rows(title, chapters)

            print(f"✅ Title {title}: inserted wordcount for {count} chapters")

        except Exception as e:
            print(f"❌ Error processing Title {title}: {e}")

        time.sleep(REQUEST_DELAY)

    print("🎉 All titles processed.")


if __name__ == "__main__":
    main()