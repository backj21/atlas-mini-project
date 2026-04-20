"""
IlliniNest Reddit Scraper
=========================
Scrapes r/UIUC (and supplementary subreddits) for apartment-related posts
and comments, then resolves each mention to a canonical complex_id.

Run this on your LOCAL MACHINE — Reddit blocks server/cloud environments.

Usage:
    python reddit_scraper.py

Requirements:
    pip install requests pandas rapidfuzz tqdm

Output files (saved to data/raw/reddit/):
    reddit_posts_raw.csv     — raw post data
    reddit_comments_raw.csv  — raw comment data

Output file (saved to data/outputs/):
    mentions.csv             — resolved, filtered mentions ready for NLP

Methods tried in order (automatic fallback):
    1. Arctic Shift API  (https://arctic-shift.photon-reddit.com)
    2. Pullpush API      (https://api.pullpush.io)
    3. Reddit JSON API   (https://www.reddit.com/r/UIUC/search.json)
    4. PRAW              (requires Reddit API credentials in .env)
"""

import os
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from rapidfuzz import fuzz, process
from tqdm import tqdm

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# Subreddits to scrape
SUBREDDITS = [
    "UIUC",
    "UIUC_Housing",
    "ChampaignUrbana",
]

# Date range — 2 years of history
DATE_END   = datetime.utcnow()
DATE_START = DATE_END - timedelta(days=730)

# Minimum word count for a mention to be considered meaningful
MIN_WORD_COUNT = 10

# Fuzzy match threshold — 0-100, higher = stricter
FUZZY_THRESHOLD = 75

# Output directories
RAW_DIR    = os.path.join("data", "raw", "reddit")
OUTPUT_DIR = os.path.join("data", "outputs")

# Request headers — always set a descriptive User-Agent
HEADERS = {
    "User-Agent": "IlliniNest-ApartmentResearch/1.0 (UIUC student project; contact via github)"
}

# Rate limit delay between requests (seconds)
REQUEST_DELAY = 1.5


# ─────────────────────────────────────────────
# ENTITY LOOKUP TABLE
# Maps every known name variant → canonical complex_id
# Add more variants as you discover them in raw data
# ─────────────────────────────────────────────

ENTITY_LOOKUP = {
    # Hendrick House
    "hendrick house":         "hendrick_house",
    "hendrick":               "hendrick_house",
    "hendricks house":        "hendrick_house",
    "hendricks":              "hendrick_house",

    # 309 Green / JSM Green
    "309 green":              "jsm_309_green",
    "309 e green":            "jsm_309_green",
    "309 east green":         "jsm_309_green",
    "jsm green":              "jsm_309_green",

    # Illini Tower
    "illini tower":           "illini_tower",
    "illini towers":          "illini_tower",

    # Lincoln Square
    "lincoln square":         "lincoln_square",
    "lincoln square apts":    "lincoln_square",

    # Campustown Arms
    "campustown arms":        "campustown_arms",
    "campustown":             "campustown_arms",

    # Green Street Realty / Green & Armory
    "green and armory":       "green_armory",
    "green & armory":         "green_armory",
    "green armory":           "green_armory",

    # 706 W. Oregon (Ramshaw)
    "706 oregon":             "ramshaw_706_oregon",
    "706 w oregon":           "ramshaw_706_oregon",
    "706 west oregon":        "ramshaw_706_oregon",

    # University Village
    "university village":     "university_village",
    "u village":              "university_village",

    # Bromley Hall
    "bromley":                "bromley_hall",
    "bromley hall":           "bromley_hall",

    # JSM (generic — flags any JSM mention for manual review)
    "jsm":                    "jsm_generic",
    "jsm properties":         "jsm_generic",
    "jsm realty":             "jsm_generic",

    # Ramshaw (generic)
    "ramshaw":                "ramshaw_generic",
    "ramshaw real estate":    "ramshaw_generic",

    # Bankier (generic)
    "bankier":                "bankier_generic",
    "bankier apartments":     "bankier_generic",
}

# Keywords to search within each subreddit
# These are combined with complex names in the search queries
SEARCH_KEYWORDS = [
    "apartment",
    "lease",
    "landlord",
    "maintenance",
    "deposit",
    "move out",
    "move in",
    "JSM",
    "Ramshaw",
    "Bankier",
    "rent",
    "sublease",
    "sublet",
    "roommate",
    "housing",
    "complex",
] + list({v.replace("_", " ") for v in set(ENTITY_LOOKUP.values()) if "generic" not in v})


# ─────────────────────────────────────────────
# SCRAPING METHODS
# ─────────────────────────────────────────────

class ArcticShiftScraper:
    """
    Arctic Shift: community-maintained Pushshift mirror.
    Docs: https://arctic-shift.photon-reddit.com
    """
    BASE_URL = "https://arctic-shift.photon-reddit.com/api"

    def search_posts(self, subreddit, keyword, after, before, limit=100):
        url = f"{self.BASE_URL}/posts/search"
        params = {
            "subreddit": subreddit,
            "q":         keyword,
            "after":     int(after.timestamp()),
            "before":    int(before.timestamp()),
            "limit":     limit,
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            print(f"  [ArcticShift posts] {keyword}: {e}")
            return []

    def search_comments(self, subreddit, keyword, after, before, limit=100):
        url = f"{self.BASE_URL}/comments/search"
        params = {
            "subreddit": subreddit,
            "q":         keyword,
            "after":     int(after.timestamp()),
            "before":    int(before.timestamp()),
            "limit":     limit,
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            print(f"  [ArcticShift comments] {keyword}: {e}")
            return []


class PullpushScraper:
    """
    Pullpush: another Pushshift-compatible API.
    Docs: https://api.pullpush.io
    """
    BASE_URL = "https://api.pullpush.io/reddit"

    def search_posts(self, subreddit, keyword, after, before, limit=100):
        url = f"{self.BASE_URL}/search/submission/"
        params = {
            "subreddit": subreddit,
            "q":         keyword,
            "after":     int(after.timestamp()),
            "before":    int(before.timestamp()),
            "size":      limit,
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            print(f"  [Pullpush posts] {keyword}: {e}")
            return []

    def search_comments(self, subreddit, keyword, after, before, limit=100):
        url = f"{self.BASE_URL}/search/comment/"
        params = {
            "subreddit": subreddit,
            "q":         keyword,
            "after":     int(after.timestamp()),
            "before":    int(before.timestamp()),
            "size":      limit,
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            print(f"  [Pullpush comments] {keyword}: {e}")
            return []


class RedditJSONScraper:
    """
    Reddit's public JSON API — no credentials needed.
    Limited to ~100 results per query, no historical access beyond ~1 year.
    """
    BASE_URL = "https://www.reddit.com"

    def search_posts(self, subreddit, keyword, after=None, before=None, limit=100):
        url = f"{self.BASE_URL}/r/{subreddit}/search.json"
        params = {
            "q":           keyword,
            "restrict_sr": "true",
            "sort":        "new",
            "limit":       min(limit, 100),
            "type":        "link",
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            children = r.json().get("data", {}).get("children", [])
            return [c["data"] for c in children]
        except Exception as e:
            print(f"  [RedditJSON posts] {keyword}: {e}")
            return []

    def search_comments(self, subreddit, keyword, after=None, before=None, limit=100):
        url = f"{self.BASE_URL}/r/{subreddit}/search.json"
        params = {
            "q":           keyword,
            "restrict_sr": "true",
            "sort":        "new",
            "limit":       min(limit, 100),
            "type":        "comment",
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            children = r.json().get("data", {}).get("children", [])
            return [c["data"] for c in children]
        except Exception as e:
            print(f"  [RedditJSON comments] {keyword}: {e}")
            return []


class PRAWScraper:
    """
    PRAW fallback — requires Reddit API credentials.
    Set these in a .env file or as environment variables:
        REDDIT_CLIENT_ID
        REDDIT_CLIENT_SECRET
        REDDIT_USER_AGENT
    """
    def __init__(self):
        self.reddit = None
        try:
            import praw
            self.reddit = praw.Reddit(
                client_id     = os.getenv("REDDIT_CLIENT_ID"),
                client_secret = os.getenv("REDDIT_CLIENT_SECRET"),
                user_agent    = os.getenv("REDDIT_USER_AGENT", "IlliniNest/1.0"),
            )
        except Exception as e:
            print(f"  [PRAW] Could not initialize: {e}")

    def search_posts(self, subreddit, keyword, after=None, before=None, limit=100):
        if not self.reddit:
            return []
        try:
            sub = self.reddit.subreddit(subreddit)
            results = []
            for post in sub.search(keyword, sort="new", limit=limit):
                results.append({
                    "id":          post.id,
                    "title":       post.title,
                    "selftext":    post.selftext,
                    "author":      str(post.author),
                    "created_utc": post.created_utc,
                    "score":       post.score,
                    "url":         f"https://reddit.com{post.permalink}",
                    "subreddit":   subreddit,
                })
            return results
        except Exception as e:
            print(f"  [PRAW posts] {keyword}: {e}")
            return []

    def search_comments(self, subreddit, keyword, after=None, before=None, limit=100):
        if not self.reddit:
            return []
        try:
            sub = self.reddit.subreddit(subreddit)
            results = []
            for comment in sub.search(keyword, sort="new", limit=limit):
                if hasattr(comment, "body"):
                    results.append({
                        "id":          comment.id,
                        "body":        comment.body,
                        "author":      str(comment.author),
                        "created_utc": comment.created_utc,
                        "score":       comment.score,
                        "url":         f"https://reddit.com{comment.permalink}",
                        "subreddit":   subreddit,
                    })
            return results
        except Exception as e:
            print(f"  [PRAW comments] {keyword}: {e}")
            return []


def get_working_scraper():
    """
    Try each scraper in order and return the first one that works.
    Tests with a single lightweight request before committing.
    """
    scrapers = [
        ("Arctic Shift", ArcticShiftScraper()),
        ("Pullpush",     PullpushScraper()),
        ("Reddit JSON",  RedditJSONScraper()),
        ("PRAW",         PRAWScraper()),
    ]

    test_after  = datetime.utcnow() - timedelta(days=30)
    test_before = datetime.utcnow()

    for name, scraper in scrapers:
        print(f"Testing {name}...")
        try:
            result = scraper.search_posts("UIUC", "apartment", test_after, test_before, limit=3)
            if result and len(result) > 0:
                print(f"  ✓ {name} working — {len(result)} test results returned")
                return name, scraper
            else:
                print(f"  ✗ {name} returned no results")
        except Exception as e:
            print(f"  ✗ {name} failed: {e}")

    raise RuntimeError("All scrapers failed. Check your internet connection and Reddit API status.")


# ─────────────────────────────────────────────
# NORMALISATION HELPERS
# ─────────────────────────────────────────────

def normalise_post(raw, source_name, subreddit):
    """Convert a raw API response dict into a standard row."""
    text  = raw.get("selftext") or raw.get("body") or ""
    title = raw.get("title", "")
    return {
        "post_id":      raw.get("id", ""),
        "title":        title,
        "text":         text,
        "full_text":    f"{title} {text}".strip(),
        "author":       str(raw.get("author", "[deleted]")),
        "created_utc":  raw.get("created_utc", 0),
        "upvotes":      raw.get("score", 0),
        "url":          raw.get("url") or raw.get("full_link", ""),
        "is_post":      "selftext" in raw or "title" in raw,
        "subreddit":    raw.get("subreddit", subreddit),
        "source":       source_name,
    }


def normalise_comment(raw, source_name, subreddit):
    """Convert a raw comment dict into a standard row."""
    return {
        "post_id":      raw.get("id", ""),
        "title":        "",
        "text":         raw.get("body", ""),
        "full_text":    raw.get("body", ""),
        "author":       str(raw.get("author", "[deleted]")),
        "created_utc":  raw.get("created_utc", 0),
        "upvotes":      raw.get("score", 0),
        "url":          raw.get("url") or raw.get("permalink", ""),
        "is_post":      False,
        "subreddit":    raw.get("subreddit", subreddit),
        "source":       source_name,
    }


# ─────────────────────────────────────────────
# ENTITY RESOLUTION
# ─────────────────────────────────────────────

def resolve_complex(text):
    """
    Try to match text to a canonical complex_id.

    Strategy:
    1. Exact match after lowercasing
    2. Fuzzy match against all known variants using rapidfuzz

    Returns (complex_id, confidence) or (None, 0) if no match found.
    """
    if not text or not isinstance(text, str):
        return None, 0

    text_lower = text.lower()

    # 1. Exact substring match — fast and unambiguous
    for variant, complex_id in ENTITY_LOOKUP.items():
        if variant in text_lower:
            return complex_id, 100

    # 2. Fuzzy match — catches misspellings and abbreviations
    variants = list(ENTITY_LOOKUP.keys())
    best_match, score, _ = process.extractOne(
        text_lower,
        variants,
        scorer=fuzz.partial_ratio
    )

    if score >= FUZZY_THRESHOLD:
        return ENTITY_LOOKUP[best_match], score

    return None, 0


def is_meaningful(text, min_words=MIN_WORD_COUNT):
    """
    Returns True if a mention is long enough to carry useful signal.
    Filters out single-word replies, bot responses, and deleted content.
    """
    if not text or not isinstance(text, str):
        return False
    if text.strip() in ["[deleted]", "[removed]", ""]:
        return False
    word_count = len(text.split())
    return word_count >= min_words


# ─────────────────────────────────────────────
# MAIN SCRAPE LOOP
# ─────────────────────────────────────────────

def scrape_all(scraper_name, scraper):
    """
    Runs the full scrape across all subreddits and keywords.
    Deduplicates by post_id and saves raw CSVs before any processing.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_posts    = []
    all_comments = []
    seen_ids     = set()

    total_queries = len(SUBREDDITS) * len(SEARCH_KEYWORDS)
    print(f"\nStarting scrape with {scraper_name}")
    print(f"Subreddits: {SUBREDDITS}")
    print(f"Keywords:   {len(SEARCH_KEYWORDS)} total")
    print(f"Date range: {DATE_START.date()} → {DATE_END.date()}")
    print(f"Total queries: {total_queries * 2} (posts + comments)\n")

    with tqdm(total=total_queries, desc="Scraping") as pbar:
        for subreddit in SUBREDDITS:
            for keyword in SEARCH_KEYWORDS:
                pbar.set_description(f"r/{subreddit} · {keyword[:20]}")

                # Posts
                raw_posts = scraper.search_posts(
                    subreddit, keyword, DATE_START, DATE_END, limit=100
                )
                for raw in raw_posts:
                    row = normalise_post(raw, scraper_name, subreddit)
                    if row["post_id"] not in seen_ids:
                        seen_ids.add(row["post_id"])
                        all_posts.append(row)

                time.sleep(REQUEST_DELAY)

                # Comments
                raw_comments = scraper.search_comments(
                    subreddit, keyword, DATE_START, DATE_END, limit=100
                )
                for raw in raw_comments:
                    row = normalise_comment(raw, scraper_name, subreddit)
                    if row["post_id"] not in seen_ids:
                        seen_ids.add(row["post_id"])
                        all_comments.append(row)

                time.sleep(REQUEST_DELAY)
                pbar.update(1)

    return all_posts, all_comments


# ─────────────────────────────────────────────
# PROCESSING & EXPORT
# ─────────────────────────────────────────────

def save_raw(posts, comments):
    """Save raw data immediately — before any filtering or resolution."""
    posts_path    = os.path.join(RAW_DIR, "reddit_posts_raw.csv")
    comments_path = os.path.join(RAW_DIR, "reddit_comments_raw.csv")

    pd.DataFrame(posts).to_csv(posts_path, index=False)
    pd.DataFrame(comments).to_csv(comments_path, index=False)

    print(f"\nRaw data saved:")
    print(f"  Posts:    {posts_path}  ({len(posts)} rows)")
    print(f"  Comments: {comments_path}  ({len(comments)} rows)")


def build_mentions_csv(posts, comments):
    """
    Combine posts and comments, resolve entity names, filter meaningful rows,
    and produce the final mentions.csv consumed by the NLP pipeline.
    """
    all_rows = posts + comments
    print(f"\nProcessing {len(all_rows)} total raw rows...")

    resolved   = []
    unresolved = []

    for row in tqdm(all_rows, desc="Entity resolution"):
        text = row.get("full_text") or row.get("text") or ""

        # Skip meaningless content
        if not is_meaningful(text):
            continue

        complex_id, confidence = resolve_complex(text)

        if complex_id:
            resolved.append({
                "complex_id":   complex_id,
                "text":         row["text"],
                "title":        row["title"],
                "created_utc":  row["created_utc"],
                "upvotes":      row["upvotes"],
                "url":          row["url"],
                "is_post":      row["is_post"],
                "subreddit":    row["subreddit"],
                "word_count":   len(text.split()),
                "match_conf":   confidence,
                "source":       row["source"],
            })
        else:
            unresolved.append(row)

    # Save mentions.csv
    mentions_path    = os.path.join(OUTPUT_DIR, "mentions.csv")
    unresolved_path  = os.path.join(RAW_DIR, "unresolved_mentions.csv")

    pd.DataFrame(resolved).to_csv(mentions_path, index=False)
    pd.DataFrame(unresolved).to_csv(unresolved_path, index=False)

    return resolved, unresolved


def print_summary(resolved, unresolved):
    """Print a summary of scraping and resolution results."""
    df = pd.DataFrame(resolved)

    print("\n" + "=" * 50)
    print("SCRAPE SUMMARY")
    print("=" * 50)
    print(f"Total resolved mentions:   {len(resolved)}")
    print(f"Total unresolved mentions: {len(unresolved)}")

    if len(resolved) > 0:
        print(f"\nMentions per complex:")
        counts = df["complex_id"].value_counts()
        for complex_id, count in counts.items():
            print(f"  {complex_id:<35} {count}")

        print(f"\nMentions per subreddit:")
        for sub, count in df["subreddit"].value_counts().items():
            print(f"  r/{sub:<30} {count}")

        print(f"\nDate range of data:")
        df["created_utc"] = pd.to_numeric(df["created_utc"], errors="coerce")
        valid = df[df["created_utc"] > 0]
        if len(valid) > 0:
            earliest = datetime.utcfromtimestamp(valid["created_utc"].min())
            latest   = datetime.utcfromtimestamp(valid["created_utc"].max())
            print(f"  Earliest: {earliest.strftime('%Y-%m-%d')}")
            print(f"  Latest:   {latest.strftime('%Y-%m-%d')}")

        print(f"\nAverage upvotes per mention: {df['upvotes'].mean():.1f}")
        print(f"Average word count:          {df['word_count'].mean():.1f}")

    output_path = os.path.join(OUTPUT_DIR, "mentions.csv")
    print(f"\nOutput saved to: {output_path}")
    print("=" * 50)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("IlliniNest Reddit Scraper")
    print("=" * 50)

    # Load .env if present (for PRAW credentials)
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Loaded .env file")
    except ImportError:
        pass

    # Detect working scraper
    scraper_name, scraper = get_working_scraper()

    # Run full scrape
    posts, comments = scrape_all(scraper_name, scraper)

    # Save raw data immediately — do this before anything else
    save_raw(posts, comments)

    # Resolve entities and build mentions.csv
    resolved, unresolved = build_mentions_csv(posts, comments)

    # Print summary report
    print_summary(resolved, unresolved)

    print("\nDone. Share data/outputs/mentions.csv with the team.")
