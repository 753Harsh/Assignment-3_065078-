"""
generate_data.py
-----------------
Generates three sample CSV datasets that mimic real-world event streams
for the SDA-G Streaming Data Analytics project (Media & Entertainment industry):

    1. ott_viewer_events.csv     -> viewer-events topic
    2. social_media_events.csv   -> social-events topic
    3. ad_events.csv             -> ad-events topic

Method: synthetic data generated with the Python `Faker` library plus
weighted `random` choices, so field distributions look realistic
(e.g. more "play" events than "skip" events).

Run:
    python generate_data.py
"""

import csv
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

ROWS_PER_DATASET = 500  # change if your assignment asks for a different row count

CONTENT_TITLES = [
    "Midnight Horizon", "The Last Signal", "Crimson Alley", "Startup Dreams",
    "Ocean's Whisper", "The Vault", "Neon Streets", "Kings of Kutch",
    "Silent Witness", "The Comeback", "Grid City", "Paper Moon",
    "The Great Escape S2", "Coastal Nights", "Rewind", "Echo Chamber",
]
CONTENT_TYPES = ["movie", "series_episode", "documentary", "live_event"]
DEVICES = ["smart_tv", "mobile", "web", "tablet", "gaming_console"]
REGIONS = ["North", "South", "East", "West", "International"]

PLATFORMS = ["Twitter", "Instagram", "Facebook", "TikTok", "YouTube"]
SOCIAL_EVENT_TYPES = ["mention", "comment", "like", "share", "hashtag_use"]
HASHTAGS = ["#MidnightHorizon", "#TheLastSignal", "#StreamingNow", "#MustWatch",
            "#BingeAlert", "#TrendingNow", "#WeekendWatch", "#NewRelease"]
SAMPLE_COMMENTS = [
    "This episode was insane, did not expect that twist!",
    "Not feeling this season, feels slower than the last one.",
    "Been waiting all week for this, worth it.",
    "The soundtrack alone deserves an award.",
    "Kind of a letdown after the hype tbh.",
    "Watching this again this weekend, absolute masterpiece.",
    "Why did they cancel this so soon??",
    "Best thing I've streamed all year.",
]

ADVERTISERS = ["Zenith Motors", "Bright Bank", "Fizzy Cola", "Peak Sportswear",
               "Homeline Furniture", "Quickbite Foods", "TechNova", "Skyline Airlines"]
CAMPAIGNS = ["Summer_Launch", "Festive_Sale", "NewProduct_Push", "Brand_Awareness",
             "Loyalty_Rewards", "Season_Kickoff"]
AD_EVENT_TYPES = ["impression", "click", "skip", "completed"]


def random_timestamp(days_back=7):
    start = datetime.now() - timedelta(days=days_back)
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return (start + delta).strftime("%Y-%m-%d %H:%M:%S")


def generate_ott_events(n):
    rows = []
    for _ in range(n):
        content_title = random.choice(CONTENT_TITLES)
        total_duration = random.randint(1200, 7200)  # 20 min - 2 hrs, in seconds
        event_type = random.choices(
            ["play", "pause", "skip", "complete"], weights=[0.45, 0.2, 0.15, 0.2]
        )[0]
        watch_duration = (
            total_duration if event_type == "complete"
            else random.randint(30, total_duration)
        )
        rows.append({
            "event_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1000, 9999)}",
            "content_id": f"content_{CONTENT_TITLES.index(content_title) + 1:03d}",
            "content_title": content_title,
            "content_type": random.choice(CONTENT_TYPES),
            "device": random.choice(DEVICES),
            "event_type": event_type,
            "watch_duration_sec": watch_duration,
            "total_duration_sec": total_duration,
            "region": random.choice(REGIONS),
            "timestamp": random_timestamp(),
        })
    return rows


def generate_social_events(n):
    rows = []
    for _ in range(n):
        content_title = random.choice(CONTENT_TITLES)
        event_type = random.choices(
            SOCIAL_EVENT_TYPES, weights=[0.2, 0.25, 0.3, 0.15, 0.1]
        )[0]
        rows.append({
            "event_id": str(uuid.uuid4()),
            "platform": random.choice(PLATFORMS),
            "user_handle": fake.user_name(),
            "content_id": f"content_{CONTENT_TITLES.index(content_title) + 1:03d}",
            "content_title": content_title,
            "event_type": event_type,
            "text": random.choice(SAMPLE_COMMENTS) if event_type in ("comment", "mention") else "",
            "hashtag": random.choice(HASHTAGS) if event_type == "hashtag_use" else "",
            "region": random.choice(REGIONS),
            "timestamp": random_timestamp(),
        })
    return rows


def generate_ad_events(n):
    rows = []
    for _ in range(n):
        content_title = random.choice(CONTENT_TITLES)
        event_type = random.choices(
            AD_EVENT_TYPES, weights=[0.55, 0.15, 0.2, 0.1]
        )[0]
        rows.append({
            "event_id": str(uuid.uuid4()),
            "ad_id": f"ad_{random.randint(100, 999)}",
            "advertiser": random.choice(ADVERTISERS),
            "campaign_name": random.choice(CAMPAIGNS),
            "content_id": f"content_{CONTENT_TITLES.index(content_title) + 1:03d}",
            "content_title": content_title,
            "event_type": event_type,
            "device": random.choice(DEVICES),
            "region": random.choice(REGIONS),
            "timestamp": random_timestamp(),
        })
    return rows


def write_csv(filename, rows, fieldnames):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {filename}")


if __name__ == "__main__":
    ott_rows = generate_ott_events(ROWS_PER_DATASET)
    social_rows = generate_social_events(ROWS_PER_DATASET)
    ad_rows = generate_ad_events(ROWS_PER_DATASET)

    write_csv("ott_viewer_events.csv", ott_rows, list(ott_rows[0].keys()))
    write_csv("social_media_events.csv", social_rows, list(social_rows[0].keys()))
    write_csv("ad_events.csv", ad_rows, list(ad_rows[0].keys()))
