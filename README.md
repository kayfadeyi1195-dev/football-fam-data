# Football Fam Data Pipeline

A Python-based data pipeline for **Football Fam**, a football player marketplace
focused on English lower league football (Steps 1–6 of the football pyramid:
National League down to Step 6 regional leagues).

## What This Project Does

1. **Collects** player, club, and match data from multiple sources (APIs and web scrapers).
2. **Stages** raw data into a PostgreSQL database.
3. **Transforms** and cleans the data into a consistent format.
4. **Resolves entities** — matches duplicate player/club records across sources.
5. **Serves** the cleaned data via a REST API that powers the Football Fam marketplace.

## Project Structure

```
football-fam-data/
├── README.md                 # You are here
├── .env.example              # Template for environment variables
├── requirements.txt          # Python dependencies
├── alembic.ini               # Database migration configuration
├── alembic/                  # Migration scripts
│   ├── env.py
│   └── versions/
├── src/
│   ├── config.py             # Loads .env settings
│   ├── db/
│   │   ├── models.py         # SQLAlchemy database models
│   │   └── session.py        # Database session factory
│   ├── api_clients/          # API integrations
│   │   ├── api_football.py   # API-Football client
│   │   ├── sportmonks.py     # SportMonks client
│   │   └── football_web_pages.py
│   ├── scrapers/             # Web scrapers
│   │   ├── pitchero.py       # Pitchero club page scraper
│   │   ├── fa_fulltime.py    # FA Full-Time results scraper
│   │   ├── fbref.py          # FBref stats scraper
│   │   └── club_websites.py  # Generic club website scraper
│   ├── etl/                  # Extract-Transform-Load logic
│   │   ├── staging.py        # Raw data staging
│   │   ├── transform.py      # Data cleaning / normalisation
│   │   └── entity_resolution.py  # Deduplication & fuzzy matching
│   ├── seeds/                # Reference / seed data
│   │   ├── pyramid.py        # League & club seed loader
│   │   └── club_directory.json
│   └── api/                  # REST API (FastAPI)
│       └── routes.py
├── scripts/                  # Runnable pipeline entry points
│   ├── run_api_football.py
│   ├── run_pitchero_scraper.py
│   ├── run_entity_resolution.py
│   └── run_all.py
└── tests/
```

## Prerequisites

- **Python 3.10+**
- **PostgreSQL** (running locally or in a container)

## Getting Started

### 1. Clone the repository

```bash
git clone <repo-url>
cd football-fam-data
```

### 2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate   # macOS / Linux
# venv\Scripts\activate    # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up environment variables

Copy the example file and fill in your real values:

```bash
cp .env.example .env
```

Edit `.env` and provide your database URL, API keys, etc.

### 5. Run database migrations

```bash
alembic upgrade head
```

### 6. Seed reference data

```bash
python -m scripts.run_all --seed-only
```

### 7. Run the full pipeline

```bash
python -m scripts.run_all
```

### 8. Start the API server

```bash
uvicorn src.api.routes:app --reload
```

The API will be available at `http://localhost:8000`.
Swagger docs are auto-generated at `http://localhost:8000/docs`.

## Running Individual Pipeline Steps

```bash
# Fetch data from API-Football
python -m scripts.run_api_football

# Scrape Pitchero club pages
python -m scripts.run_pitchero_scraper

# Run entity resolution (deduplication)
python -m scripts.run_entity_resolution
```

## Running Tests

```bash
pytest
```

## Data Sources

| Source | Type | Covers |
|---|---|---|
| API-Football | REST API | Fixtures, standings, squads |
| SportMonks | REST API | Player stats, transfers |
| Football Web Pages | REST API | Results, tables |
| Pitchero | Web scraper | Club pages, rosters |
| FA Full-Time | Web scraper | Grassroots results |
| FBref | Web scraper | Advanced stats |
| Club websites | Web scraper | Squad lists, news |

## License

Private — Football Fam Ltd.
