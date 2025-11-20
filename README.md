# 🎬 Movie Score ETL

*A simple, clean ETL pipeline to unify movie data from multiple providers.*

## 🚀 What It Does

* Reads **CSV/JSON** from `data/raw/`
* Normalizes fields into a **canonical schema**
* Merges movies across providers using deterministic rules
* Outputs two files in `data/processed/`:

  * `movies_canonical_YYYY-MM-DD.json`
  * `movies_merged_YYYY-MM-DD.json`

Everything is modular, testable, and easy to extend.

---

## ▶️ Run the Pipeline

### With pipenv (recommended):

```bash
pipenv run python3 -m src.main
```

You’ll see a small banner + step-by-step logs.



## 📥 Input Format

Place provider files under:

```
data/raw/
```

Examples:

```
provider1.csv
provider2.json
provider3_domestic.csv
```

disclaimer: currently code just supports .csv and .json formats.


## 📤 Output Example (merged)

```json
{
      "movie_id": "75d9e8d5",
      "movie_title": "Inception",
      "release_year": 2010,
      "ratings": {
        "critic": {
          "score": 8.7,
          "top_score": 8.1,
          "total_ratings": 450
        },
        "audience": {
          "score": 9.1,
          "total_ratings": 1500000
        }
      },
      "financials": {
        "domestic_box_office_usd": 292576195,
        "worldwide_box_office_usd": 535700000,
        "production_budget_usd": 160000000,
        "marketing_spend_usd": 100000000
      },
      "providers": [
        "provider1",
        "provider2",
        "provider3_domestic",
        "provider3_financials",
        "provider3_international"
      ]
    }
```

## 🔧 Code Style & Formatting

This project uses **Black** as the official code formatter.

Black provides a consistent, opinionated style across the entire codebase.  
To format all Python files:

```
pipenv run black .
```


Here it is, perfectly formatted in Markdown and ready to paste:


## 🧪 Tests

Run all tests:

```bash
pipenv run pytest
```

Quick mode:

```bash
pipenv run pytest -q
```

### 🧪 Testing Overview


✔️ **12+ unit tests** covering happy paths and edge cases of all modules
✔️ **2 integration tests** simulating a full ETL run on sample datasets


## 🧠 Design Decisions

The design of this ETL pipeline follows principles of clarity, modularity, and incremental complexity.  
Below are the key decisions shaping the current implementation:

### 1. Modular ETL Structure
The pipeline is split into logical components (`extract`, `transform`, `merge`, `load`) to keep concerns isolated and enable future extension.  


### 2. Canonical Data Model
A lightweight canonical movie schema is used to normalize heterogeneous provider inputs.  
This ensures downstream consumers always work with predictable fields, regardless of upstream source differences.

### 3. Minimal Dependencies (on Purpose)
The solution avoids heavy frameworks (e.g., pandas, ORM layers) to keep the initial implementation transparent and easy to review.  
This choice also makes deployment and reproducibility easier.

### 4. Deterministic File-Based Ingestion
Raw data is ingested from a simple directory structure (`data/raw/`) to reflect the assignment constraints.  
File iteration is deterministic and predictable, simplifying debugging and reproducibility. There is the option to easily simulate a new batch of data (`data/raw_new/`) it can be easily selected on `main.py` line27 `RAW_FOLDER = os.getenv("RAW_FOLDER", "raw_new")  # default = raw`

### 5. Explicit Run Entry Point (`python -m src.main`)
Using module-based execution avoids path issues and aligns with modern Python packaging.  
This also prepares the module for execution inside Docker or Kubernetes, where explicit entrypoints are required.

### 6. Logging as a First-Class Concern
Structured, readable (and pretty) logs were prioritized to support troubleshooting and story-telling.

### 7. Reproducible Environment via Pipenv
`pipenv` is used to:
* lock dependencies
* ensure reviewers run the exact same environment
* provide a clean, isolated virtual environment
* simplify running commands (pipenv run python -m src.main)

### 8. Git for Version Control
Git keeps the project clean and traceable — every change is stored, branches help isolate features, and reviewers can follow the evolution easily.



## 💡 Notes

* Code is structured for clarity and maintainability.
* Logging is friendly and colorful.
* Easy to extend with new providers or dashboards.


## 🚀 Future Improvements

Several enhancements can help this ETL evolve into a production-grade service:

### 🔁 Deployment & Execution
- Containerize the module with Docker and run it as a Kubernetes Job or CronJob.
- Introduce environment-based configuration for ingestion directories, logging, and output targets.

### 📐 Data Quality & Validation
- Add schema validation using **Pydantic** or SQLAlchemy models.
- Define clearer merge policies (provider priority, confidence scoring, fallback rules).
- Improve error handling and data-quality reporting at row and file level.

### 🧰 Developer Workflow
- Add optional **pre-commit** hooks (Black, Ruff) for consistent formatting and linting.
- Integrate **SonarQube** or a similar static analysis platform to track:
  - technical debt  
  - code smells  
  - maintainability metrics  
  - test coverage  
  - security hotspots

### 📦 Ingestion & Extensibility
- Extend ingestion logic to support additional file layouts (partitioned directories, timestamped folders).
- Add support for cloud-based ingestion paths (S3/GCS).
- Introduce a plugin-style provider architecture for new data sources.

These improvements maintain simplicity while making the pipeline far easier to scale, monitor, and operate in a real production environment.

