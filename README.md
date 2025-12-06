# NBA Reddit Engagement Pipeline (ELT)

An end-to-end ELT pipeline that ingests Reddit NBA posts, matches text to NBA players using N-gram tokenization, and combines engagement metrics with player game logs for “Hype vs. Performance” analysis.

---

## Architecture

**Flow:** Reddit API + NBA API → Python Ingestion → BigQuery (Raw) → dbt (Staging → Marts)

- **Extract:** Python pulls the last 48 hours of Reddit posts and recent NBA game logs.  
- **Load:** Raw data is appended to BigQuery.  
- **Transform:** dbt cleans, deduplicates, matches players, and builds final fact tables.

---

## Tech Stack

**Ingestion:** Python, PRAW, nba_api,Pandas  
**Warehouse:** Google BigQuery  
**Transformation:** dbt  
**Orchestration:** Local scripts (→ migrating to Airflow)  
**Visualization:** Looker Studio (planned)

---

## Key Features

### **Hype vs. Performance Metrics**
Aggregates Reddit engagement (upvotes + comments) per player and joins it with official NBA game stats. So the online attention a player receives relative to their performance can be analyzed.

---

### **Incremental Ingestion (48h Rolling Window)**
The ingestion script always pulls the last 48 hours of Reddit posts.  
Reason: dbt models run every 24 hours, so the wider window ensures the pipeline captures both new posts and posts whose engagement grows significantly after their creation.

---

### **Scalable Entity Extraction (N-Grams)**
Extracting player names from unstructured text normally requires complex Regex patterns, which scale poorly as the number of players increases.

**Solution:** Generate 2-gram and 3-gram tokens in BigQuery and perform equality joins against the active player roster.

**Benefit:**  
Transforms a slow “text parsing” problem into an efficient SQL hash join.  
This allows the process to scale linearly with the number of posts—independent of roster size.

---

### **Timezone-Safe dbt Models (Targeted Merge Strategy)**

Most dbt models use **insert_overwrite** as the incremental strategy.  
However, for the **player-post matching model**, `insert_overwrite` causes data loss due to ET (NBA games) vs. UTC (Reddit timestamps) date misalignment.

**Problem scenario:**  
- A player plays on Dec 5 (ET).  
- Reddit posts about that game appear on both Dec 5 and Dec 6 (UTC).  
- On the Dec 6 run, the player does *not* appear in the “players who played today” list.  
- `insert_overwrite` overwrites the Dec 6 partition → posts from Dec 6 referencing the Dec 5 game are lost.

**Solution:**  
For this specific model, we use **merge** instead of `insert_overwrite`.  
**Merge** updates new rows while preserving previously matched historical rows, so all valid player–post matches remain intact.

---

## 🔮 Roadmap

- Migrate workflow orchestration to Apache Airflow  
- Build Looker Studio dashboards for “Hype vs. Performance” visualizations  
- Ingest Reddit comment bodies and apply sentiment analysis (NLTK/TextBlob)

---
