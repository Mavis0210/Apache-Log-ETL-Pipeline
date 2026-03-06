# Apache Server Log ETL Pipeline (Python \& SQL)


### Project Overview

As a Technical Operations Engineer pivoting to Data Engineering, I built this Python pipeline to automate the analysis of raw server logs. The goal was to replace manual log inspection with an automated script that extracts key metrics like traffic volume and error rates.


### The Problem

Raw server logs are messy, unstructured text files. Analyzing them manually to find security threats (DDoS attacks) or operational failures (404 errors) is impossible at scale.


### The Solution

I developed a Python script that:

1. **Extracts:** Fetches raw log data from a remote URL using requests.
2. **Parses:** Cleaned unstructured text data by splitting log lines into structured fields (IP, Date, Endpoint, Status).
3. **Transforms:**  Used Pandas to convert status codes to integers and handle missing values.
4. **Loads:** Saved the structured data into a local SQLite database (`production_logs.db`) for querying.
5. **Analyzes:** Generated charts using Seaborn to identify critical system errors (404/500 status codes).

### Tech Stack

* Python 3.x
* Pandas (Data Manipulation)
* SQLAlchemy (Database ORM)
* Requests (HTTP Library)
* Matplotlib/Seaborn (Visualization)


### Key Insights
* Analyzed 10,000 log entries in seconds.
* Identified the top 5 IP addresses (potential bot traffic).
Visualized the distribution of critical errors to prioritize fixes.

### How to Run
1. Clone the repository
2. Install dependencies: `pip install pandas requests sqlalchemy matplotlib seaborn`
3. Run: `server_log_pipeline.py`
