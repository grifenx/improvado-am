# Channel Attribution models

## Overview

This project implements four different marketing attribution models using ClickHouse SQL. The models included are:

1. **First Touch Attribution**
2. **Last Touch Attribution**
3. **Linear Attribution**
4. **U-Shaped Attribution**

### Directory and Files Description

- **`data-generator.ipynb`**: Jupyter Notebook used to create dummy data.
- **`ma_first_touch.sql`**: First touch attribution sample.
- **`ma_last_touch.sql`**: Last touch attribution sample.
- **`ma_linear.sql`**: Linear attribution sample.
- **`ma_u_shaped.sql`**: U-shaped attribution sample.
- **`marketing_dummy_data.csv`**: Sample marketing data used to test the SQL scripts.

## Known Assumptions and Limitations

### Data
Data generator was designed with following in mind:
- +- 90 - 100k rows, 
- should be similar to event stream of user sessions with URLs (with UTMs)
- for simplicity sake every single URL is supposed to be ad click, which might convert to purchase or not
- should containt different sources, mediums, campaigns, contents, terms (for further usability, even though models use only source from UTMs)
- each event inside session should be consecutive
- session shouldn't be longer than 1 day
- 20% for random purchase within session
- only 1 conversion event within session

### Attribution
- For simplicity's sake attribution window is taken as a single session. More widely used 1, 7 day windows can be assigned by creating window session_id
- 4 most popular models were taken: First touch, Last touch, Linear, U-shaped

### Limitations
- SQL models assume that either your data is already loaded into Clickhouse
- Actual final results of revenue attribution are way too evenly split due to data being randomly generated

### SQL details
- SQL queries were designed to resemble actual dbt layers
- Query formatting follows dbt best practices (excluding comment sections)
- Flavor of choice - Clickhouse SQL

## Model Details

### 1. First Touch Attribution

Attributes 100% of the revenue to the first touchpoint in the customer session.

### 2. Last Touch Attribution

Attributes 100% of the revenue to the last touchpoint in the customer session that led to purchase. Events after purchase are excluded from calculation.

### 3. Linear  Attribution

Attributes the revenue evenly among all events in session. Events after purchase are included in calculation, since our attribution window is limited to a single session, which is rather short.

### 3. U-shaped  Attribution

Attributes the revenue unevenly among events in session: 40% to first touchpoint, 40% to purchase event, 20% split among events in between. Events after purchase are excluded from calculation.
Potential edge cases:
- if session length is single event, then it attributes 100% of revenue
- if session length is 2 events, then revenue is split 50/50
