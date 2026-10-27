# Data

The data set is **not** stored in Git (about 130 MB). On the first start the producer
downloads it automatically into this folder:

- *Individual Household Electric Power Consumption*, UCI Machine Learning Repository
  (Hebrail & Berard, 2006), https://doi.org/10.24432/C58K54
- 2,075,259 readings, one per minute, 16 Dec 2006 to 26 Nov 2010
- File: `household_power_consumption.txt` (semicolon separated, missing values = `?`)

Manual download (optional): `python scripts/download_data.py`
