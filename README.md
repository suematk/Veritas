# DataFrame Comparison Tool : Veritas

## Purpose

This Python script is designed to facilitate the comparison of DataFrame summaries between an original database table and a original summary table from the data owner. The script connects to a PostgreSQL database, generates summary reports of the DataFrame and identifies discrepancies between the generated report and the expected comparison report. The results are saved as CSV files for further analysis.

Automating the process of comparing and validating data, helps organizations quickly identify discrepancies and maintain data integrity across various systems and reports.


## Features

- Connects to PostgreSQL databases.
- Generates summary reports for DataFrames including shape, datatype, null counts, non null counts, duplicate counts, unique counts, and most occuring value(first in sorted order if more than one exists).
- Discrepancy Detection: Compares generated DataFrame summaries with the data owner's summary and flags any discrepancies (e.g., missing or extra values).
- Saves reports and discrepancies to CSV files for further analysis or reporting..

## Installation

1. **Prerequisites:**

   Ensure you have the following Python packages installed:
   - `pandas`
   - `sqlalchemy`
   - `psycopg2`

   You can install these packages using pip:

   ```bash
   pip install pandas sqlalchemy psycopg2

   Clone this repo into your environment and run the main.py script and enter the credentials based on the prompts.

## License

**Note:** This code is provided for personal or internal use only. Redistribution, modification, or commercial use of this code is not permitted. Unauthorized use of this code is prohibited.
