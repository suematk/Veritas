# DataFrame Comparison Tool : Veritas

## Purpose

Veritas is a Python based tool designed to compare DataFrame summaries between an original database table and a reference summary table provided by the data owner. The script connects to a PostgreSQL database, generates a detailed summary of the DataFrame, and identifies discrepancies between the generated report and the expected comparison report provided by the data owner. The results are then saved as CSV files for further analysis.

By automating the comparison and validation of data, Veritas helps you quickly detect discrepancies, ensuring data integrity across various systems and reports.


## Features

- Database Connectivity: Connects to PostgreSQL databases.
- Generates summary reports for DataFrames including shape, datatype, null counts, non null counts, duplicate counts, unique counts, and most occuring value(first in sorted order if more than one exists).
- Discrepancy Detection: Compares generated DataFrame summaries with the data owner's summary and flags any discrepancies (e.g., missing or extra values).
- CSV Output: Saves reports and discrepancies to CSV files under the reports folder for further analysis or reporting. The logs are also exported to the logs folder to help track and troubleshoot issues if any.

## Installation

1. **Prerequisites:**

   Ensure you have the following Python packages installed:
   - `pandas`
   - `sqlalchemy`
   - `psycopg2`

   You can install these packages using pip:

   ```bash
   pip install pandas sqlalchemy psycopg2
   ```

   Clone this repo into your environment and run the main.py script and enter the credentials based on the prompts to generate the report!

## Usage
1. Clone the repository into your local environment:
```bash
git clone https://github.com/suematk/Veritas.git
```

2. Navigate to the directory and run the main.py script either through the CLI or through your IDE
``` bash
python main.py
```
3. Follow the prompts to input your PostgreSQL database credentials, and the tool will generate the DataFrame comparison report.

## Additional Notes
You can modify the logic to add more comparison logic as needed.

