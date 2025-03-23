import os
import random 
import string
import pandas as pd 
from datetime import datetime
from utils.logging_config import logger

def id_generator(size: int=6, chars: str=string.ascii_uppercase + string.digits) -> str:
    '''
    Generates a random string of a specified size to use as a suffix for file names
    when a file with a timestamp prefix already exists.

    Parameters:
    size (int): The length of the generated string (default is 6).
    chars (str): The characters from which the string will be generated (default is uppercase ASCII letters and digits).

    Returns:
    str: A randomly generated string of the specified size.
    '''

    return ''.join(random.choice(chars) for _ in range(size))

def export_report_to_csv(report_df: pd.DataFrame) -> int:
    '''
    Exports the generated report as a CSV file with a unique filename.
    It attempts to generate a unique filename with up to 3 retries if a file with the same name already exists.

    Parameters:
    report_df (pd.DataFrame): A Pandas DataFrame containing the generated report.

    Returns:
    int: 1 if the export is successful, 0 if the export fails.
    '''

    max_retries = 3
    filename = 'Report_' + (datetime.now().strftime("%Y%m%d_%H%M%S")) + '.csv'
    logger.info(f'Checking if filename already exists')
    try:
        report_df.to_csv(f'./reports/{filename}', mode='x', index=False)
        logger.info(f'The report was successfully generated. Filename is {filename}.')
        print(f'The report was successfully generated. Filename is {filename}.')
        return 1
    except FileExistsError:
        while max_retries>0:
            filename = 'Report_' + id_generator() + '.csv'
            if os.path.isfile(filename)==True:
                logger.error(f'A file with the same name already exists. Retrying...')
                max_retries -= 1
            else:
                logger.info(f'The report was successfully generated. Filename is {filename}.')
                print(f'The report was successfully generated. Filename is {filename}.')
                report_df.to_csv(f'./reports/{filename}', mode='x', index=False)
                return 1
    except Exception as e:
        logger.error(f'An unknown error occured. Please retry later. {e}')
        print('Export failed due to an issue. Please check the logs and retry later!')
        return 0
    logger.error('Max retry attempt reached. Please try generating the report later.')
    print('Max retry attempt reached. Please try generating the report later.')
    return 0