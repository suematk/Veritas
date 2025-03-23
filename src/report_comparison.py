import pandas as pd
from utils.logging_config import logger

def compare_columns(user_cols: list, owner_cols: list, match_for: str):

    '''
    Helper function to compare columns and parameters betweent the generated report and the data owner's report to return common, missing and extra values.

    Parameters:
    user_cols (list): List containing parameters or columns from the generated report.
    owner_cols (list): List containing the parameters or columns from the data owner's report.
    match_for (string): 'params' implies the columns of the reports are being compared.
                        'columns' implies the values of the field named Column, i.e. the columns of the tables are being compared.

    Returns:
    Optional[Tuple[List[str], List[str], List[str]]]: 
        - common: List of common values.
        - missing: List of values missing from the user's report.
        - extra: List of values missing from the owner's report.
    Returns None if an unexpected match_for value is provided.
    '''

    user_cols.sort()
    owner_cols.sort()
    
    match_for = match_for.strip().lower()

    if match_for=='params':
        # Matching parameters
        if user_cols!=owner_cols and len(user_cols)>len(owner_cols):
            logger.warning(f'Parameters do not match. Extra parameters found! {set(user_cols)-set(owner_cols)}. Generating report for common parameters.')
        elif user_cols!=owner_cols and len(user_cols)<len(owner_cols):
            logger.warning(f'Parameters do not match. Fewer parameters found! {set(owner_cols)-set(user_cols)}. Generating report for common parameters.')
        elif user_cols!=owner_cols:
            logger.warning(f'Parameters do not match! Missing parameters column(s):{set(owner_cols)-set(user_cols)} Extra parameters column(s):{set(user_cols)-set(owner_cols)}. Generating report for common parameters.')
        else:
            logger.info(f'All parameters found.')
    elif match_for=='columns':
            if user_cols!=owner_cols and len(user_cols)>len(owner_cols):
                logger.warning(f'Columns do not match. Extra columns found! {set(user_cols)-set(owner_cols)}. Generating report for common columns.')
            elif user_cols!=owner_cols and len(user_cols)<len(owner_cols):
                logger.warning(f'Columns do not match. Fewer columns found! {set(owner_cols)-set(user_cols)}. Generating report for common columns.')
            elif user_cols!=owner_cols:
                logger.warning(f'Columns do not match! Missing column(s):{set(owner_cols)-set(user_cols)} Extra column(s):{set(user_cols)-set(owner_cols)}. Generating report for common columns.')
            else:
                logger.info(f'All columns found.')
    else:
        logger.error("Unexpected value encountered! Aborting")
        return None

    common = list(set(user_cols).intersection(set(owner_cols)))
    missing = list(set(owner_cols).difference(set(user_cols)))
    extra = list(set(user_cols).difference(set(owner_cols)))

    return common, missing, extra


def compare_dataframes(generated_report:pd.DataFrame, original_report:pd.DataFrame) -> pd.DataFrame:

    '''
    Function to compare user generated report and the data owner's report, flag mismatches if any and returns it.

    Parameters:
    generated_report (pd.DataFrame): Pandas DataFrame containing the report generated from the user's table.
    original_report (pd.DataFrame): Pandas DataFrame containing the extracted data owner's report.

    Returns:
    combined (pd.DataFrame): Pandas DataFrame with mismatches flagged after comparing user generated report with the data owner's report.
    Returns None if any error is encountered.
    '''

    owner_cols = original_report.columns.tolist()
    user_cols = generated_report.columns.tolist()

    result = compare_columns(user_cols,owner_cols,'params')

    if result is None:
        logger.error("Error: Report comparison failed due to invalid parameters or missing columns.")
        print("Aborting report generation.")
        return None
    else:
        common_params, missing_params, extra_params = result
        logger.info(f'Common params: {common_params}, Missing params: {missing_params}, Extra params: {extra_params}')

    if 'Column' not in common_params:
        logger.warning(f'Field with columns not found in {common_params}. Aborting...')
        print('Field with columns not found. Aborting...')
        return None
    else:
        original_report = original_report[common_params]
        generated_report = generated_report[common_params]

    # Matching columns
    owner_cols = list(original_report['Column'])
    user_cols = list(generated_report['Column'])
    
    result = compare_columns(user_cols, owner_cols, 'columns')

    if result is None:
        logger.error("Error: Report comparison failed due to invalid parameters or missing columns.")
        print("Aborting report generation.")
        return None
    else:
        common_cols, missing_cols, extra_cols = result
        logger.info(f'Common cols: {common_cols}, Missing cols: {missing_cols}, Extra cols: {extra_cols}')

    if len(common_cols)==0:
        logger.warning(f'No common columns found!')
    else:
        original_report = original_report[original_report['Column'].isin(common_cols)]
        generated_report = generated_report[generated_report['Column'].isin(common_cols)]
    
    logger.info(f'Checking for mismatches if any from common columns {common_cols}')
    for col in common_params:
        original_report[col] = original_report[col].astype(str)
        generated_report[col] = generated_report[col].astype(str)

        mask = original_report[col] != generated_report[col]

        original_report.loc[mask, col] = 'MISMATCH' 
    combined = original_report.copy()

    if len(missing_cols)>0:
        logger.info("Adding missing columns in the report if any")
        new_row = pd.DataFrame({'Column': missing_cols})
        combined = pd.concat([combined, new_row], ignore_index=True)
        combined = combined.fillna('MISSING COLUMN')

    if len(extra_cols)>0:
        logger.info("Adding extra columns to the report if any")
        new_row = pd.DataFrame({'Column': extra_cols})
        combined = pd.concat([combined, new_row], ignore_index=True)
        combined = combined.fillna('EXTRA COLUMN')          

    logger.info("Adding missing parameters in the report if any")
    for param in missing_params:
        combined[param] = 'MISSING PARAMETER'

    logger.info("Adding extra parameters in the report if any")
    for param in extra_params:
        combined[param] = 'EXTRA PARAMETER'
        
    report_cols = ['Column'] + [x for x in combined.columns.tolist() if x!='Column']
    combined = combined[report_cols]
    logger.info(f'Report generation complete! Shape {combined.shape}')
    
    return combined
