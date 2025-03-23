from utils.logging_config import logger 
from src.get_input_config import input_configuration
from src.generate_report import dataframe_summary
from src.get_owners_report import get_owner_report
from src.report_comparison import compare_dataframes
from src.export_report import export_report_to_csv

def main():
    logger.info('FETCHING USER TABLE DETAILS')
    res = input_configuration(table_owner='user')
    if res is None:
        logger.error("Failed to retrieve user table details. Exiting...")
        exit(1)  
    else:
        conn, table, db = res
        print('_______________________________________________')

    logger.info('GENERATING USER REPORT')
    user_report = dataframe_summary(conn, table, db)
    if user_report is None:
        logger.error("USER REPORT GENERATION FAILED. EXITING...")
        exit(1)
    logger.info(f'USER REPORT GENERATED SUCCESFULLY! {user_report.shape}')

    logger.info('GETTING OWNER\'S CONNECTION DETAILS')
    res = input_configuration(table_owner='owner')
    if res is None:
        logger.error("Failed to retrieve owner's table details. Exiting...")
        exit(1)  
    else:
        conn, table, db = res
        print('_______________________________________________')


    logger.info("FETCHING OWNER'S REPORT")
    owner_report = get_owner_report(conn, db, table)
    if owner_report is None:
        logger.error("FAILED TO FETCH OWNER'S REPORT. EXITING...")
        exit(1)
    logger.info(f"OWNER'S REPORT RETRIEVED SUCCESSFULLY! {owner_report.shape}")
    print('_______________________________________________')

    logger.info(f"COMPARING THE REPORTS {user_report.shape}, {owner_report.shape}")
    report = compare_dataframes(user_report, owner_report)
    if report is None:
        logger.error("REPORT COMPARISON FAILED. EXITING...")
        exit(1)
        
    logger.info("EXPORTING REPORT")
    res = export_report_to_csv(report)
    if res==1:
        logger.info('EXPORTED REPORT SUCCESSFULLY!')
    else:
        logger.info(f'EXPORT FAILED. PLEASE TRY AGAIN LATER!')

if __name__=="__main__":
    main()