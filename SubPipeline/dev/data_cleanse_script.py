import sqlite3
import pandas as pd
import ast
import numpy as np
from sqlalchemy import create_engine
import logging

pd.options.mode.chained_assignment = None

logging.basicConfig(filename="./SubPipeline/dev/cleanse_db.log",
                    format="%(asctime)s - %(name)s -%(levelname)s - %(message)s",
                    filemode='w',
                    level=logging.DEBUG,
                    force=True)
logger = logging.getLogger(__name__)

def cleanse_student_table(df):
    """
    Cleanse the `cademycode_students` table based on the discoveries from the jupyter notebook
    
    Parameters:
        df: `student` table from `cademycode.db`
    
    Returns:
        df: cleaned version of the input table
        missing_data: incomplete data that was removed from the original table
        
    """
    now = pd.to_datetime.now()
    df['age'] = (now - pd.to_datetime(df['dob'])).dt.days // 365
    df['age_group'] = (df['age'] // 10)*10
    
    df['contact_info'] = df['contact_info'].apply(lambda x: ast.literal_eval(x))
    explode_contact_info = pd.json_normalize(df['contact_info'])
    df = pd.concat([df.drop('contact_info', axis=1).reset_index(drop=True), explode_contact_info], axis=1)
    
    split_address = df.mailing_address.str.split('', expand=True)
    split_address.columns = ['street', 'city', 'state', 'zip_code']
    df = pd.concat([df.drop('mailing_address', axis=1), split_address], axis=1)
    
    df['job_id'] = df['job_id'].astype(float)
    df['current_career_path_id'] = df['current_career_path_id'].astype(float)
    df['num_course_taken'] = df['num_course_taken'].astype(float)
    df['time_spent_hrs'] = df['time_spent_hrs'].astype(float)
    
    missing_data = pd.DataFrame()
    missing_course_taken = df[df[['num_course_taken']].isnull().any(axis=1)]
    missing_data = pd.concat([missing_data, missing_course_taken])
    df = df.dropna(subset=['num_course_taken'])
    
    missing_job_id = df[df[['job_id']].isnull().any(axis=1)]
    missing_data = pd.concat([missing_data, missing_job_id])
    df = df.dropna(subset=['job_id'])
    
    df['current_career_path_id'] = np.where(df['current_career_path_id'].isnull(), 0, df['current_career_path_id'])
    df['time_spent_hrs'] = np.where(df['time_spent_hrs'].isnull(), 0, df['time_spent_hrs'])
    
    return (df, missing_data)

def cleanse_career_path(df):
    """
    Cleanse the `cadenycode_courses` table based on the discoveries from the jupyter notebook
    
    Parameters:
        df: `cademycode_courses` table from `cademycode.db`
        
    Returns:
        df: cleaned version of the input table
    """
    
    not_applicable = {'career_path_id': 0,
                      'career_path_name': 'not applicable',
                      'hours_to_complete': 0}
    
    df.loc[len(df)] = not_applicable
    return(df.drop_duplicates())

def cleanse_student_jobs(df):
    """
    Cleanse the `cademycode_student_jobs` table based on the discoveries from the jupyter notebook
    
    Parameters:
        df: `cademycode_student_jobs` table from `cademycode.db`
        
    Returns:
        df: cleaned version of the input table
    """
    return(df.drop_duplicates())

def test_for_path_id(students, career_paths):
    """
    Unit test to ensure that join keys exist between the students and courses tables

    Parameters:
        students: `cademycode_student_jobs` table from `cademycode.db`
        career_paths: `cademycode_courses` table from `cademycode.db`

    Returns:
        None
    """
    
    student_table = students.curreent_career_path_id.unique()
    is_subset = np.isin(student_table, career_paths.career_path_id.unique())
    missing_id = student_table[~is_subset]
    try:
        assert len(missing_id) == 0, "Missing career_path_id(s): " + str(list(missing_id)) + " in `career_paths` table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('All career_path_ids are present.')
        
def test_for_job_id(students, student_jobs):
    """
    Unit test to ensure that join keys exist between the students and student_jobs tables

    Parameters:
        students: `cademycode_student_jobs` table from `cademycode.db`
        student_jobs: `cademycode_student_jobs` table from `cademycode.db`

    Returns:
        None
    """
    
    student_table = students.job_id.unique()
    is_subset = np.isin(student_table, student_jobs.job_id.unique())
    missing_id = student_table[~is_subset]
    try:
        assert len(missing_id) == 0, "Missing job_id(s): " + str(list(missing_id)) + " in `student_jobs` table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('All job_ids are present.')
        
def test_nulls(df):
    """
    Unit test to ensure that no rows in the cleaned table are null

    Parameters:
        df: DataFrame of the cleansed table

    Returns:
        None
    """
    
    df_missing = df[df.isnull().any(axis=1)]
    cnt_missing = len(df_missing)
    
    try:
        assert cnt_missing == 0, "There are " + str(cnt_missing) + " nulls in the table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('No null rows found.')
        
def test_num_cols(local_df, db_df):
    """
    Unit test to ensure that the number of columns in the cleaned DataFrame match the
    number of columns in the SQLite3 database table.

    Parameters:
        local_df: DataFrame of the cleansed table
        db_df: `cademycode_aggregated` table from `cademycode_cleansed.db`

    Returns:
        None
    """
    try:
        assert len(local_df.columns) == len(db_df.columns)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('Number of columns are the same.')
        
def test_schema(local_df, db_df):
    """
    Unit test to ensure that the column dtypes in the cleaned DataFrame match the
    column dtypes in the SQLite3 database table.

    Parameters:
        local_df: DataFrame of the cleansed table
        db_df: `cademycode_aggregated` table from `cademycode_cleansed.db`

    Returns:
        None
    """
    errors = 0
    for col in db_df:
        try:
            if local_df[col].dtypes != db_df[col].dtypes:
                errors+=1
        except NameError as ne:
            logger.exception(ne)
            raise ne
        
    if errors > 0:
        assert_err_msg = str(errors) + " column(s) dtypes aren't the same"
        logger.exception(assert_err_msg)
    assert errors == 0, assert_err_msg
    
def main():
    
    logger.info("Start Log")
    
    con = sqlite3.connect('./SubPipeline/dev/cademycode.db')
    students = pd.read_sql_query("SELECT * FROM cademycode students", con)
    career_paths = pd.read_sql_query("SELECT * FROM cademycode_courses", con)
    student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
    con.close()
    
    try:
        con = sqlite3.connect("./SubPipeline/dev/cademycode_cleansed.db")
        clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", con)
        missing_db = pd.read_sql_query("SELECT * FROM incomplete_data", con)
        con.close()
        
        new_students = students[~np.isin(students.uuid.unique(), clean_db.uuid.unique())]
    except:
        new_students = students
        clean_db = []
        
    clean_new_students, missing_data = cleanse_student_table(new_students)
    
    try:
        new_missing_data = missing_data[~np.isin(missing_data.uuid.unique(), missing_db.uuid.unique())]
    except:
        new_missing_data = missing_data
        
    if len(clean_new_students) > 0:
        clean_career_paths = cleanse_career_path(career_paths)
        clean_student_jobs = cleanse_student_jobs(student_jobs)
        
        test_for_job_id(clean_new_students, clean_student_jobs)
        test_for_path_id(clean_new_students, clean_career_paths)
        
        clean_new_students['job_id'] = clean_new_students['job_id'].astype(int)
        clean_new_students['current_career_path_id'] = clean_new_students['current_career_path_id'].astype(int)
        
        df_clean = clean_new_students.merge(clean_career_paths, left_on='current_career_path_id', right_on='career_path_id', how='left')
        df_clean = df_clean.merge(clean_student_jobs, on='job_id', how='left')