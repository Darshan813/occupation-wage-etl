from logging import config
import os
from dataclasses import dataclass  
import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine, text
import re

# Configuration dataclass
@dataclass
class PipelineConfig:
    url: str = "https://data.bls.gov/oes/#/area/0100000"
    output_html_path: str = "bls_oes_area_0100000.html"
    db_connection_string: str = "postgresql+psycopg2://admin:admin@postgres:5432/oews_net"
    skills_excel_path: str = "Skills.xlsx"
    selenium_wait_time: int = 30
    chunksize: int = 10_000

# Selenium handler class
class SeleniumHandler:
    def __init__(self, config: PipelineConfig):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.url = config.url
        self.wait_time = config.selenium_wait_time

    # Fetch page and return HTML content
    def fetch_page(self) -> str:
        try:
            self.driver.get(self.url)
            time.sleep(self.wait_time)
            print(f"Saved page source ({len(self.driver.page_source)} chars)")
            return self.driver.page_source
        except Exception as e:
            print(f"Error fetching page: {e}")
            return ""
        finally:
            self.driver.quit()

# Data extraction class
class DataExtractor:
    def __init__(self, html: str):
        self.soup = BeautifulSoup(html, "html.parser")

    # Extract the target table from the HTML
    def extract_table(self) -> pd.DataFrame:
        table = None
        table_list = self.soup.find_all('table')
        for t in table_list:
            thead = t.find('thead')
            if thead and thead.find('tr').find('th').get_text(strip=True) == 'Occupation (SOC code)':
                table = t
                print("Target table found.")
                break

        if not table:
            raise ValueError("Target table not found in HTML.")

        columns = [th.get_text(strip=True) for th in table.find('thead').find_all('th')]
        all_rows = []

        # Extract rows
        for row in table.find('tbody').find_all('tr'):
            row_data = []
            th = row.find('th')
            if th:
                row_data.append(th.get_text(strip=True))
            for td in row.find_all('td'):
                text = td.get_text(strip=True)
                row_data.append(text)
            all_rows.append(row_data)
        all_rows = all_rows[:-2]

        if len(all_rows) == 0:
            raise ValueError("No data rows found in the table.")

        return pd.DataFrame(all_rows, columns=columns)

# Data cleaning class
class Cleaner:
    def __init__(self, df: pd.DataFrame):
        self.df = df    
    
    # Clean column names
    def clean_columns(self):
        self.df.columns = (
                            self.df.columns
                            .map(lambda col: col.split("(")[0].strip() if 'occupation' not in col.lower() else col)    
                            .str.replace(" ", "_")
                            .str.replace(",", "")
                            .str.replace("(", "")
                            .str.replace(")", "")
                            .str.replace("standard_error", "std_error")
                            .str.lower()
                            .str.strip()
                        )
        
    # Split occupation and SOC code into separate columns
    def split_occupation_soc(self):
        self.df['occupation'] = self.df['occupation_soc_code'].str.split('(').str[0].str.strip()
        self.df['soc_code'] = self.df['occupation_soc_code'].str.split('(').str[1].str.replace(')', '').str.strip()
        self.df = self.df.drop(columns=['occupation_soc_code'])
    
    # Clean individual data entries
    def clean_data(self, x):
        try:
            if pd.isna(x) or re.fullmatch(r"\(\d+\)-", x):
                return pd.NA
            if ')' in x:
                x = x.split(')', 1)
                if len(x) > 1:
                    x = x[1].strip()
            x = x.replace('$', '').replace(',', '')
            return x
        except Exception as e:
            print(f"Error processing value: {x} - {e}")

    # Convert data types of columns
    def clean_data_types(self):
        integer_cols = ['employment', 'annual_mean_wage', 'annual_10th_percentile_wage', 'annual_25th_percentile_wage', 'annual_median_wage', 'annual_75th_percentile_wage', 'annual_90th_percentile_wage']
        float_columns = [c for c in self.df.columns if c not in integer_cols and c not in ['occupation', 'soc_code']]
        for col in integer_cols:
            self.df[col] = self.df[col].astype('Int64')
        for col in float_columns:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

    # Get the cleaned DataFrame
    def get_cleaned_df(self) -> pd.DataFrame:
        self.clean_columns()
        self.split_occupation_soc()
        self.df = self.df.applymap(self.clean_data)
        self.clean_data_types()
        self.df = self.df.where(pd.notna(self.df), None)
        first = ["soc_code", "occupation"]
        rest = [c for c in self.df.columns if c not in first]
        self.df = self.df[first + rest]
        return self.df

# Database handler class
class DatabaseHandler:
    def __init__(self, config: PipelineConfig):
        self.engine = create_engine(config.db_connection_string)
    
    # Create tables if they do not exist
    def create_tables(self, table_name: str):
        with self.engine.connect() as conn:
            if table_name == "oews_by_state":
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS public.oews_by_state (
                        soc_code                        TEXT,
                        occupation                      TEXT,
                        employment                      BIGINT,
                        employment_percent_relative_std_error          NUMERIC(6,2),
                        hourly_mean_wage                NUMERIC(10,2),
                        annual_mean_wage                BIGINT,
                        wage_percent_relative_std_error                NUMERIC(6,2),
                        hourly_10th_percentile_wage     NUMERIC(10,2),
                        hourly_25th_percentile_wage     NUMERIC(10,2),
                        hourly_median_wage              NUMERIC(10,2),
                        hourly_75th_percentile_wage     NUMERIC(10,2),
                        hourly_90th_percentile_wage     NUMERIC(10,2),
                        annual_10th_percentile_wage     BIGINT,
                        annual_25th_percentile_wage     BIGINT,
                        annual_median_wage              BIGINT,
                        annual_75th_percentile_wage     BIGINT,
                        annual_90th_percentile_wage     BIGINT,
                        employment_per_1000_jobs        NUMERIC(10,3),
                        location_quotient               NUMERIC(10,3)
                    );
                """))
                conn.commit()
                print("Table oews_by_state created or already exists.")

            elif table_name == "onet_skills":
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS public.onet_skills (
                        onet_soc_code      TEXT        NOT NULL,
                        title              TEXT,
                        element_id         TEXT        NOT NULL,
                        element_name       TEXT,
                        scale_id           TEXT        NOT NULL,
                        scale_name         TEXT,
                        data_value         NUMERIC(18,6),
                        n                  BIGINT,
                        standard_error     NUMERIC(18,6),
                        lower_ci_bound     NUMERIC(18,6),
                        upper_ci_bound     NUMERIC(18,6),
                        recommend_suppress TEXT,        
                        not_relevant       TEXT,       
                        "date"             DATE       NOT NULL,
                        domain_source      TEXT
                    );
                """))
                conn.commit()
                print("Table onet_skills created or already exists.")
                
    # Save DataFrame to database
    def save_to_db(self, df: pd.DataFrame, table_name: str):
        try:
            self.create_tables(table_name)
            df.to_sql(table_name, con = 'postgresql+psycopg2://admin:admin@postgres:5432/oews_net', if_exists='append', index=False)
            print(f'Successfully saved {len(df)} records to {table_name}.')
            print("-" * 50)
        except Exception as e:
            print(f"Error saving to database: {e}")

# Skills data loader class
class SkillsDataLoader:
    def __init__(self, config: PipelineConfig, onet_skills_df: pd.DataFrame):
        self.config = config
        self.onet_skills_df = onet_skills_df

    # Load skills data from Excel
    def load_skills_data(self) -> pd.DataFrame:
        try:
            self.onet_skills_df = self.onet_skills_df.where(pd.notna(self.onet_skills_df), None)
            self.onet_skills_df.columns = (self.onet_skills_df.columns
                                        .str.strip()
                                        .str.replace(' ', '_')
                                        .str.replace('-', '_')
                                        .str.replace('*', '')
                                        .str.lower()
                                    )
            self.onet_skills_df['date'] = pd.to_datetime(self.onet_skills_df["date"], format="%m/%Y", errors="coerce")
            self.onet_skills_df = self.onet_skills_df.where(pd.notna(self.onet_skills_df), None)
            return self.onet_skills_df

        except Exception as e:
            print(f"Error loading skills data: {e}")
            return pd.DataFrame()

class datetime:
    @staticmethod
    def normalize():
        return pd.to_datetime("today").normalize()
    
def get_clean_date():
    today_date = datetime.normalize()
    return today_date.strftime("%Y-%m-%d")

def create_folders(folder_name:str):
    clean_date = get_clean_date()
    raw_folder = os.path.join(f"{folder_name}/{clean_date}/raw")
    os.makedirs(raw_folder, exist_ok=True)
    parquet_path = os.path.join(raw_folder, f"{folder_name}_raw.parquet")
    return parquet_path

def extract_oews_data():
    config = PipelineConfig()
    selenium_handler = SeleniumHandler(config)
    html = selenium_handler.fetch_page()
    extractor = DataExtractor(html)
    oews_df = extractor.extract_table()
    raw_parquet_path = create_folders("oews_raw")
    oews_df.to_parquet(raw_parquet_path, index=False)

def transform_oews_data() -> pd.DataFrame:
    clean_date = get_clean_date()
    oews_df = pd.read_parquet(f"oews_raw/{clean_date}/oews_raw.parquet")
    cleaner = Cleaner(oews_df)
    cleaned_df = cleaner.get_cleaned_df()
    cleaned_parquet_path = create_folders("oews_cleaned")
    cleaned_df.to_parquet(cleaned_parquet_path, index=False)

def load_oews_data():
    clean_date = get_clean_date()
    config = PipelineConfig()
    db_handler = DatabaseHandler(config)
    oews_df = pd.read_parquet(f"oews_cleaned/{clean_date}/oews_cleaned.parquet")
    db_handler.save_to_db(oews_df, "oews_by_state")

def extract_onet_skills_data():
    config = PipelineConfig()
    onet_skills_df = pd.read_excel(config.skills_excel_path, engine="openpyxl")
    raw_parquet_path = create_folders("onet_skills_raw")
    onet_skills_df.to_parquet(raw_parquet_path, index=False)

def transform_onet_skills_data():
    clean_date = get_clean_date()
    onet_skills_df = pd.read_parquet(f"onet_skills_raw/{clean_date}/onet_skills.parquet")
    skills_loader = SkillsDataLoader(config, onet_skills_df)
    onet_skills_df = skills_loader.load_skills_data("onet_skills")
    cleaned_parquet_path = create_folders("onet_skills_cleaned")
    onet_skills_df.to_parquet(cleaned_parquet_path, index=False)

def load_onet_skills_data():
    clean_date = get_clean_date()
    config = PipelineConfig()
    db_handler = DatabaseHandler(config)
    onet_skills_df = pd.read_parquet(f"onet_skills_cleaned/{clean_date}/onet_skills_cleaned.parquet")
    db_handler.save_to_db(onet_skills_df, "onet_skills")

def main():

    import time

    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"Pipeline started at {start_time}")
    print("-" * 50)

    config = PipelineConfig()
    
    # Step 1: Fetch HTML using Selenium
    print("Fetching HTML content...")
    selenium_handler = SeleniumHandler(config)
    html = selenium_handler.fetch_page()
    
    if not html:
        print("Failed to fetch HTML content.")
        return
    
    print("HTML content fetched successfully.")
    print("-" * 50)
    
    # Step 2: Extract data from HTML
    print("Extracting data from HTML...")   
    extractor = DataExtractor(html)
    oews_df = extractor.extract_table()
    print(f"Extracted {len(oews_df)} rows successfully.")
    print("-" * 50)

    # Step 3: Clean the data
    print("Cleaning data...")
    cleaner = Cleaner(oews_df)
    oews_df = cleaner.get_cleaned_df()
    print("Data cleaned successfully.")
    print("-" * 50)

    # Step 4: Load skills data from Excel
    print("Loading skills data from Excel...")
    skills_loader = SkillsDataLoader(config)
    onet_skills_df = skills_loader.load_skills_data("onet_skills")
    print(f"Loaded {len(onet_skills_df)} rows of skills data successfully.")
    print("-" * 50)

    # Step 5: Save data to database
    print("Saving data to database...")
    db_handler = DatabaseHandler(config)
    db_handler.save_to_db(oews_df, "oews_by_state")
    db_handler.save_to_db(onet_skills_df, "onet_skills")
    print("Data saved to database successfully.")
    print("-" * 50)

    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"Pipeline completed at {end_time}")
   

if __name__ == "__main__":
    extract_onet_skills_data()
    print("-" * 50)
    transform_onet_skills_data()
    print("-" * 50) 
    load_onet_skills_data()
    print("-" * 50)
