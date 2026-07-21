import os
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------
# 5. LOGGING CONFIGURATION (Print yoxdur!)
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)

# Database Connection Info
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "etl_db")

DB_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---------------------------------------------------------
# 1. EXTRACT
# ---------------------------------------------------------
def extract_csv(file_path: str) -> pd.DataFrame:
    logging.info(f"CSV faylı oxunur: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logging.info(f"CSV uğurla oxundu: {len(df)} sətir")
        return df
    except Exception as e:
        logging.error(f"CSV oxunarkən xəta baş verdi: {e}")
        return pd.DataFrame()

def extract_api(api_url: str) -> pd.DataFrame:
    logging.info(f"API-dən data çəkilir: {api_url}")
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logging.info(f"API-dən data çəkildi: {len(df)} sətir")
        return df
    except Exception as e:
        logging.error(f"API sorğusunda xəta: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# 2. TRANSFORM & 6. ERROR HANDLING
# ---------------------------------------------------------
def transform_data(df_api: pd.DataFrame, df_csv: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transform mərhələsi başladı...")
    
    # 6. Xəta İdarəetməsi: CSV-dəki pozulmuş sətirləri təmizləmək
    cleaned_csv_rows = []
    for idx, row in df_csv.iterrows():
        try:
            # Score sütununu float-a çevirməyə çalışırıq
            score = float(row['score'])
            user_id = int(row['user_id'])
            cleaned_csv_rows.append({
                'user_id': user_id,
                'signup_source': str(row['signup_source']),
                'score': score
            })
        except (ValueError, TypeError) as e:
            # Pozulmuş sətir pipeline-ı çökdürmür, loglayıb keçir!
            logging.warning(f"Pozulmuş sətir kənara atıldı (Row {idx}): {row.to_dict()} - Xəta: {e}")

    df_csv_clean = pd.DataFrame(cleaned_csv_rows)

    # API Datasını hazırlamaq
    df_api_clean = df_api[['id', 'name', 'email']].rename(columns={'id': 'user_id'})

    # 2. İki mənbənin ortaq 'user_id' açarı ilə birləşdirilməsi (INNER JOIN)
    merged_df = pd.merge(df_api_clean, df_csv_clean, on='user_id', how='inner')
    
    # Boş (NULL) dəyərləri təmizləmək
    merged_df.dropna(subset=['email', 'score'], inplace=True)
    
    logging.info(f"Transform tamamlandı. Emal olunmuş təmiz sətir sayısı: {len(merged_df)}")
    return merged_df

# ---------------------------------------------------------
# 3. LOAD & 4. IDEMPOTENCY (UPSERT)
# ---------------------------------------------------------
def load_to_postgres(df: pd.DataFrame):
    if df.empty:
        logging.warning("Yüklənməyə data yoxdur.")
        return

    logging.info("PostgreSQL-ə Load mərhələsi başladı...")
    engine = create_engine(DB_URI)

    # 3. Batch Insert və 4. Idempotent Upsert Məntiqi
    upsert_query = text("""
        INSERT INTO users_etl (user_id, name, email, signup_source, score)
        VALUES (:user_id, :name, :email, :signup_source, :score)
        ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            signup_source = EXCLUDED.signup_source,
            score = EXCLUDED.score,
            updated_at = CURRENT_TIMESTAMP;
    """)

    records = df.to_dict(orient='records')

    try:
        with engine.begin() as connection:
            # Batch executemany - Bütün massiv tək əmrlə göndərilir
            connection.execute(upsert_query, records)
        logging.info(f"Uğurlu batch upsert: {len(records)} sətir işləndi.")
    except Exception as e:
        logging.error(f"Bazaya yazarkən xəta: {e}")

# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def run_pipeline():
    logging.info("================ ETL PIPELINE BAŞLADI ================")
    
    # 1. Extract
    df_csv = extract_csv("data/users_extra.csv")
    df_api = extract_api("https://jsonplaceholder.typicode.com/users")
    
    # 2. Transform
    df_transformed = transform_data(df_api, df_csv)
    
    # 3. Load
    load_to_postgres(df_transformed)
    
    logging.info("================ ETL PIPELINE BITDI ================")

if __name__ == "__main__":
    run_pipeline()