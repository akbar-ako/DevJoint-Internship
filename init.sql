CREATE TABLE IF NOT EXISTS users_etl (
    user_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    signup_source VARCHAR(50),
    score FLOAT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);