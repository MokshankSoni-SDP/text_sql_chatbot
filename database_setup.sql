-- Database setup script for text-to-SQL chatbot
-- Run this script to create the necessary tables

-- Create chat_history table
CREATE TABLE IF NOT EXISTS chat_history (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_chat_history_session ON chat_history(session_id);
CREATE INDEX IF NOT EXISTS idx_chat_history_timestamp ON chat_history(timestamp);

-- Sample footwear_product_master table structure (if you need to create it)
-- Modify this according to your actual requirements
/*
CREATE TABLE IF NOT EXISTS footwear_product_master (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    category VARCHAR(100),
    price NUMERIC(10, 2),
    rating NUMERIC(3, 2),
    launch_year INTEGER,
    color VARCHAR(50),
    size_range VARCHAR(50),
    material VARCHAR(100),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
*/

-- Grant necessary permissions (adjust user as needed)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_user;
-- GRANT INSERT, SELECT, DELETE ON chat_history TO your_user;
