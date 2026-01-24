# Quick Start Guide

## 1. Setup Environment

```bash
# Activate virtual environment
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate
```

## 2. Configure Database

1. Create `.env` file from template:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your credentials:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=text_to_sql_chatbot
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_SCHEMA=public
   
   GROQ_API_KEY=your_groq_api_key_here
   GROQ_MODEL=llama-3.3-70b-versatile
   
   CHAT_HISTORY_LIMIT=5
   ```

## 3. Setup Database Tables

```bash
psql -U your_username -d text_to_sql_chatbot -f database_setup.sql
```

## 4. Run the Application

```bash
streamlit run app.py
```

## 5. Use the Chatbot

1. Click "Connect to Database" in the sidebar
2. Review the extracted schema (you can edit it if needed)
3. Start asking questions!

### Example Questions:
- "Show me all products from Nike"
- "What's the average price of shoes?"
- "List the top 5 highest-rated products"
- "How many products are there per brand?"
- "Which shoes were launched in 2023?"
