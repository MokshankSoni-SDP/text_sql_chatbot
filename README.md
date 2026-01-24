# Text-to-SQL Chatbot

A natural language to SQL query chatbot powered by Groq LLaMA and PostgreSQL.

## Architecture Overview

This chatbot allows users to query a PostgreSQL database using natural language. The system:
1. Extracts database schema dynamically
2. Uses chat history for context
3. Generates SQL queries via Groq LLaMA
4. Validates and executes queries safely
5. Returns results in natural language

### Why No Embeddings?

This implementation deliberately avoids embeddings and vector similarity search because:
- **Simplicity**: Direct schema injection is simpler and more transparent
- **Accuracy**: Complete schema context prevents hallucination
- **Maintenance**: No need to maintain vector databases or embedding pipelines
- **Cost**: Reduced computational overhead

## Features

- ✅ Natural language to SQL conversion
- ✅ Chat history context (last 5 messages)
- ✅ Dynamic schema extraction
- ✅ SQL validation (SELECT-only, no dangerous operations)
- ✅ Error handling and user-friendly messages
- ✅ Streamlit-based chat interface

## Setup

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Groq API key

### Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd text_to_sql
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials and Groq API key
   ```

### Database Setup

Ensure your PostgreSQL database has:
- Database: `text_to_sql_chatbot`
- Schema: `public`
- Tables: `footwear_product_master`, `chat_history`

## Usage

Run the Streamlit app:
```bash
streamlit run app.py
```

### Example Queries

- "Show me all products from Nike"
- "What's the average price of shoes launched in 2023?"
- "List top 5 highest-rated products"
- "How many products are there per brand?"

## Project Structure

```
text_to_sql/
├── app.py                  # Main Streamlit application
├── modules/
│   ├── db_connection.py    # PostgreSQL connection handler
│   ├── schema_extractor.py # Schema extraction logic
│   ├── chat_history.py     # Chat history management
│   ├── llm_client.py       # Groq LLaMA client
│   ├── sql_validator.py    # SQL validation
│   └── sql_executor.py     # SQL execution
├── requirements.txt        # Python dependencies
├── .env.example           # Environment variable template
└── README.md              # This file
```

## Error Handling

- **Invalid SQL**: Automatically detects and rejects unsafe operations
- **Execution errors**: Gracefully handles database errors
- **Ambiguous questions**: Prompts user for clarification
- **No stack traces**: User-friendly error messages only

## Limitations

- Only supports SELECT queries
- Limited to configured database schema
- Requires valid Groq API key
- Chat history limited to last 5 messages

## License

MIT License

## Contributing

Pull requests welcome! Please ensure all tests pass before submitting.
