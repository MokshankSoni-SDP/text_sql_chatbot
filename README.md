# Text-to-SQL Chatbot with Multi-Tenant Architecture

A production-ready natural language to SQL query chatbot powered by Groq LLaMA 3.3 and PostgreSQL. Features multi-tenant data isolation, automated hallucination prevention, and zero-result retry logic.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 🌟 Key Features

### 🔐 Multi-Tenant Architecture
- **Schema-based isolation**: Each user project gets its own PostgreSQL schema
- **No data leakage**: Complete isolation between projects
- **User document upload**: Support for CSV, Excel, and JSON files
- **Dynamic schema creation**: Automatic table generation from uploaded data
- **Independent chat history**: Conversations are project-specific

### 🎯 Hallucination Prevention Mechanisms

#### 1. **Enriched Schema with Possible Values**
- Extracts actual distinct values from text columns (brands, categories, etc.)
- Provides LLM with exact valid values to choose from
- Example: Instead of guessing "Nike" could be "NIKE" or "nike", shows `['Nike', 'Adidas', 'Puma']`

#### 2. **Zero-Result Retry Logic**
- Automatically detects when queries return 0 results
- Re-prompts LLM with corrective feedback
- Uses possible values to fix hallucinated filter values
- Example: Fixes "Nikee" → "Nike" automatically

#### 3. **Schema-Qualified Queries**
- All queries use `schema_name.table_name` format
- Prevents cross-schema data access
- Ensures queries execute in correct context

#### 4. **Smart Context Management**
- Chat history limited to last 5 messages (configurable)
- Auto-summarization of long assistant responses
- Prevents context overflow while maintaining relevance

### 💬 Advanced Query Features
- **Multi-question processing**: Handles multiple questions in a single input
- **Conversation context**: Uses chat history for follow-up questions
- **ID column exclusion**: Automatically hides technical IDs from results
- **Natural language answers**: Converts SQL results to human-readable responses

### 🛡️ Security & Validation
- **SELECT-only queries**: Blocks INSERT, UPDATE, DELETE, DROP, etc.
- **SQL injection prevention**: Input sanitization and parameterized queries
- **Read-only operations**: No data modification allowed
- **Safe error handling**: No stack traces exposed to users

---

## 🏗️ Technology Stack

### Backend
- **Python 3.8+**: Core programming language
- **PostgreSQL**: Primary database with schema-based multi-tenancy
- **SQLAlchemy**: ORM and database toolkit
- **psycopg2**: PostgreSQL adapter for Python

### AI/LLM
- **Groq Cloud API**: Ultra-fast LLM inference
- **LLaMA 3.3 70B**: SQL generation and answer formatting
- **Zero-shot prompting**: No fine-tuning required

### Frontend
- **Streamlit**: Interactive web UI
- **Pandas**: Data manipulation and preview
- **Python-dotenv**: Environment variable management

### Architecture Patterns
- **Singleton pattern**: Database connection pooling
- **Schema-based multi-tenancy**: PostgreSQL schema isolation
- **Session management**: Streamlit session state

---

## 📊 How It Works

### Project Creation Flow
```
User Upload (CSV/Excel/JSON)
    ↓
Input Sanitization
    ↓
Create Schema (proj_user_projectname)
    ↓
Ingest Data → Create Tables
    ↓
Create Chat History Table
    ↓
Ready for Queries!
```

### Query Processing Flow
```
User Question
    ↓
Get Chat History (schema-specific)
    ↓
Extract Enriched Schema (with possible values)
    ↓
LLM Generates SQL (using exact values)
    ↓
Validate SQL (SELECT-only)
    ↓
Execute in Correct Schema
    ↓
Zero Results? → Auto-Retry with Correction
    ↓
Format Natural Language Answer
    ↓
Store in Chat History
```

---

## 🚀 Setup & Installation

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 12+ (with database creation privileges)
- Groq API key ([Get one here](https://console.groq.com))

### Installation Steps

#### 1. Clone the Repository
```bash
git clone https://github.com/MokshankSoni-SDP/text_sql_chatbot.git
cd text_sql_chatbot
```

#### 2. Create Virtual Environment
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

#### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 4. Database Setup

Create PostgreSQL database:
```sql
CREATE DATABASE text_to_sql_chatbot;
```

Optional: Set up public schema with sample data:
```bash
psql -U your_username -d text_to_sql_chatbot -f database_setup.sql
```

#### 5. Configure Environment Variables

Copy the example environment file:
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=text_to_sql_chatbot
DB_USER=your_username
DB_PASSWORD=your_password
DB_SCHEMA=public

# Groq API
GROQ_API_KEY=your_groq_api_key_here
GROQ_MODEL=llama-3.3-70b-versatile

# Application Settings
CHAT_HISTORY_LIMIT=5
SCHEMA_MAX_UNIQUE_VALUES=20
```

#### 6. Run the Application
```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

---

## 📖 Usage Guide

### Creating a New Project

1. **Enter User ID**: Your unique identifier (e.g., `john_doe`)
2. **Enter Project Name**: Name for your project (e.g., `Sales Data`)
3. **Upload Data File**: Choose CSV, Excel (.xlsx, .xls), or JSON file
4. **Preview Data**: Review the data preview before creating
5. **Create Project**: Click "🚀 Create Project"

Your data is now isolated in schema `proj_john_doe_sales_data`

### Loading an Existing Project

1. **Enter User ID**: Same ID used during creation
2. **View Projects**: See all your projects with metadata
3. **Click Load**: Load the project you want to query
4. **Load Schema**: Click "🔌 Load Schema" to extract database structure

### Asking Questions

**Single Question:**
```
Show me all products from Nike
```

**Multiple Questions (auto-split):**
```
How many products do we have?
What's the most expensive item?
```

**Follow-up Questions (uses context):**
```
User: Show me Nike products
Assistant: Found 5 Nike products...
User: What about Adidas?  ← Uses context!
```

### Example Queries

#### Basic Queries
- "Show me all products"
- "How many rows are in the products table?"
- "What brands do we have?"

#### Filtered Queries
- "Show me Nike products under $100"
- "List products in the Footwear category"
- "Find products with rating above 4.5"

#### Aggregations
- "What's the average price by brand?"
- "Count products per category"
- "Show top 5 highest-rated products"

#### Complex Queries
- "Compare average prices between Nike and Adidas"
- "Show brands with more than 10 products"
- "What's the price range for each category?"

---

## 📁 Project Structure

```
text_to_sql/
├── app.py                      # Main Streamlit application
├── modules/
│   ├── __init__.py
│   ├── db_connection.py        # PostgreSQL connection singleton
│   ├── schema_extractor.py     # Schema extraction with possible values
│   ├── chat_history.py         # Schema-specific chat history
│   ├── llm_client.py           # Groq LLaMA client (SQL & answer generation)
│   ├── sql_validator.py        # SQL validation (SELECT-only)
│   ├── sql_executor.py         # Schema-aware SQL execution
│   ├── project_manager.py      # Multi-tenant schema management
│   └── data_ingestion.py       # CSV/Excel/JSON file processing
├── requirements.txt            # Python dependencies
├── .env.example               # Environment template
├── .env                       # Your credentials (gitignored)
├── database_setup.sql         # Sample database setup
├── QUICKSTART.md             # Quick start guide
└── README.md                 # This file
```

---

## 🛠️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `text_to_sql_chatbot` |
| `DB_USER` | Database user | - |
| `DB_PASSWORD` | Database password | - |
| `DB_SCHEMA` | Default schema | `public` |
| `GROQ_API_KEY` | Groq API key | - |
| `GROQ_MODEL` | LLM model | `llama-3.3-70b-versatile` |
| `CHAT_HISTORY_LIMIT` | Max chat messages for context | `5` |
| `SCHEMA_MAX_UNIQUE_VALUES` | Max values to extract per column | `20` |

---

## 🔬 Technical Deep Dive

### Multi-Tenant Schema Design

Each user project gets an isolated PostgreSQL schema:

```sql
-- Schema naming: proj_{user_id}_{project_name}
CREATE SCHEMA proj_john_sales_data;

-- User tables (from uploaded files)
CREATE TABLE proj_john_sales_data.products (...);
CREATE TABLE proj_john_sales_data.orders (...);

-- Isolated chat history
CREATE TABLE proj_john_sales_data.chat_history (...);
```

**Benefits:**
- ✅ Complete data isolation
- ✅ Same table names across projects
- ✅ Easy cleanup (DROP SCHEMA CASCADE)
- ✅ No user management table needed

### Hallucination Prevention Details

#### Enriched Schema Extraction

**Problem:** LLMs often guess filter values that don't exist
```sql
-- User asks: "Show Nike products"
-- LLM might generate: WHERE brand = 'NIKE' or 'nike' or 'Nike Inc.'
-- Result: 0 rows (actual value is 'Nike')
```

**Solution:** Provide exact possible values
```
Table: products
  - brand (text) NOT NULL
    → Possible Values: ['Nike', 'Adidas', 'Puma']
```

**Implementation:**
```python
# Schema-qualified query (our fix!)
SELECT DISTINCT brand
FROM proj_john_sales_data.products
WHERE brand IS NOT NULL
ORDER BY brand
LIMIT 20;
```

#### Zero-Result Retry Logic

When a query returns 0 results:
1. Detect empty result set
2. Re-prompt LLM with corrective instructions
3. Show possible values explicitly
4. LLM generates corrected query
5. Execute and return results

**Example:**
```
Failed Query: WHERE brand = 'Nikee'  ← Typo
Schema: → Possible Values: ['Nike', 'Adidas', 'Puma']
Corrected: WHERE brand = 'Nike'      ← Fixed!
```

---

## 🔒 Security Features

### SQL Injection Prevention
- Input sanitization using regex
- Parameterized queries with psycopg2
- Schema name validation
- No dynamic SQL construction from user input

### Access Control
- **SELECT-only**: Blocks all modification operations
- **Dangerous keyword blocking**: DROP, DELETE, ALTER, TRUNCATE, etc.
- **Schema isolation**: Users can't access other schemas
- **Read-only execution**: No write operations permitted

### Validation Pipeline
```python
# 1. Parse SQL
# 2. Check for dangerous keywords
# 3. Ensure SELECT-only
# 4. Validate syntax
# 5. Execute with timeout
```

---

## 📊 Supported File Formats

### CSV Files
- **Encoding**: UTF-8, Latin-1 (auto-detected)
- **Delimiters**: Comma, semicolon, tab (auto-detected)
- **Size limit**: Recommended < 100MB
- **Column names**: Auto-sanitized (spaces → underscores)

### Excel Files
- **Formats**: .xlsx, .xls
- **Multiple sheets**: Creates one table per sheet
- **Size limit**: Recommended < 50MB
- **Formula support**: Values only (formulas evaluated)

### JSON Files
- **Formats**: Array of objects, line-delimited (JSONL)
- **Nested structures**: Auto-flattened
- **Size limit**: Recommended < 100MB

---

## 🚨 Error Handling

### User-Facing Errors
- ❌ **SQL Validation Failed**: Query contains unsafe operations
- ❌ **Query Execution Failed**: Database error (connection, syntax, etc.)
- ⚠️ **Zero Results**: Triggers auto-retry with correction
- ⚠️ **Schema Not Loaded**: Prompts user to load schema first

### Logging
- All errors logged to console with context
- No sensitive data in logs
- Stack traces hidden from users

---

## 💡 Design Decisions

### Why No Vector Embeddings?

**Deliberate choice for simplicity and accuracy:**

| Aspect | Vector DB Approach | Our Approach |
|--------|-------------------|--------------|
| **Complexity** | High (RAG pipeline) | Low (direct schema) |
| **Accuracy** | Similarity-based | Exact schema match |
| **Maintenance** | Sync vectors + DB | Just DB |
| **Cost** | Embeddings + Vector DB | Only LLM API |
| **Latency** | 2 API calls | 1 API call |

**Result:** Simpler, faster, more accurate for structured data.

### Why Schema-Based Multi-Tenancy?

**Alternatives considered:**
1. ❌ **Table prefix** (`user_john_products`) - Namespace pollution
2. ❌ **Tenant ID column** - Complex queries, no isolation
3. ✅ **Separate schemas** - Clean isolation, simple queries

### Why Groq LLaMA?

- ⚡ **Ultra-fast inference**: 800+ tokens/sec
- 💰 **Cost-effective**: Competitive pricing
- 🎯 **Good SQL generation**: LLaMA 3.3 70B excels at code
- 🔓 **Open model**: Transparency and trust

---

## 🎯 Limitations

- **Query type**: Only SELECT queries supported
- **Database**: PostgreSQL only (not MySQL, SQLite, etc.)
- **File size**: Large files (>100MB) may be slow to process
- **Concurrent users**: Single database connection pool
- **LLM dependency**: Requires Groq API (internet connection)
- **Context window**: Chat history limited to recent messages

---

## 🗺️ Roadmap

- [ ] Support for MySQL and SQLite
- [ ] Query result export (CSV, Excel)
- [ ] Visualization generation (charts from data)
- [ ] Query history and favorites
- [ ] Role-based access control (RBAC)
- [ ] Scheduled query execution
- [ ] API endpoint for programmatic access

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/new-feature`
3. **Make changes** with clear commit messages
4. **Test thoroughly** with different databases and queries
5. **Submit a pull request** with description

### Code Style
- Follow PEP 8 for Python code
- Use type hints where applicable
- Add docstrings to new functions
- Keep functions focused and modular

---

## 📄 License

MIT License - See [LICENSE](LICENSE) file for details

---

## 🙏 Acknowledgments

- **Groq**: For providing ultra-fast LLM inference
- **Meta**: For open-sourcing LLaMA models
- **Streamlit**: For the amazing UI framework
- **PostgreSQL**: For robust schema-based multi-tenancy

---

## 📞 Support

For issues, questions, or feature requests:
- **GitHub Issues**: [Create an issue](https://github.com/MokshankSoni-SDP/text_sql_chatbot/issues)
- **Email**: mokshank.soni@example.com

---

## 🌟 Star History

If you find this project useful, please consider giving it a ⭐ on GitHub!

---

**Built with ❤️ using Python, PostgreSQL, and LLaMA 3.3**
