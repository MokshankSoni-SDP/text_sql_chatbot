---
title: DB Talk - Text-to-SQL Chatbot
emoji: 💬
colorFrom: blue
colorTo: purple
sdk: streamlit
sdk_version: "1.30.0"
app_file: app.py
pinned: false
license: mit
---

# 💬 DB Talk - Multi-Tenant Text-to-SQL Chatbot

Ask questions about your data in natural language and get instant SQL-powered answers! Upload your CSV/Excel files and chat with your data using AI.

## 🌟 Features

### 🔐 Multi-Tenant Architecture
- **Private Projects**: Each user gets isolated database schemas
- **No Data Mixing**: Complete separation between different users
- **Upload & Query**: Upload CSV, Excel, or JSON files and start asking questions

### 🎯 Intelligent Query Processing
- **Hallucination Prevention**: Auto-corrects queries that return zero results
- **Multi-Question Support**: Ask multiple questions in one go
- **Context Awareness**: Remembers conversation context for follow-up questions
- **Natural Language Answers**: Get human-readable responses, not just raw data

### 🛡️ Security First
- **Read-Only Access**: Only SELECT queries allowed
- **SQL Injection Protection**: Input sanitization and validation
- **SELECT-Only Queries**: No data modification possible

## 🚀 How to Use

### 1️⃣ Create a Project
1. Enter your **User ID** (e.g., `john_doe`)
2. Enter a **Project Name** (e.g., `sales_data`)
3. **Upload your data file** (CSV, Excel, or JSON)
4. Click **🚀 Create Project**

### 2️⃣ Start Asking Questions
1. Click **📖 Load** on your project
2. Click **🔌 Load Schema** in the sidebar
3. Type your questions in natural language!

### 3️⃣ Example Questions
- "Show me all products from Nike"
- "What's the average price by brand?"
- "How many products do we have in each category?"
- "Compare sales between Nike and Adidas"

## 💡 Tips

- **Multiple Questions**: Separate questions with `?` or newlines
- **Follow-up Questions**: The bot remembers context from previous questions
- **Zero Results**: If a query returns no results, the bot automatically retries with corrected values

## 🏗️ Technology Stack

- **LLM**: Groq LLaMA 3.3 70B (ultra-fast inference)
- **Database**: PostgreSQL with Aiven Cloud
- **Frontend**: Streamlit
- **Schema Isolation**: PostgreSQL schema-based multi-tenancy

## 📊 Supported File Formats

- **CSV**: Comma-separated values
- **Excel**: .xlsx, .xls files
- **JSON**: Array of objects or line-delimited JSON

## 🔗 Links

- **Main Repository**: [GitHub](https://github.com/MokshankSoni-SDP/text_sql_chatbot)
- **Documentation**: [README.md](https://github.com/MokshankSoni-SDP/text_sql_chatbot#readme)

## 📝 Example Use Cases

- Sales data analysis
- Product inventory queries
- Customer analytics
- Financial data exploration
- HR data insights

## 🆘 Troubleshooting

### Database Connection Issues
Click the **🔄 Test Connection** button on the home page to verify connectivity.

### Schema Not Loaded
Make sure to click **🔌 Load Schema** in the sidebar before asking questions.

### Zero Results
The bot automatically retries with corrected values. Check the generated SQL to see what was executed.

---

**Built with ❤️ using Streamlit, PostgreSQL, and LLaMA 3.3**
