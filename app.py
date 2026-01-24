"""
Main Streamlit application for Text-to-SQL chatbot.
"""

import streamlit as st

def main():
    st.set_page_config(
        page_title="Text-to-SQL Chatbot",
        page_icon="💬",
        layout="wide"
    )
    
    st.title("💬 Text-to-SQL Chatbot")
    st.markdown("Ask questions about your database in natural language!")
    
    # Placeholder for app logic
    st.info("🚧 Application under development")

if __name__ == "__main__":
    main()
