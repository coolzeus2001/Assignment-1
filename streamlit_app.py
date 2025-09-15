import streamlit as st
import json
import re
import sqlparse
from google.generativeai import GenerativeModel
import google.generativeai as genai
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class Text2SQLSystem:
    def __init__(self):
        self.schema = self.load_schema()
        self.synonyms = {
            'employees': 'users',
            'issues': 'incidents',
            'department head': 'manager_id',
            'ticket': 'tickets',
            'incident': 'incidents',
            'asset': 'assets',
            'dept': 'departments',
            'kb': 'knowledge_base',
            'article': 'knowledge_base',
            'change': 'change_requests',
            'log': 'logs'
        }
        self.unsafe_keywords = ['DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
        
    def load_schema(self):
        """Load the database schema from JSON file"""
        try:
            # Try multiple possible paths for the schema file
            schema_paths = [
                'simple_enterprise_schema.json',
                './simple_enterprise_schema.json',
                '/Users/adityarajput/Assignment 1 /simple_enterprise_schema.json'
            ]
            
            for path in schema_paths:
                try:
                    with open(path, 'r') as f:
                        return json.load(f)
                except FileNotFoundError:
                    continue
            
            st.error(f"Schema file not found! Tried paths: {schema_paths}")
            return None
        except Exception as e:
            st.error(f"Error loading schema: {str(e)}")
            return None
    
    def preprocess_query(self, query):
        """Clean and preprocess the user query"""
        # Clean the query
        cleaned_query = query.lower().strip()
        cleaned_query = re.sub(r'[^\w\s]', ' ', cleaned_query)
        cleaned_query = ' '.join(cleaned_query.split())
        
        # Check for potential DML intent (but don't block - let Gemini decide)
        potential_dml_patterns = [
            r'\b(delete|remove|erase|clear)\b',
            r'\b(insert|add|create|created)\b',
            r'\b(update|modify|change|edit|updated)\b',
            r'\b(drop|truncate|alter)\b'
        ]
        
        possible_dml = False
        detected_words = []
        for pattern in potential_dml_patterns:
            match = re.search(pattern, cleaned_query)
            if match:
                possible_dml = True
                # Extract the matched word for context
                detected_words.append(match.group(1))  # Get the first captured group
        
        # Check for queries asking for information not available in schema
        unavailable_info_patterns = [
            r'\b(removed|deleted|archived|inactive|disabled)\b',
            r'\b(how many.*removed|count.*removed|list.*removed)\b',
            r'\b(removal|deletion|archive)\b'
        ]
        
        for pattern in unavailable_info_patterns:
            if re.search(pattern, cleaned_query):
                return None, f"Unsupported Query: The query asks for information about user removal/deletion, but the schema does not contain columns for tracking removed users (like 'removed', 'deleted', or 'archived' status)."
        
        # Replace synonyms
        words = cleaned_query.split()
        processed_words = []
        for word in words:
            if word in self.synonyms:
                processed_words.append(self.synonyms[word])
            else:
                processed_words.append(word)
        
        processed_query = ' '.join(processed_words)
        
        # DEBUG: Print preprocessing results
        print(f"DEBUG - Processed query: {processed_query}")
        print(f"DEBUG - Possible DML: {possible_dml}")
        print(f"DEBUG - Detected words: {detected_words}")
        
        # Return both the processed query and DML detection info
        return {
            'query': processed_query,
            'possible_dml': possible_dml,
            'detected_words': detected_words
        }
    
    def check_unsafe_query(self, query):
        """Check if query contains unsafe operations"""
        query_upper = query.upper()
        for keyword in self.unsafe_keywords:
            # Use word boundaries to avoid false positives in column names
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, query_upper):
                return False, f"DML operations like {keyword} are not supported. Only SELECT queries are allowed."
        return True, "Safe query"
    
    def create_schema_context(self):
        """Create schema context for the LLM"""
        context = "Database Schema:\n\n"
        for table in self.schema['tables']:
            context += f"Table: {table['table_name']}\n"
            context += f"Description: {table['description']}\n"
            context += "Columns:\n"
            for col_name, col_desc in table['columns'].items():
                context += f"  - {col_name}: {col_desc}\n"
            context += "\n"
        
        context += "\nImportant Notes:\n"
        context += "- Use only the tables and columns listed above\n"
        context += "- For joins, use the correct foreign key relationships:\n"
        context += "  * incidents.user_id -> users.user_id\n"
        context += "  * assets.assigned_to -> users.user_id\n"
        context += "  * tickets.user_id -> users.user_id\n"
        context += "  * tickets.assigned_to -> users.user_id\n"
        context += "  * departments.manager_id -> users.user_id\n"
        context += "  * knowledge_base.created_by -> users.user_id\n"
        context += "  * change_requests.requested_by -> users.user_id\n"
        context += "  * logs.user_id -> users.user_id\n"
        context += "- Only generate SELECT queries, no DML operations\n"
        
        return context
    
    def generate_sql_with_gemini(self, processed_data):
        """Use Gemini API to generate SQL from natural language"""
        try:
            # Configure Gemini API - try both keys
            api_key = os.getenv('GEMINI_API_KEY') or os.getenv('GEMINI_API_KEY_2') or "AIzaSyDOua5rct6bbDP-shJCeRV0k_Nw_cvxghg"
            if not api_key:
                return None, "Gemini API key not found."
            
            genai.configure(api_key=api_key)
            model = GenerativeModel('gemini-1.5-flash')
            
            # Extract data from processed_data
            processed_query = processed_data['query']
            possible_dml = processed_data['possible_dml']
            detected_words = processed_data.get('detected_words', [])
            
            # Create the prompt
            schema_context = self.create_schema_context()
            
            # Add DML detection context if needed
            dml_context = ""
            if possible_dml:
                dml_context = f"""
IMPORTANT DML DETECTION:
This query contains words like: {', '.join(detected_words)}
Please carefully analyze if these words refer to:
- Schema columns (like created_at, deleted_flag, updated_on) → SAFE, generate SELECT query
- DML operations (INSERT, UPDATE, DELETE, CREATE TABLE, etc.) → UNSUPPORTED, return error

If the query is asking for information about data (SELECT), generate SQL.
If the query is trying to modify data (DML), respond with: "ERROR: Only SELECT queries are allowed."
"""
            
            prompt = f"""{schema_context}{dml_context}

Convert this natural language query to SQL:
"{processed_query}"

CRITICAL REQUIREMENTS:
1. Generate only a SELECT query
2. Use ONLY the exact tables and columns listed in the schema above - NO EXCEPTIONS
3. Do NOT invent, assume, or add any columns that are not explicitly listed in the schema
4. If the query asks for information that requires columns NOT in the schema, return an error
5. Use appropriate JOINs based on foreign key relationships
6. Return only the SQL query, no explanations
7. Use proper SQL syntax and formatting

SCHEMA VALIDATION - MANDATORY:
- Check EVERY column reference against the schema above
- If ANY column doesn't exist in the schema, return an error
- Do NOT assume common columns like 'created_at', 'updated_at', 'modified_at', etc. exist
- Do NOT assume typical database columns exist unless explicitly listed
- Even if the query makes logical sense, if the required columns don't exist, return an error

EXAMPLE:
- If query asks for "tickets created last week" but tickets table has no created_at column → ERROR
- If query asks for "users updated yesterday" but users table has no updated_at column → ERROR
- Do NOT generate SQL with non-existent columns

IMPORTANT: If the query cannot be answered with the available schema (missing tables/columns), respond with: "ERROR: Query cannot be answered with available schema"
"""
            
            # DEBUG: Print the full prompt being sent to Gemini
            print(f"DEBUG - Full prompt sent to Gemini:\n{prompt}")
            
            response = model.generate_content(prompt)
            sql_query = response.text.strip()
            
            # DEBUG: Print Gemini's raw response
            print(f"DEBUG - Gemini raw response: {repr(sql_query)}")
            
            # Clean up the response (remove markdown formatting if present)
            if sql_query.startswith('```'):
                sql_query = re.sub(r'^```sql\s*|\s*```$', '', sql_query, flags=re.IGNORECASE)
            
            # DEBUG: Print cleaned response
            print(f"DEBUG - Cleaned response: {repr(sql_query)}")
            
            # Check if Gemini returned an error instead of SQL
            if sql_query.upper().startswith('ERROR:') or 'cannot be answered' in sql_query.lower():
                print(f"DEBUG - Gemini returned error: {sql_query}")
                return None, f"Unsupported Query: {sql_query}"
            
            print(f"DEBUG - Returning SQL query: {sql_query}")
            return sql_query, None
            
        except Exception as e:
            return None, f"Error generating SQL: {str(e)}"
    
    def validate_sql(self, sql_query):
        """Validate the generated SQL query"""
        try:
            # Parse SQL to check syntax
            parsed = sqlparse.parse(sql_query)
            if not parsed:
                return False, "Unsupported Query: Invalid SQL syntax"
            
            statement = parsed[0]
            
            # Check if it's a SELECT statement
            if not statement.get_type() == 'SELECT':
                return False, "Unsupported Query: Only SELECT queries are supported. DML operations (INSERT, UPDATE, DELETE) are not allowed."
            
            # Extract table names and column references from the query
            tables_in_query = set()
            columns_in_query = set()
            tokens = statement.flatten()
            current_token = None
            
            for token in tokens:
                if token.ttype is sqlparse.tokens.Name:
                    if current_token and current_token.value.upper() in ['FROM', 'JOIN']:
                        tables_in_query.add(token.value.lower())
                    elif current_token and current_token.value.upper() == 'SELECT':
                        # This might be a column reference
                        if '.' in token.value:
                            table_name, col_name = token.value.lower().split('.', 1)
                            columns_in_query.add((table_name, col_name))
                    # Check for column references anywhere in the query (including WHERE clauses)
                    if '.' in token.value:
                        table_name, col_name = token.value.lower().split('.', 1)
                        columns_in_query.add((table_name, col_name))
                    current_token = token
                elif token.ttype is sqlparse.tokens.Keyword:
                    current_token = token
            
            # Validate table names
            valid_tables = {table['table_name'] for table in self.schema['tables']}
            invalid_tables = tables_in_query - valid_tables
            
            if invalid_tables:
                return False, f"Unsupported Query: Table(s) '{', '.join(invalid_tables)}' do not exist in the database schema. Available tables: {', '.join(sorted(valid_tables))}"
            
            # Validate column references
            invalid_columns = []
            for table_name, col_name in columns_in_query:
                if table_name in valid_tables:
                    # Find the table in schema
                    table_info = next((t for t in self.schema['tables'] if t['table_name'] == table_name), None)
                    if table_info:
                        valid_cols = set(table_info['columns'].keys())
                        if col_name not in valid_cols:
                            invalid_columns.append(f"{table_name}.{col_name}")
            
            if invalid_columns:
                # Get available columns for the tables mentioned
                table_columns_info = []
                for invalid_col in invalid_columns:
                    table_name = invalid_col.split('.')[0]
                    table_info = next((t for t in self.schema['tables'] if t['table_name'] == table_name), None)
                    if table_info:
                        available_cols = list(table_info['columns'].keys())
                        table_columns_info.append(f"{table_name} table: {', '.join(available_cols)}")
                
                return False, f"Unsupported Query: Column(s) '{', '.join(invalid_columns)}' do not exist in the schema. Available columns: {'; '.join(table_columns_info)}"
            
            # Check for unsafe operations
            is_safe, safety_msg = self.check_unsafe_query(sql_query)
            if not is_safe:
                return False, f"Unsupported Query: {safety_msg}"
            
            return True, "Valid SQL query"
            
        except Exception as e:
            return False, f"Unsupported Query: SQL validation error - {str(e)}"
    
    def process_query(self, user_query):
        """Main processing pipeline"""
        # Step 1: Preprocessing
        processed_data = self.preprocess_query(user_query)
        if isinstance(processed_data, tuple):
            return None, processed_data[1], None, None  # Return error from preprocessing
        
        # Step 2: Generate SQL with Gemini
        sql_query, error = self.generate_sql_with_gemini(processed_data)
        if error:
            return None, f"SQL Generation Error: {error}", None, None
        
        # Step 3: Validate SQL
        is_valid, validation_msg = self.validate_sql(sql_query)
        if not is_valid:
            return None, validation_msg, sql_query, "Validation Failed"
        
        return sql_query, None, sql_query, "Validation Passed"

def main():
    st.set_page_config(
        page_title="Text2SQL System",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Text2SQL System")
    st.markdown("Convert natural language queries to SQL using AI")
    
    # Initialize the system
    if 'text2sql_system' not in st.session_state:
        st.session_state.text2sql_system = Text2SQLSystem()
    
    system = st.session_state.text2sql_system
    
    if not system.schema:
        st.error("Failed to load database schema. Please check the schema file.")
        return
    
    # Sidebar with schema information
    with st.sidebar:
        st.header("📊 Database Schema")
        
        for table in system.schema['tables']:
            with st.expander(f"Table: {table['table_name']}"):
                st.write(f"**Description:** {table['description']}")
                st.write("**Columns:**")
                for col_name, col_desc in table['columns'].items():
                    st.write(f"• {col_name}: {col_desc}")
    
    # Main interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("💬 Enter Your Query")
        user_query = st.text_area(
            "Ask a question about the database in natural language:",
            placeholder="Example: Show all open incidents reported by John Doe",
            height=100
        )
        
        if st.button("🔄 Generate SQL", type="primary"):
            if not user_query.strip():
                st.warning("Please enter a query first!")
            else:
                with st.spinner("Processing your query..."):
                    sql_query, error, gemini_response, validation_status = system.process_query(user_query)
                    
                    if error:
                        if "Unsupported Query" in error:
                            st.error(f"🚫 {error}")
                            st.info("💡 **Tip**: Try rephrasing your query to use only SELECT operations with tables and columns from the schema shown in the sidebar.")
                        else:
                            st.error(f"❌ {error}")
                    else:
                        st.success("✅ SQL generated successfully!")
                        
                        st.subheader("📋 Generated SQL Query")
                        st.code(sql_query, language="sql")
                        
                        # Copy button
                        st.button("📋 Copy SQL", help="Click to copy the SQL query")
    
    with col2:
        st.subheader("🚫 Unsupported Query Types")
        st.markdown("""
        **The system does NOT support:**
        - **DML Operations**: INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER
        - **Non-existent tables**: Tables not in the schema
        - **Invalid columns**: Columns that don't exist in tables
        - **Data modification**: Any query that changes data
        """)

if __name__ == "__main__":
    main()
