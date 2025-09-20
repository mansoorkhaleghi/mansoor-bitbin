from autogen import AssistantAgent, UserProxyAgent
import requests
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any
import re

OLLAMA_URL = "http://localhost:11434"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}


def call_ollama(prompt, model="qwen2.5:3b", max_tokens=512, temperature=0.7):
    payload = {
        "model": model,
        "prompt": prompt,
        "options": {
            "num_predict": max_tokens,
            "temperature": temperature,
            "top_p": 0.9,
        },
    }

    try:
        with requests.post(
            f"{OLLAMA_URL}/api/generate", json=payload, stream=True
        ) as resp:
            if resp.status_code != 200:
                raise RuntimeError(f"Failed: {resp.status_code} {resp.text}")

            full_response = ""
            for line in resp.iter_lines():
                if line:
                    line_content = line.decode("utf-8")
                    try:
                        json_obj = json.loads(line_content)
                        if "response" in json_obj:
                            full_response += json_obj["response"]
                        if json_obj.get("done", False):
                            break
                    except json.JSONDecodeError:
                        continue

            return full_response.strip()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Request failed: {e}")


class PostgreSQLTools:
    def __init__(self, config):
        self.config = config
        self.connection = None

    def get_connection(self):
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(**self.config)
        return self.connection

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, params)

            if cursor.description:  # It's a SELECT query
                results = cursor.fetchall()
                cursor.close()
                return [dict(row) for row in results]
            else:  # It's an INSERT/UPDATE/DELETE
                conn.commit()
                cursor.close()
                return [{"status": "success", "rows_affected": cursor.rowcount}]

        except psycopg2.Error as e:
            if self.connection:
                self.connection.rollback()
            return [{"error": f"Database error: {str(e)}"}]
        except Exception as e:
            return [{"error": f"Unexpected error: {str(e)}"}]

    def get_tables(self) -> List[Dict[str, Any]]:
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """
        return self.execute_query(query)

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
        """
        return self.execute_query(query, (table_name,))


db_tools = PostgreSQLTools(POSTGRES_CONFIG)


# Tool Functions
def query_database(query: str) -> str:
    """Execute a SQL query against the PostgreSQL database"""
    try:
        results = db_tools.execute_query(query)
        return json.dumps(results, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": f"Failed to execute query: {str(e)}"})


def list_tables() -> str:
    try:
        tables = db_tools.get_tables()
        table_names = [row["table_name"] for row in tables]
        return f"Available tables: {', '.join(table_names)}\n\nFull details:\n{json.dumps(tables, indent=2)}"
    except Exception as e:
        return json.dumps({"error": f"Failed to list tables: {str(e)}"})


def describe_table(table_name: str) -> str:
    try:
        schema = db_tools.get_table_schema(table_name)
        columns = []
        for col in schema:
            col_info = f"- {col['column_name']} ({col['data_type']})"
            if col["is_nullable"] == "NO":
                col_info += " [NOT NULL]"
            if col["column_default"]:
                col_info += f" [DEFAULT: {col['column_default']}]"
            columns.append(col_info)

        summary = f"Table '{table_name}' has {len(schema)} columns:\n" + "\n".join(
            columns
        )
        return f"{summary}\n\nFull schema details:\n{json.dumps(schema, indent=2)}"
    except Exception as e:
        return json.dumps(
            {"error": f"Failed to describe table '{table_name}': {str(e)}"}
        )


class ToolDetector:
    def __init__(self):
        self.tools = {
            "list_tables": list_tables,
            "describe_table": describe_table,
            "query_database": query_database,
        }

    def extract_tool_call(self, text: str) -> tuple:
        text = text.strip()

        sql_patterns = [
            r"```sql\s*(.*?)\s*```",
            r"```\s*(SELECT.*?)\s*```",
            r"query_database\s*\(\s*[\"'](.*?)[\"']\s*\)",
            r"query_database\s*\(\s*[\"']{3}(.*?)[\"']{3}\s*\)",
            r"Use query_database\s*\(\s*[\"']{3}(.*?)[\"']{3}\s*\)",
            r"Use query_database\s*\(\s*[\"'](.*?)[\"']\s*\)",
        ]

        for pattern in sql_patterns:
            match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
            if match:
                sql_query = match.group(1).strip()
                if sql_query and any(
                    keyword in sql_query.upper()
                    for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE"]
                ):
                    return "query_database", sql_query

        simple_patterns = [
            r"(?:use|call|execute)\s+(list_tables|describe_table)\s*\(\s*['\"]?([^'\"]*)['\"]?\s*\)",
            r"(?:use|call|execute)\s+(list_tables|describe_table)",
            r"(?:use|call)\s+(list_tables)",
        ]

        for pattern in simple_patterns:
            match = re.search(pattern, text.lower())
            if match:
                tool_name = match.group(1)
                param = match.group(2) if len(match.groups()) > 1 else None
                return tool_name, param

        if any(
            phrase in text.lower()
            for phrase in ["select ", "from books", "from ratings", "join"]
        ):
            lines = text.split("\n")
            sql_lines = []
            in_sql = False

            for line in lines:
                line = line.strip()
                if any(
                    word in line.upper()
                    for word in [
                        "SELECT",
                        "FROM",
                        "WHERE",
                        "GROUP BY",
                        "ORDER BY",
                        "JOIN",
                    ]
                ):
                    in_sql = True
                    sql_lines.append(line)
                elif (
                    in_sql
                    and line
                    and not any(
                        word in line.lower()
                        for word in ["use", "tool", "execute", "query_database"]
                    )
                ):
                    sql_lines.append(line)
                elif in_sql and (not line or line.endswith(";")):
                    if line.endswith(";"):
                        sql_lines.append(line)
                    break

            if sql_lines:
                sql_query = " ".join(sql_lines).replace(";", "").strip()
                if sql_query:
                    return "query_database", sql_query

        return None, None

    def execute_tool(self, tool_name: str, param: str = None) -> str:
        if tool_name not in self.tools:
            return f"Error: Unknown tool '{tool_name}'. Available tools: {list(self.tools.keys())}"

        try:
            if tool_name == "list_tables":
                return self.tools[tool_name]()
            elif tool_name == "describe_table":
                if not param:
                    return "Error: describe_table requires a table name parameter"
                return self.tools[tool_name](param)
            elif tool_name == "query_database":
                if not param:
                    return "Error: query_database requires an SQL query parameter"
                return self.tools[tool_name](param)
        except Exception as e:
            return f"Error executing {tool_name}: {str(e)}"


tool_detector = ToolDetector()

SYSTEM_PROMPT = """
You are a helpful AI assistant with access to a PostgreSQL database containing books and ratings data.

DATABASE SCHEMA:
- books table: book_id (INT), title (TEXT), authors (TEXT), original_publication_year (INT)
- ratings table: rating_id (SERIAL), book_id (INT), user_id (INT), rating (INT)

AVAILABLE TOOLS:
1. list_tables() - Shows all available tables in the database
2. describe_table('table_name') - Shows the structure/schema of a specific table  
3. query_database('SQL_QUERY') - Executes any SQL query

HOW TO USE TOOLS:
When you need database information, respond with:
- "Use list_tables" to see all tables
- "Use describe_table('books')" to see a table's structure
- For SQL queries, write the query clearly in your response like:

```sql
SELECT b.authors, AVG(r.rating) as avg_rating, COUNT(r.rating) as review_count
FROM books b 
JOIN ratings r ON b.book_id = r.book_id 
GROUP BY b.authors 
HAVING COUNT(r.rating) >= 5
ORDER BY avg_rating DESC 
LIMIT 3
```

EXAMPLES of good queries for finding best authors:
- Authors with highest average ratings (minimum 5 reviews)
- Authors with most books rated above 4 stars
- Authors with consistently high ratings across multiple books

Be conversational and explain what you're doing. Always analyze the results and provide insights.
"""


class DatabaseAgent(AssistantAgent):
    def __init__(self, name="DatabaseAgent", model="qwen2.5:3b"):
        super().__init__(
            name=name,
            llm_config=False,
            system_message=SYSTEM_PROMPT,
        )
        self.model = model
        self.conversation_history = []

    def on_user_message(self, message: str) -> str:
        self.conversation_history.append(f"User: {message}")

        history_context = "\n".join(self.conversation_history[-3:])  # Last 3 exchanges
        full_prompt = f"""
{SYSTEM_PROMPT}

Recent conversation:
{history_context}

Current user message: {message}

Respond helpfully. If you need to query the database, write a clear SQL query in your response.
For questions about "best authors", consider metrics like average rating, number of reviews, etc.
"""

        assistant_response = call_ollama(full_prompt, model=self.model, max_tokens=512)

        tool_name, param = tool_detector.extract_tool_call(assistant_response)

        if tool_name:
            print(f"\n[DETECTED TOOL CALL: {tool_name}]")
            if param:
                print(f"[PARAMETER: {param[:100]}{'...' if len(param) > 100 else ''}]")

            tool_result = tool_detector.execute_tool(tool_name, param)
            print(f"[TOOL RESULT]\n{tool_result}\n")

            follow_up_prompt = f"""
{SYSTEM_PROMPT}

Recent conversation:
{history_context}

Previous assistant response: {assistant_response}

Tool executed: {tool_name}{f' with parameter: {param}' if param else ''}
Tool result: {tool_result}

Now analyze these results and provide a helpful response to the user. If the results show authors and ratings, explain who the best authors are based on the data.
"""

            final_response = call_ollama(
                follow_up_prompt, model=self.model, max_tokens=512
            )

            self.conversation_history.append(f"Assistant: {final_response}")

            return f"{assistant_response}\n\n[TOOL EXECUTED: {tool_name}]\n{tool_result}\n\n{final_response}"
        else:
            self.conversation_history.append(f"Assistant: {assistant_response}")
            return assistant_response


# Initialize agents
user_proxy = UserProxyAgent(
    name="User",
    human_input_mode="ALWAYS",
    max_consecutive_auto_reply=0,
    code_execution_config=False,
)

MODEL_TO_TRY = "llama3.2"

agent = DatabaseAgent(name="DB-Agent", model=MODEL_TO_TRY)

if __name__ == "__main__":
    print(" AutoGen Database Agent with PostgreSQL Tools")
    print(f" Model: {MODEL_TO_TRY}")
    print(" Tips:")
    print(
        " - Say 'show me authors with highest average ratings in persion masalan begoo behtarin author ro moarefi kon'"
    )
    print("   - Type 'exit' to quit")
    print("=" * 60)

    print("Testing database connection...")
    try:
        test_result = db_tools.execute_query("SELECT 1 as test")
        if "error" in test_result[0]:
            print(f"Database connection failed: {test_result[0]['error']}")
            exit(1)
        print("Database connection successful!")
    except Exception as e:
        print(f"Database connection test failed: {e}")
        exit(1)

    welcome_msg = call_ollama(
        f"{SYSTEM_PROMPT}\n\nUser: Hello, what can you do with the books and ratings database?",
        model=MODEL_TO_TRY,
        max_tokens=200,
    )
    print(f"\nAgent: {welcome_msg}\n")

    while True:
        try:
            user_input = input("You: ").strip()
            if user_input.lower() in ["exit", "quit", "bye"]:
                print("Goodbye!")
                break

            if not user_input:
                continue

            print("\nAgent:", end=" ", flush=True)
            response = agent.on_user_message(user_input)
            print(response)
            print("\n" + "=" * 60)

        except KeyboardInterrupt:
            print("\n\n Interrupted by user. Goodbye!")
            break
        except Exception as e:
            print(f"\n Error: {e}")
            print("Please try again or type 'exit' to quit.")
