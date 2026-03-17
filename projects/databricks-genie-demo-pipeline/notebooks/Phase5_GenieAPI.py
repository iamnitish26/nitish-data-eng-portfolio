# Databricks notebook source
# =============================================================
# Phase 5: Genie Conversation API
# =============================================================
# OBJECTIVE: Test the Genie Conversation API for programmatic
# natural language querying of your data
# =============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Before running this notebook:
# MAGIC 1. Create a Genie Space in the Databricks UI
# MAGIC    - Go to SQL > Genie Spaces > Create Space
# MAGIC    - Name: "Retail Analytics"
# MAGIC    - Add tables: gold_daily_sales, gold_customer_summary
# MAGIC    - Add column descriptions and sample SQL queries
# MAGIC    - Note down the Space ID from the URL
# MAGIC 2. Get your workspace URL and access token

# COMMAND ----------

# Configuration
DATABRICKS_HOST = "https://<your-workspace>.azuredatabricks.net"  # Update this
GENIE_SPACE_ID = "<your-space-id>"  # Update this after creating space

# Use the Databricks SDK (pre-installed in Databricks Runtime)
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Start a Conversation

# COMMAND ----------

import time

def ask_genie(space_id: str, question: str, conversation_id: str = None):
    """
    Ask a question to a Genie Space and wait for the response.

    Args:
        space_id: The Genie Space ID
        question: Natural language question
        conversation_id: Optional, to continue an existing conversation

    Returns:
        dict with conversation_id, message_id, status, and result
    """
    if conversation_id is None:
        # Start new conversation
        response = w.genie.start_conversation(
            space_id=space_id,
            content=question
        )
        conv_id = response.conversation.id
        msg_id = response.message.id
    else:
        # Continue existing conversation
        response = w.genie.create_message(
            space_id=space_id,
            conversation_id=conversation_id,
            content=question
        )
        conv_id = conversation_id
        msg_id = response.message.id

    # Poll for completion
    max_attempts = 30
    for attempt in range(max_attempts):
        msg = w.genie.get_message(
            space_id=space_id,
            conversation_id=conv_id,
            message_id=msg_id
        )
        if msg.status in ("COMPLETED", "FAILED"):
            break
        time.sleep(2)

    return {
        "conversation_id": conv_id,
        "message_id": msg_id,
        "status": msg.status,
        "message": msg
    }

# COMMAND ----------

# Ask first question
result = ask_genie(
    GENIE_SPACE_ID,
    "What was the total revenue last month?"
)

print(f"Status: {result['status']}")
print(f"Conversation ID: {result['conversation_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Follow-up Question (Context Retention)

# COMMAND ----------

# Continue the same conversation
result2 = ask_genie(
    GENIE_SPACE_ID,
    "Break that down by country",
    conversation_id=result["conversation_id"]
)

print(f"Status: {result2['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Complex Analytical Question

# COMMAND ----------

result3 = ask_genie(
    GENIE_SPACE_ID,
    "Which customers have the highest lifetime value and what products do they buy most?"
)

print(f"Status: {result3['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Batch Questions for Evaluation

# COMMAND ----------

test_questions = [
    "What is the total revenue?",
    "Show me the top 5 selling products",
    "How many unique customers do we have?",
    "What is the average order value by country?",
    "Which month had the highest sales?",
    "Show me revenue trend over time",
    "What percentage of orders are returns?",
    "Who are our top 10 customers by spending?",
]

results = []
for q in test_questions:
    try:
        r = ask_genie(GENIE_SPACE_ID, q)
        results.append({
            "question": q,
            "status": r["status"],
            "has_result": r["status"] == "COMPLETED"
        })
        print(f"✓ {q} -> {r['status']}")
    except Exception as e:
        results.append({
            "question": q,
            "status": "ERROR",
            "error": str(e)
        })
        print(f"✗ {q} -> ERROR: {e}")
    time.sleep(3)  # Rate limiting

# COMMAND ----------

# Summary
import pandas as pd

df_results = pd.DataFrame(results)
success_rate = df_results["has_result"].mean() * 100
print(f"\nSuccess rate: {success_rate:.0f}% ({df_results['has_result'].sum()}/{len(df_results)})")
display(spark.createDataFrame(df_results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring
# MAGIC
# MAGIC | Aspect | Rating | Notes |
# MAGIC |--------|--------|-------|
# MAGIC | API ease of use | /10 | |
# MAGIC | Answer accuracy | /10 | |
# MAGIC | Context retention | /10 | |
# MAGIC | Complex query handling | /10 | |
# MAGIC | Response time | /10 | |
# MAGIC
# MAGIC **Overall Phase 5 Rating: /10**
