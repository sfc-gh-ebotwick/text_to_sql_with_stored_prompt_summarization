from typing import Dict, List, Optional

import _snowflake
import json
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import complete
from snowflake.core import Root


#Set up root and session objects
session = get_active_session()
root = Root(session)


DATABASE = "CUSTOMER_LTV_DATA"
SCHEMA = "PUBLIC"
STAGE = "SEMANTIC"
FILE = "customer_lifetime_value.yaml"

with st.sidebar:
        st.selectbox(
            "Selected LLM for Summarization:",
            ["claude-3-5-sonnet",
            "llama4-maverick",
            "snowflake-llama-3.3-70b",
            "gemma-7b",
            "jamba-1.5-mini",
            "jamba-1.5-large",
            "jamba-instruct",
            "llama2-70b-chat",
            "llama3-8b",
            "llama3-70b",
            "llama3.1-8b",
            "llama3.1-70b",
            "llama3.1-405b",
            "llama3.2-1b",
            "llama3.2-3b",
            "llama3.3-70b",
            "mistral-large",
            "mistral-large2",
            "mistral-7b",
            "mixtral-8x7b",
            "reka-core",
            "reka-flash",
            "snowflake-arctic",
            "snowflake-llama-3.1-405b"],
            key = "selected_summarization_model",
        )


class CortexAnalyst():
    def call_analyst_api(self, prompt: str) -> dict:

        """Calls the REST API and returns the response."""
        request_body = {
            "messages": st.session_state.messages,
            "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
        }
        resp = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/cortex/analyst/message",
            {},
            {},
            request_body,
            {},
            30000,
        )
        if resp["status"] < 400:
            return json.loads(resp["content"])
        else:
            st.session_state.messages.pop()
            raise Exception(
                f"Failed request with status {resp['status']}: {resp}"
            )

    def process_api_response(self, prompt: str) -> str:
        """Processes a message and adds the response to the chat."""
        st.session_state.messages.append(
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        )
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Generating response..."):
                # response = "who had the most rec yards week 10"
                response = self.call_analyst_api(prompt=prompt)
                request_id = response["request_id"]
                content = response["message"]["content"]
                st.session_state.messages.append(
                    {**response['message'], "request_id": request_id}
                )
                final_return = self.process_sql(content=content, request_id=request_id)  # type: ignore[arg-type]
                
        return final_return
        
    def process_sql(self,
        content: List[Dict[str, str]],
        request_id: Optional[str] = None,
        message_index: Optional[int] = None,
    ) -> str:
        """Displays a content item for a message."""
        message_index = message_index or len(st.session_state.messages)
        sql_markdown = 'No SQL returned!'
        if request_id:
            with st.expander("Request ID", expanded=False):
                st.markdown(request_id)
        for item in content:
            if item["type"] == "text":
                st.markdown(item["text"])
            elif item["type"] == "suggestions":
                with st.expander("Suggestions", expanded=True):
                    for suggestion_index, suggestion in enumerate(item["suggestions"]):
                        if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                            st.session_state.active_suggestion = suggestion
            elif item["type"] == "sql":
                sql_markdown = self.execute_sql(sql = item["statement"])

        return sql_markdown

    # @st.cache_data
    def execute_sql(self, sql: str) -> None:
        with st.expander("SQL Query", expanded=False):
            st.code(sql, language="sql")
        with st.expander("Results", expanded=True):
            with st.spinner("Running SQL..."):
                session = get_active_session()
                df = session.sql(sql).to_pandas()
                if len(df.index) > 1:
                    data_tab, line_tab, bar_tab = st.tabs(
                        ["Data", "Line Chart", "Bar Chart"]
                    )
                    data_tab.dataframe(df)
                    if len(df.columns) > 1:
                        df = df.set_index(df.columns[0])
                    with line_tab:
                        st.line_chart(df)
                    with bar_tab:
                        st.bar_chart(df)
                else:
                    st.dataframe(df)

        return df.to_markdown(index=False)
    
    def search_prompt(self, prompt: str):

        # fetch service
        search_service = (root
        	.databases[DATABASE]
        	.schemas[SCHEMA]
        	.cortex_search_services["PROMPT_SEARCH_CUSTOMER_LTV"]
        )
        
        # query service
        resp = search_service.search(
        	query= prompt,
        	columns=["USER_QUERY", "LLM_PROMPT"],
        	limit=1,
        )

        st.write("Using following retrieved prompt for summarization")
        st.write(resp.results[0]['LLM_PROMPT'])


        return resp.results
            

    def summarize_sql_results(self, query: str) -> str:
        sql_result = self.process_api_response(query)
        prompt_to_use = self.search_prompt(query)
        st.write("**Summarizing result...**")
        summarized_result = complete(st.session_state.selected_summarization_model, 
                                     f'''{prompt_to_use}
                                     User query  - {query}
                                     Sql result markdown - {sql_result}''')
        st.write(f"**{summarized_result}**")
        return summarized_result


#instantiate class
CA = CortexAnalyst()


def show_conversation_history() -> None:
    for message_index, message in enumerate(st.session_state.messages):
        chat_role = "assistant" if message["role"] == "analyst" else "user"
        with st.chat_message(chat_role):
               try:
                   CA.process_sql(
                        content=message["content"],
                        request_id=message.get("request_id"),
                        message_index=message_index,
                    )
               except: 
                   st.write("No history found!")


def reset() -> None:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None



st.title(f":snowflake: Text to SQL Assistant with Snowflake Cortex :snowflake:")

st.markdown(f"Semantic Model: `{FILE}`")

if "messages" not in st.session_state:
    reset()

with st.sidebar:
    if st.button("Reset conversation"):
        reset()

show_conversation_history()

if user_input := st.chat_input("What is your question?"):
        CA.summarize_sql_results(query=user_input)
    
if st.session_state.active_suggestion:
    CA.summarize_sql_results(query=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None
