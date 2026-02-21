import streamlit as st
import zipfile
import io
from datetime import datetime

st.set_page_config(
    page_title="Snowflake Initializr",
    page_icon=":material/ac_unit:",
    layout="wide",
)

PROJECT_TYPES = {
    "Streamlit App": {
        "icon": ":material/dashboard:",
        "description": "Interactive web application with Streamlit in Snowflake",
        "files": ["app.py", "environment.yml"],
    },
    "Cortex Agent": {
        "icon": ":material/smart_toy:",
        "description": "AI-powered agent using Snowflake Cortex",
        "files": ["agent.py", "tools.py", "environment.yml"],
    },
    "Snowpark Python": {
        "icon": ":material/code:",
        "description": "Data processing with Snowpark Python",
        "files": ["main.py", "utils.py", "environment.yml"],
    },
    "Native App": {
        "icon": ":material/apps:",
        "description": "Snowflake Native Application package",
        "files": ["manifest.yml", "setup_script.sql", "readme.md"],
    },
    "dbt Project": {
        "icon": ":material/transform:",
        "description": "Data transformation with dbt on Snowflake",
        "files": ["dbt_project.yml", "profiles.yml", "models/example.sql"],
    },
    "Notebook": {
        "icon": ":material/note:",
        "description": "Jupyter notebook for Snowflake analysis",
        "files": ["analysis.ipynb", "environment.yml"],
    },
}

DEPENDENCIES = {
    "Data & ML": [
        {"name": "snowflake-ml-python", "desc": "Snowflake ML library for model training and registry", "default": False},
        {"name": "pandas", "desc": "Data manipulation and analysis", "default": True},
        {"name": "numpy", "desc": "Numerical computing", "default": False},
        {"name": "scikit-learn", "desc": "Machine learning algorithms", "default": False},
        {"name": "xgboost", "desc": "Gradient boosting framework", "default": False},
    ],
    "Visualization": [
        {"name": "altair", "desc": "Declarative statistical visualization", "default": False},
        {"name": "plotly", "desc": "Interactive graphing library", "default": False},
        {"name": "matplotlib", "desc": "Static plotting library", "default": False},
    ],
    "AI & LLM": [
        {"name": "snowflake-cortex", "desc": "Cortex AI functions (LLM, embeddings)", "default": False},
        {"name": "langchain", "desc": "LLM application framework", "default": False},
    ],
    "Utilities": [
        {"name": "pyyaml", "desc": "YAML parser and emitter", "default": False},
        {"name": "requests", "desc": "HTTP library", "default": False},
        {"name": "pydantic", "desc": "Data validation using Python type hints", "default": False},
    ],
}


def generate_environment_yml(project_name: str, python_version: str, selected_deps: list[str]) -> str:
    deps = ["snowflake-snowpark-python"] + selected_deps
    deps_str = "\n".join(f"  - {d}" for d in deps)
    return f"""name: {project_name}
channels:
  - snowflake
dependencies:
  - python={python_version}
{deps_str}
"""


def generate_streamlit_app(project_name: str, description: str, include_sidebar: bool, include_snowflake: bool) -> str:
    sidebar_code = '''
with st.sidebar:
    st.selectbox("Select option", ["Option 1", "Option 2", "Option 3"])
    st.date_input("Select date")
''' if include_sidebar else ""
    
    snowflake_code = '''
from snowflake.snowpark.context import get_active_session

session = get_active_session()

@st.cache_data
def load_data():
    return session.sql("SELECT CURRENT_TIMESTAMP() as ts").to_pandas()

df = load_data()
st.dataframe(df)
''' if include_snowflake else '''
st.write("Hello from your new Streamlit app!")
'''
    
    return f'''import streamlit as st

st.set_page_config(
    page_title="{project_name}",
    page_icon=":material/ac_unit:",
    layout="wide",
)

st.title("{project_name}")
st.caption("{description}")
{sidebar_code}{snowflake_code}
'''


def generate_cortex_agent(project_name: str, description: str) -> tuple[str, str]:
    agent_py = f'''"""
{project_name} - Cortex Agent
{description}
"""
from snowflake.cortex import Complete
from tools import available_tools

SYSTEM_PROMPT = """You are a helpful assistant. Use the available tools to answer questions."""

def run_agent(user_message: str, model: str = "claude-3-5-sonnet") -> str:
    response = Complete(
        model=model,
        prompt=f"{{SYSTEM_PROMPT}}\\n\\nUser: {{user_message}}",
    )
    return response

if __name__ == "__main__":
    result = run_agent("Hello, what can you help me with?")
    print(result)
'''
    
    tools_py = '''"""Tool definitions for the Cortex Agent"""

def search_data(query: str) -> str:
    """Search for data in Snowflake tables"""
    return f"Search results for: {query}"

def analyze_data(table_name: str) -> str:
    """Analyze data from a specified table"""
    return f"Analysis of table: {table_name}"

available_tools = {
    "search_data": search_data,
    "analyze_data": analyze_data,
}
'''
    return agent_py, tools_py


def generate_snowpark_project(project_name: str, description: str) -> tuple[str, str]:
    main_py = f'''"""
{project_name} - Snowpark Python Project
{description}
"""
from snowflake.snowpark import Session
from utils import get_connection_parameters, transform_data

def main():
    connection_params = get_connection_parameters()
    session = Session.builder.configs(connection_params).create()
    
    df = session.table("SAMPLE_DATA")
    result = transform_data(df)
    result.show()
    
    session.close()

if __name__ == "__main__":
    main()
'''
    
    utils_py = '''"""Utility functions for Snowpark project"""
import os
from snowflake.snowpark import DataFrame

def get_connection_parameters() -> dict:
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }

def transform_data(df: DataFrame) -> DataFrame:
    """Apply transformations to the DataFrame"""
    return df.select("*")
'''
    return main_py, utils_py


def generate_native_app(project_name: str, description: str) -> tuple[str, str, str]:
    manifest = f'''manifest_version: 1

artifacts:
  setup_script: setup_script.sql
  readme: readme.md

privileges:
  - CREATE DATABASE:
      description: Required to install the application

configuration:
  log_level: INFO
'''
    
    setup_sql = f'''-- {project_name} Setup Script
-- {description}

CREATE APPLICATION ROLE IF NOT EXISTS app_user;

CREATE OR ALTER VERSIONED SCHEMA core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_user;

CREATE OR REPLACE PROCEDURE core.hello_world()
RETURNS STRING
LANGUAGE SQL
AS
$$
    SELECT 'Hello from {project_name}!'
$$;

GRANT USAGE ON PROCEDURE core.hello_world() TO APPLICATION ROLE app_user;
'''
    
    readme = f'''# {project_name}

{description}

## Installation

1. Create an application package
2. Upload files to a stage
3. Create the application

## Usage

```sql
CALL core.hello_world();
```
'''
    return manifest, setup_sql, readme


def generate_dbt_project(project_name: str, description: str, database: str, schema: str) -> tuple[str, str, str]:
    dbt_project = f'''name: '{project_name}'
version: '1.0.0'
config-version: 2

profile: 'snowflake'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

vars:
  start_date: '2024-01-01'
'''
    
    profiles = f'''snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{{{ env_var('SNOWFLAKE_ACCOUNT') }}}}"
      user: "{{{{ env_var('SNOWFLAKE_USER') }}}}"
      password: "{{{{ env_var('SNOWFLAKE_PASSWORD') }}}}"
      role: "{{{{ env_var('SNOWFLAKE_ROLE') }}}}"
      database: {database}
      warehouse: "{{{{ env_var('SNOWFLAKE_WAREHOUSE') }}}}"
      schema: {schema}
      threads: 4
'''
    
    example_model = f'''-- {project_name} example model
-- {description}

{{{{ config(materialized='view') }}}}

SELECT
    CURRENT_TIMESTAMP() AS created_at,
    '{project_name}' AS project_name
'''
    return dbt_project, profiles, example_model


def generate_notebook(project_name: str, description: str) -> str:
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"# {project_name}\n\n{description}"]
            },
            {
                "cell_type": "code",
                "metadata": {},
                "source": [
                    "from snowflake.snowpark.context import get_active_session\n",
                    "\n",
                    "session = get_active_session()\n",
                    "session.sql('SELECT CURRENT_TIMESTAMP()').show()"
                ],
                "execution_count": None,
                "outputs": []
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## Data Analysis\n\nAdd your analysis code below."]
            },
            {
                "cell_type": "code",
                "metadata": {},
                "source": [
                    "import pandas as pd\n",
                    "\n",
                    "# Load your data here\n",
                    "# df = session.table('YOUR_TABLE').to_pandas()"
                ],
                "execution_count": None,
                "outputs": []
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    import json
    return json.dumps(notebook, indent=2)


def create_zip(files: dict[str, str], project_name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files.items():
            zf.writestr(f"{project_name}/{filename}", content)
    buffer.seek(0)
    return buffer.getvalue()


st.title("Snowflake Initializr")
st.caption("Generate starter projects for Snowflake applications")

col_left, col_right = st.columns([2, 1])

with col_left:
    with st.container(border=True):
        st.subheader("Project metadata")
        
        c1, c2 = st.columns(2)
        with c1:
            project_name = st.text_input("Project name", value="my-snowflake-app", help="Name of your project")
            database = st.text_input("Database", value="MY_DATABASE", help="Target Snowflake database")
        with c2:
            artifact = st.text_input("Artifact", value="my_snowflake_app", help="Artifact identifier (no spaces/hyphens)")
            schema = st.text_input("Schema", value="PUBLIC", help="Target Snowflake schema")
        
        description = st.text_input("Description", value="A Snowflake application", help="Brief description of your project")
    
    with st.container(border=True):
        st.subheader("Project type")
        
        selected_type = st.radio(
            "Select project type",
            options=list(PROJECT_TYPES.keys()),
            format_func=lambda x: f"{PROJECT_TYPES[x]['icon']} {x}",
            horizontal=True,
            label_visibility="collapsed",
        )
        st.caption(PROJECT_TYPES[selected_type]["description"])
    
    with st.container(border=True):
        st.subheader("Python version")
        python_version = st.radio(
            "Python version",
            options=["3.11", "3.10", "3.9"],
            horizontal=True,
            label_visibility="collapsed",
        )
    
    if selected_type == "Streamlit App":
        with st.container(border=True):
            st.subheader("Streamlit options")
            c1, c2 = st.columns(2)
            with c1:
                include_sidebar = st.checkbox("Include sidebar", value=True)
            with c2:
                include_snowflake = st.checkbox("Include Snowflake connection", value=True)

with col_right:
    with st.container(border=True):
        st.subheader("Dependencies")
        search = st.text_input("Search dependencies", placeholder="pandas, scikit-learn...", label_visibility="collapsed")
        
        selected_deps = []
        for category, deps in DEPENDENCIES.items():
            filtered = [d for d in deps if not search or search.lower() in d["name"].lower() or search.lower() in d["desc"].lower()]
            if filtered:
                with st.expander(f"{category} ({len(filtered)})", expanded=not search):
                    for dep in filtered:
                        if st.checkbox(dep["name"], value=dep["default"], key=f"dep_{dep['name']}", help=dep["desc"]):
                            selected_deps.append(dep["name"])

st.space("medium")

with st.container(horizontal=True, horizontal_alignment="center"):
    generate_btn = st.button("Generate", type="primary", icon=":material/download:", use_container_width=False)
    explore_btn = st.button("Explore", icon=":material/search:", use_container_width=False)

if generate_btn:
    files = {}
    
    if selected_type == "Streamlit App":
        files["app.py"] = generate_streamlit_app(project_name, description, include_sidebar, include_snowflake)
        files["environment.yml"] = generate_environment_yml(artifact, python_version, selected_deps + ["streamlit"])
    
    elif selected_type == "Cortex Agent":
        agent_py, tools_py = generate_cortex_agent(project_name, description)
        files["agent.py"] = agent_py
        files["tools.py"] = tools_py
        files["environment.yml"] = generate_environment_yml(artifact, python_version, selected_deps)
    
    elif selected_type == "Snowpark Python":
        main_py, utils_py = generate_snowpark_project(project_name, description)
        files["main.py"] = main_py
        files["utils.py"] = utils_py
        files["environment.yml"] = generate_environment_yml(artifact, python_version, selected_deps)
    
    elif selected_type == "Native App":
        manifest, setup_sql, readme = generate_native_app(project_name, description)
        files["manifest.yml"] = manifest
        files["setup_script.sql"] = setup_sql
        files["readme.md"] = readme
    
    elif selected_type == "dbt Project":
        dbt_proj, profiles, example = generate_dbt_project(project_name, description, database, schema)
        files["dbt_project.yml"] = dbt_proj
        files["profiles.yml"] = profiles
        files["models/example.sql"] = example
    
    elif selected_type == "Notebook":
        files["analysis.ipynb"] = generate_notebook(project_name, description)
        files["environment.yml"] = generate_environment_yml(artifact, python_version, selected_deps)
    
    zip_data = create_zip(files, artifact)
    
    st.download_button(
        label=f"Download {artifact}.zip",
        data=zip_data,
        file_name=f"{artifact}.zip",
        mime="application/zip",
        icon=":material/folder_zip:",
    )
    st.success(f"Generated {selected_type} project with {len(files)} files", icon=":material/check_circle:")

if explore_btn:
    st.subheader("Project preview")
    
    preview_files = {}
    if selected_type == "Streamlit App":
        preview_files["app.py"] = generate_streamlit_app(project_name, description, 
                                                          include_sidebar if selected_type == "Streamlit App" else False,
                                                          include_snowflake if selected_type == "Streamlit App" else False)
        preview_files["environment.yml"] = generate_environment_yml(artifact, python_version, selected_deps + ["streamlit"])
    elif selected_type == "Cortex Agent":
        agent_py, tools_py = generate_cortex_agent(project_name, description)
        preview_files["agent.py"] = agent_py
        preview_files["tools.py"] = tools_py
    elif selected_type == "Snowpark Python":
        main_py, utils_py = generate_snowpark_project(project_name, description)
        preview_files["main.py"] = main_py
        preview_files["utils.py"] = utils_py
    elif selected_type == "Native App":
        manifest, setup_sql, readme = generate_native_app(project_name, description)
        preview_files["manifest.yml"] = manifest
        preview_files["setup_script.sql"] = setup_sql
    elif selected_type == "dbt Project":
        dbt_proj, profiles, example = generate_dbt_project(project_name, description, database, schema)
        preview_files["dbt_project.yml"] = dbt_proj
        preview_files["models/example.sql"] = example
    elif selected_type == "Notebook":
        preview_files["analysis.ipynb"] = generate_notebook(project_name, description)
    
    tabs = st.tabs(list(preview_files.keys()))
    for tab, (filename, content) in zip(tabs, preview_files.items()):
        with tab:
            lang = "python" if filename.endswith(".py") else "yaml" if filename.endswith((".yml", ".yaml")) else "sql" if filename.endswith(".sql") else "json" if filename.endswith(".ipynb") else "markdown"
            st.code(content, language=lang)
