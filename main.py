import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os, uuid

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv

st.set_page_config(page_title="Financial Dashboard", page_icon=":money_with_wings:", layout="wide")

# load .env file
load_dotenv()

catagory_file = os.getenv("CATEGORY_FILE")
blob_container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
blob_name = os.getenv("AZURE_STORAGE_BLOB_NAME")

if "categories" not in st.session_state:
    st.session_state.categories = {
        "Uncategorised": [],
    }

if os.path.exists(catagory_file):
    with open(catagory_file, "r") as f:
        st.session_state.categories = json.load(f)

def save_categories():
    with open(catagory_file, "w") as f:
        json.dump(st.session_state.categories, f)

def connect_to_azure_blob_storage():
    # Read environment variables
    blob_cnn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    print(f"Blob connection string: {blob_cnn_str}")
    print(f"Blob container name: {blob_container_name}")
    
    # Check if the connection string and container name are set
    if not blob_cnn_str or not blob_container_name:
        print("Azure Storage connection string or container name not set.")
        return None
    try:
        # Use DefaultAzureCredential to authenticate
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=blob_cnn_str,
            credential=credential
        )
        return blob_service_client
    except Exception as e:
        print(f"Error connecting to Azure Blob Storage: {e}")
        return None
    
def categorise_transactions(df):
    df["Category"] = "Uncategorised"
    for category, keywords in st.session_state.categories.items():
        if category == "Uncategorised" or not keywords:
            continue
        
        lowered_keywords = [keyword.lower().strip() for keyword in keywords]
        

        for indx, row in df.iterrows():
            details = row["Narrative"].lower().strip()
            if details in lowered_keywords:
                df.at[indx, "Category"] = category
    return df

def load_transaction_data(file_path):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        # Convert the 'Date' column to datetime format
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
        # st.write(df) <- debugging line
        return categorise_transactions(df)
    except Exception as e:
        st.error(f"Error procesisng file: {str(e)}")
        return None

def add_keywords_to_category(category, keyword):
    keyword = keyword.strip()
    if keyword and keyword not in st.session_state.categories[category]:
        st.session_state.categories[category].append(keyword)
        save_categories()
        return True
    
    return False

def main():

    st.title("Financial Dashboard")
    # Connect to Azure Blob Storage
    blob_service_client = connect_to_azure_blob_storage()
    if not blob_service_client:
        return
    
    # Download a file from Azure Blob Storage
    container_name = blob_container_name
    print(f"Blob name: {blob_name}")
 
    local_file_path = os.path.join(os.getcwd(), blob_name)
    try:
        # Create a container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Download the blob to a local file
        with open(local_file_path, "wb") as download_file:
            download_stream = container_client.download_blob(blob_name)
            download_file.write(download_stream.readall())
        
        print(f"Downloaded {blob_name} to {local_file_path}")
    except Exception as e:
        print(f"Error downloading blob: {e}")

    # Load the transaction data
    df = load_transaction_data(local_file_path)
    # create a df for debits where the column "Debit Amount" is positive
    if df is not None:
        df["Debit Amount"] = pd.to_numeric(df["Debit Amount"], errors='coerce')
        df["Credit Amount"] = pd.to_numeric(df["Credit Amount"], errors='coerce')
        df = df.fillna(0)
        debits_df = df[df["Debit Amount"] > 0].copy()
        credits_df = df[df["Credit Amount"] > 0].copy()
        st.session_state.debits_df = debits_df.copy()
        # Create a new column "Transaction Type" based on the "Debit Amount" and "Credit Amount"
        tab1, tab2 = st.tabs(["Expenses (Debits)", "Payments (Credits)"])
        with tab1:
            new_category = st.text_input("Add a new category")
            add_button = st.button("Add Category")

            if add_button and new_category:
                if new_category not in st.session_state.categories:
                    st.session_state.categories[new_category] = []
                    save_categories()
                    st.rerun()
            st.subheader("Your Expenses")
            edited_df = st.data_editor(
                st.session_state.debits_df[["Date", "Narrative", "Debit Amount", "Category"]],
                column_config={
                    "Data": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                    "Narrative": st.column_config.TextColumn("Narrative"),
                    "Debit Amount": st.column_config.NumberColumn("Debit Amount", format="%.2f AUD"),
                    "Category": st.column_config.SelectboxColumn(
                        "Category",
                        options=list(st.session_state.categories.keys()),
                        default="Uncategorised",
                    )
                },
                hide_index=True,
                use_container_width=True,
                key="category_editor"
            )

            save_button = st.button("Apply Changes", type="primary")
            if save_button:
                for idx, row in edited_df.iterrows():
                    new_category = row["Category"]
                    if new_category == st.session_state.debits_df.at[idx,"Category"]:
                        continue 

                    narrative = row["Narrative"]
                    st.session_state.debits_df.at[idx, "Category"] = new_category
                    add_keywords_to_category(new_category, narrative)

            st.subheader("Expense Summary")
            category_totals = st.session_state.debits_df.groupby("Category")["Debit Amount"].sum().reset_index()
            category_totals = category_totals.sort_values("Debit Amount", ascending=False)

            st.dataframe(
                category_totals,
                column_config={
                    "Debit Amount": st.column_config.NumberColumn("Debit Amount", format="%.2f AUD")
                },
                use_container_width=True,
                hide_index=True,
            )

            fig = px.pie(
                category_totals,
                values="Debit Amount",
                names="Category",
                title="Expense Distribution by Category",
                color_discrete_sequence=px.colors.sequential.RdBu,
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)


                    
        with tab2: 
            st.write(credits_df)

main()
