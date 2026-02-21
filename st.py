import streamlit as st
import pandas as pd

st.title('Banking insights')

customer = pd.read_csv("customers.csv")
accounts = pd.read_csv('accounts.csv')
loans = pd.read_json('loans.json')
branches = pd.read_json('branches.json',lines=True)
transactions = pd.read_csv('transactions.csv')
credit_card = pd.read_json('credit_cards.json')
support_tickets = pd.read_json('support_tickets.json')

Tables = {
    "Customers": customer,
    "Accounts": accounts,
    "Loans": loans,
    "Branches": branches,
    "Transactions": transactions,
    "Credit Cards": credit_card,
    "Support Tickets": support_tickets
}

Show_Table = st.selectbox('Show table', list(Tables.keys()))
st.dataframe(Tables[Show_Table])
"""
Show_Table = st.selectbox('Show table', list(Tables.keys()))
"""