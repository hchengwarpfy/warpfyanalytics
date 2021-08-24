import pandas as pd
import numpy as np


def master_keywords_cleaned_data(master_keywords_file):
    
    master_keywords = pd.read_excel(master_keywords_file)
    
    master_keywords['MARKET LIFETIME'] = master_keywords['MARKET LIFETIME'].replace("--", "0 days").\
    map(lambda x: x.split(' ')[0]).str.replace(',', '').astype(int)

    master_keywords['AVG SALES LISTED'] = master_keywords['AVG SALES LISTED'].replace("--", "0 days").\
    map(lambda x: x.split(' ')[0]).str.replace(',', '').astype(int)

    master_keywords['ORDERS/MONTH'] = master_keywords['ORDERS/MONTH'].replace("--", "0").astype(str).\
    str.replace(',', '').astype(int)

    master_keywords['AVG SALES/MONTH'] = master_keywords['AVG SALES/MONTH'].replace("--", "0").astype(str).\
    str.replace(',', '').str.replace('$', '', regex=True).astype(int)

    master_keywords['ACTIVE COMPETING PRODUCTS'] = master_keywords['ACTIVE COMPETING PRODUCTS'].replace("--", "0").\
    astype(str).str.replace(',', '').astype(int)

    master_keywords['AVG PRICE'] = master_keywords['AVG PRICE'].replace("--", "0").\
    astype(str).str.replace('$', '', regex=True).\
    astype(float).apply(lambda x:round(x,2))

    master_keywords['Market Concentration'] = master_keywords['Market Concentration'].replace("--", "0").astype(str).\
    str.replace('%', '', regex=True).astype(float).\
    apply(lambda x:round(x,2))

    master_keywords['Market Activeness'] = master_keywords['Market Activeness'].replace("--", "0").astype(str).astype(int)

    master_keywords['Reviews'] = master_keywords['Reviews'].replace("--", "0").astype(str).\
    str.replace('%', '', regex=True).astype(float)

    master_keywords['Price'] = master_keywords['Price'].replace("--", "0").\
    str.replace('%', '', regex=True).astype(float).apply(lambda x:round(x,2))

    master_keywords['Product video'] = master_keywords['Product video'].replace("--", "0").\
    str.replace('%', '', regex=True).astype(float).apply(lambda x:round(x,2))

    master_keywords['A+ content'] = master_keywords['A+ content'].replace("--", "0").\
    str.replace('%', '', regex=True).astype(float).apply(lambda x:round(x,2))

    master_keywords['Coupon'] = master_keywords['Coupon'].replace("--", "0").\
    astype(float).apply(lambda x:round(x,2))

    master_keywords['Time listed'] = master_keywords['Time listed'].replace("--", "0").\
    astype(float).apply(lambda x:round(x,2))

    master_keywords['Customer rating'] = master_keywords['Customer rating'].replace("--", "0").\
    str.replace('%', '', regex=True).astype(float).apply(lambda x:round(x,2))

    master_keywords['Variation'] = master_keywords['Variation'].replace("--", "0").\
    str.replace('%', '', regex=True).astype(float).apply(lambda x:round(x,2))

    master_keywords['Seller type'] = master_keywords['Seller type'].replace("--", "0").\
    str.replace('%', '', regex=True).astype(float).apply(lambda x:round(x,2))

    master_keywords['Others'] = master_keywords['Others'].replace("--", "0").\
    str.replace('%', '', regex=True).astype(float).apply(lambda x:round(x,2))

    master_keywords['TOP10 Orders/Day'] = master_keywords['TOP10 Orders/Day'].replace("--", 0).astype(int)
    master_keywords['TOP20 Orders/Day'] = master_keywords['TOP20 Orders/Day'].replace("--", 0).astype(int)
    master_keywords['TOP50 Orders/Day'] = master_keywords['TOP50 Orders/Day'].replace("--", 0).astype(int)
    master_keywords['TOP100 Orders/Day'] = master_keywords['TOP100 Orders/Day'].replace("--", 0).astype(int)

    master_keywords['TOP10 Min. Reviews'] = master_keywords['TOP10 Min. Reviews'].replace("--", 0).\
    astype(str).str.replace(',', '').astype(int)

    master_keywords['TOP20 Min. Reviews'] = master_keywords['TOP20 Min. Reviews'].replace("--", 0).\
    astype(str).str.replace(',', '').astype(int)

    master_keywords['TOP50 Min. Reviews'] = master_keywords['TOP50 Min. Reviews'].replace("--", 0).\
    astype(str).str.replace(',', '').astype(int)

    master_keywords['TOP100 Min. Reviews'] = master_keywords['TOP100 Min. Reviews'].replace("--", 0).\
    astype(str).str.replace(',', '').astype(int)
    
    master_keywords.to_csv('master_keywords_marketinsights_cleaned.csv', index = False)
    
    
def master_keywords_searchinfo_cleaned_data(master_keywords_searchinfo_file):
    
    master_keywords_searchinfo = pd.read_excel(master_keywords_searchinfo_file)
    master_keywords_searchinfo['Keyword'] = master_keywords_searchinfo['Keyword'].str.strip()
    master_keywords_searchinfo['Date'] = pd.to_datetime(master_keywords_searchinfo['Date'])
    master_keywords_searchinfo['SponsoredRank'] = master_keywords_searchinfo['SponsoredRank'].str.replace('x', '')
    master_keywords_searchinfo['OrganicRank'] = master_keywords_searchinfo['OrganicRank'].str.replace('x', '')
    master_keywords_searchinfo.to_csv('master_keywords_searchinfo_cleaned.csv', index=False)
    
    


if __name__ == "__main__":
    master_keywords_cleaned_data('/Users/venkatasai/Desktop/data_sanitizing/sample - master_keywords_marketinsights.xlsx')
    master_keywords_searchinfo_cleaned_data('/Users/venkatasai/Desktop/data_sanitizing/sample - master_keywords_searchinfo.xlsx')
