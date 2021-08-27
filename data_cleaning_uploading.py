import pandas as pd
from cred import data
from sqlalchemy import create_engine
import psycopg2
import time

conn = psycopg2.connect(host=data['host'], dbname=data['database'],
                        user=data['user'], password=data['password'])
cursor = conn.cursor()

cursor.execute("""SELECT relname FROM pg_class WHERE relkind='r'
                  AND relname !~ '^(pg_|sql_)';""") # "rel" is short for relation.

tables = [i[0] for i in cursor.fetchall()] # A list() of tables.


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
    
    master_keywords = master_keywords.fillna(0)

    
    master_keywords.to_csv('master_keywords_marketinsights_cleaned.csv', index = False, index_label = True, sep = ',')
    
def master_keywords_searchinfo_cleaned_data(master_keywords_searchinfo_file):
    
    master_keywords_searchinfo = pd.read_excel(master_keywords_searchinfo_file)
    master_keywords_searchinfo['Keyword'] = master_keywords_searchinfo['Keyword'].str.strip()
    master_keywords_searchinfo['Date'] = pd.to_datetime(master_keywords_searchinfo['Date'])
    master_keywords_searchinfo['SponsoredRank'] = master_keywords_searchinfo['SponsoredRank'].str.replace('x', '')
    master_keywords_searchinfo['OrganicRank'] = master_keywords_searchinfo['OrganicRank'].str.replace('x', '')
    master_keywords_searchinfo = master_keywords_searchinfo.fillna(0)
    master_keywords_searchinfo.to_csv('master_keywords_searchinfo_cleaned.csv', index=False, index_label = True, sep = ',')
    
    

def rename_columns_cleaned_files(query, cleaned_csv_file, filename):
    cursor.execute(query)

    # Extract the column names
    col_names = []
    for elt in cursor.description:
        col_names.append(elt[0])

    renamed_df = pd.read_csv(cleaned_csv_file)
    renamed_df.columns = col_names
    renamed_df.to_csv(filename, index=False, sep = ',')
        
def rename_columns_original_files(query, original_xlsx_file, filename):
    cursor.execute(query)

    # Extract the column names
    col_names = []
    for elt in cursor.description:
        col_names.append(elt[0])

    renamed_df = pd.read_excel(original_xlsx_file)
    renamed_df.columns = col_names
    renamed_df.to_csv(filename, index=False, sep = ',')
    
def copy_csv_files(file_name, table_name):
    with open(file_name, 'r') as f:
        next(f)
        cursor.copy_from(f, table_name, sep=',')

    conn.commit()
    

curr = conn.cursor()
def insert_market_insights(curr, asin, keyword, market_lifetime, avg_sales_listed,
                     orders_per_month, avg_sales_per_month, active_competing_products,
                     avg_price, estimated_profit_margin, estimated_cogs,
                     market_maturity, market_capacity, market_competition,
                     market_fund, market_concentration, market_activeness,
                     sales_attribution_reviews, sales_attribution_price, 
                     sales_attribution_product_video, sales_attribution_aplustcontent,
                     sales_attribution_coupon, sales_attribution_time_listed,
                     sales_attribution_customer_rating, sales_attribution_variation,
                     sales_attribution_seller_type, sales_attribution_others, 
                     top10_order_per_day, top20_order_per_day, top50_order_per_day,
                     top100_order_per_day, top10_min_reviews, top20_min_reviews,
                     top50_min_reviews, top100_min_reviews):
    
    insert_into_insights = ("""INSERT INTO master_keywords_market_insights 
                    (asin, keyword, market_lifetime, avg_sales_listed,
                     orders_per_month, avg_sales_per_month, active_competing_products,
                     avg_price, estimated_profit_margin, estimated_cogs,
                     market_maturity, market_capacity, market_competition,
                     market_fund, market_concentration, market_activeness,
                     sales_attribution_reviews, sales_attribution_price, 
                     sales_attribution_product_video, sales_attribution_aplustcontent,
                     sales_attribution_coupon, sales_attribution_time_listed,
                     sales_attribution_customer_rating, sales_attribution_variation,
                     sales_attribution_seller_type, sales_attribution_others, 
                     top10_order_per_day, top20_order_per_day, top50_order_per_day,
                     top100_order_per_day, top10_min_reviews, top20_min_reviews,
                     top50_min_reviews, top100_min_reviews)
                    VALUES(%s,%s,%s,%s,
                           %s,%s,%s,
                           %s,%s,%s,
                           %s,%s,%s,
                           %s,%s,%s,
                           %s,%s,
                           %s,%s,
                           %s,%s,
                           %s,%s,
                           %s,%s,
                           %s,%s,%s,
                           %s,%s,%s,
                           %s,%s);""")
    row_to_insert = (asin, keyword, market_lifetime, avg_sales_listed,
                     orders_per_month, avg_sales_per_month, active_competing_products,
                     avg_price, estimated_profit_margin, estimated_cogs,
                     market_maturity, market_capacity, market_competition,
                     market_fund, market_concentration, market_activeness,
                     sales_attribution_reviews, sales_attribution_price, 
                     sales_attribution_product_video, sales_attribution_aplustcontent,
                     sales_attribution_coupon, sales_attribution_time_listed,
                     sales_attribution_customer_rating, sales_attribution_variation,
                     sales_attribution_seller_type, sales_attribution_others, 
                     top10_order_per_day, top20_order_per_day, top50_order_per_day,
                     top100_order_per_day, top10_min_reviews, top20_min_reviews,
                     top50_min_reviews, top100_min_reviews)
    
    curr.execute(insert_into_insights, row_to_insert)


def append_from_insights_df(curr, insights_df):
    for i, row in insights_df.iterrows():
        insert_market_insights(curr, row['asin'],row['keyword'],row['market_lifetime'],
                             row['avg_sales_listed'],row['orders_per_month'],row['avg_sales_per_month'],
                             row['active_competing_products'],row['avg_price'],row['estimated_profit_margin'],
                             row['estimated_cogs'],row['market_maturity'],row['market_capacity'],
                             row['market_competition'],row['market_fund'],row['market_concentration'],
                             row['market_activeness'],row['sales_attribution_reviews'],row['sales_attribution_price'],
                             row['sales_attribution_product_video'],row['sales_attribution_aplustcontent'],
                             row['sales_attribution_coupon'],row['sales_attribution_time_listed'],
                             row['sales_attribution_customer_rating'],row['sales_attribution_variation'],
                             row['sales_attribution_seller_type'],row['sales_attribution_others'],
                             row['top10_order_per_day'],row['top20_order_per_day'],row['top50_order_per_day'],
                             row['top100_order_per_day'],row['top10_min_reviews'],row['top20_min_reviews'],
                             row['top50_min_reviews'],row['top100_min_reviews'])
        
if __name__ == '__main__':
    master_keywords_cleaned_data('/Users/venkatasai/Desktop/data_sanitizing/sample - master_keywords_marketinsights.xlsx')
    master_keywords_searchinfo_cleaned_data('/Users/venkatasai/Desktop/data_sanitizing/sample - master_keywords_searchinfo.xlsx')
    
    rename_columns_cleaned_files('SELECT * FROM master_keywords_market_insights;', 'master_keywords_marketinsights_cleaned.csv',
                                                              'master_keywords_marketinsights_renamed.csv')

    rename_columns_cleaned_files('SELECT * FROM master_query_searchinfo;', 'master_keywords_searchinfo_cleaned.csv',
                                'master_keywords_searchinfo_renamed.csv')
    
    rename_columns_original_files('SELECT * FROM asin_category;', 'sample - product category table.xlsx',
                                 'asin_category.csv')
    
    rename_columns_original_files('SELECT * FROM asin_product_page;', 'sample - Amazon Product Page By ASIN.xlsx',
                                 'asin_product_page.csv')
    
    copy_csv_files('asin_category.csv', 'asin_category')
    copy_csv_files('master_keywords_searchinfo_renamed.csv', 'master_query_searchinfo')
    
    market_insights_df = pd.read_csv('master_keywords_marketinsights_renamed.csv')
    append_from_insights_df(curr, market_insights_df)
    conn.commit()