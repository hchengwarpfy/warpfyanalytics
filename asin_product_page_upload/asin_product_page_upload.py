import pandas as pd
import time
import psycopg2
import psycopg2.extras
from cred import data



asin_product_page = pd.read_csv('asin_product_page.csv')
asin_product_page = asin_product_page.fillna(0)
asin_product_page['review_count'] = asin_product_page['review_count'].str.replace(',', '').str.replace('rating', '')\
.astype(float).apply(lambda x: round(x, 2))
asin_product_page['product_length'] = asin_product_page['product_length'].apply(lambda x: round(x, 2))
asin_product_page['product_width'] = asin_product_page['product_width'].apply(lambda x: round(x, 2))
asin_product_page['product_height'] = asin_product_page['product_height'].str.replace('"', '').\
astype(float).apply(lambda x: round(x, 2))
asin_product_page['product_weight'] = asin_product_page['product_weight'].apply(lambda x: round(x, 2))
asin_product_page['package_length'] = asin_product_page['package_length'].apply(lambda x: round(x, 2))
asin_product_page['package_width'] = asin_product_page['package_width'].apply(lambda x: round(x, 2))
asin_product_page['package_height'] = asin_product_page['package_height'].apply(lambda x: round(x, 2))
asin_product_page['rating'] = asin_product_page['rating'].apply(lambda x: round(x, 2))
asin_product_page['sale_price'] = asin_product_page['sale_price'].str.replace(',', '').\
astype(float).apply(lambda x: round(x, 2))
asin_product_page['regular_price'] = asin_product_page['regular_price'].str.replace(',', '').\
astype(float).apply(lambda x: round(x, 2))
asin_product_page['shipping_charge'] = asin_product_page['shipping_charge'].apply(lambda x: round(x, 2))
asin_product_page['available_quantity'] = asin_product_page['available_quantity'].apply(lambda x: round(x, 2))
asin_product_page['product_reviews'] = asin_product_page['product_reviews'].apply(lambda x: round(x, 2))
asin_product_page = asin_product_page.drop_duplicates(subset='asin', keep='first')



TABLE_NAME = 'asin_product_page'



values = []
for i, row in asin_product_page.iterrows():
    values.append((row['url'], row['asin'], row['product_length'], row['product_width'], row['product_height'],
                                row['product_dimension_unit'], row['product_weight'], row['product_weight_unit'],
                                row['package_length'], row['package_width'], row['package_height'], row['category'], row['name'],
                                row['brand'], row['seller'], row['seller_url'], row['review_count'], row['rating'], row['currency'],
                                row['sale_price'], row['regular_price'], row['shipping_charge'], row['small_description'],
                                row['full_description'], row['availability_status'], row['available_quantity'],
                                row['model'], row['attributes'], row['product_category'], row['product_information'],
                                row['highlighted_specifications'], row['image_urls'], row['variation_asin'],
                                row['product_variations'], row['frequently_bought_together'], row['product_reviews'],
                                row['sponsored_products'], row['rating_histogram']))


def measure_time(func):
    def time_it(*args, **kwargs):
        time_started = time.time()
        func(*args, **kwargs)
        time_elapsed = time.time()
        print("{execute} running time is {sec} seconds for inserting {rows} rows.".format(execute=func.__name__,
                                                                                          sec=round(
                                                                                              time_elapsed - time_started,
                                                                                              4), rows=len(
                kwargs.get('values'))))

    return time_it





class PsycopgTest():

    def connect(self):
        conn_string = "host={0} user={1} dbname={2} password={3}".format(data['host'],
                                                                         data['user'],
                                                                         data['database'], data['password'])
        self.connection = psycopg2.connect(conn_string)
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.cursor.execute(
            '''CREATE TABLE IF NOT EXISTS {table} (url varchar,
           asin varchar UNIQUE,
           Product_Length NUMERIC(11,2),
           Product_Width NUMERIC,
           Product_Height NUMERIC,
           Product_Dimension_Unit varchar,
           Product_Weight NUMERIC(11,2),
           Product_Weight_Unit TEXT,
           Package_Length NUMERIC(11,2),
           Package_Width NUMERIC(11,2),
           Package_Height NUMERIC(11,2),
           Category varchar,
           name varchar,
           brand varchar,
           seller varchar,
           seller_url varchar,
           review_count NUMERIC(17,2),
           rating  NUMERIC(11,2),
           currency varchar,
           sale_price NUMERIC(11,2),
           regular_price NUMERIC(11,2),
           shipping_charge NUMERIC(11,2),
           small_description varchar,
           full_description varchar,
           availability_status varchar,
           available_quantity NUMERIC(11,2),
           model varchar,
           attributes varchar,
           product_category varchar,
           product_information varchar,
           highlighted_specifications varchar,
           image_urls varchar,
           variation_asin varchar,
           product_variations varchar,
           frequently_bought_together varchar,
           product_reviews varchar,
           sponsored_products varchar,
           rating_histogram varchar)'''.format(table=TABLE_NAME))
        self.connection.commit()

    def truncate_table(self):
        self.cursor.execute("TRUNCATE TABLE {table} RESTART IDENTITY".format(table=TABLE_NAME))
        self.connection.commit()
        
    @measure_time
    def method_execute_batch(self, values):
        psycopg2.extras.execute_batch(self.cursor, '''INSERT INTO {table} VALUES (%s,%s,%s,%s,
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
                           %s,%s,
                           %s,%s,
                           %s,%s)'''.format(table=TABLE_NAME),
                                      values)
        self.connection.commit()

        
        
def main():
    psyco = PsycopgTest()
    psyco.connect()
    psyco.create_table()
    psyco.truncate_table()
    psyco.method_execute_batch(values=values)
    
if __name__ == '__main__':
    main()
