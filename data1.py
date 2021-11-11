# %%
import pandas as pd
import re
import numpy as np
from sql_engine import created_server
from sqlalchemy import create_engine
import urllib
import pyodbc

verbose = True
#list_misleading_products = ['M']
#list_adjusting_products = []
# %%
## read data from csv file and load them to df
df_fromFile=pd.read_csv('data.csv', encoding='ISO-8859-1')

if verbose:
    print(df_fromFile.head(),'\n\n')
    print(df_fromFile.columns,'\n\n')
    print(df_fromFile.shape,'\n\n')
print('data loaded!!!')
# %%
## create new columns

##datetime type columns
df_fromFile['InvoiceDate'] = pd.to_datetime(df_fromFile.InvoiceDate)
df_fromFile['monthYear'] = df_fromFile.apply(lambda x: str(x.InvoiceDate.month) + '-'+ str(x.InvoiceDate.year), axis= 1)
df_fromFile['YearQuater'] = df_fromFile.apply(lambda x: str(x.InvoiceDate.quarter) + '-'+ str(x.InvoiceDate.year), axis= 1)

## Sales column = price * unit
df_fromFile['Sales'] = df_fromFile.apply(lambda x: x.Quantity * x.UnitPrice, axis=1)
print('The columns are created!!!')
##===============================================================================================================
# %%
## checking NAN values
list_columns = ['StockCode','Quantity','InvoiceDate','UnitPrice', 'CustomerID', 'Country']
for col in list_columns:    
    nul_values_total = df_fromFile[col].isnull().sum()
    uniq_values = df_fromFile[col].unique()
    if nul_values_total > 0:
        print('!!!The column {} total of null values is {}'.format(col, nul_values_total))
    print('For a column {}  unique values {}'.format(col, len(uniq_values)))
##===============================================================================================================

# %%
def checkPrice(df):
    ##checking by negative price
    df_negative_Price = df.loc[df.UnitPrice < 0,:]
    print('Negative price df ',df_negative_Price.head())


    ## zero in price
    price_zero = df.loc[df.UnitPrice==0]
    price_zero1 = price_zero.groupby('Description').size()
    print('Nan description ', price_zero1)

##create a new df with price more than zero
df_1 = df_fromFile.loc[df_fromFile.UnitPrice>0].copy()
checkPrice(df_1)

# %%
##==================================================================================
##checking by negative quantity

##get list of products which have negative values
def checkQuantity(df):    
    df_negative_quantity = df.loc[df.Quantity < 0,:]
    df_negative_quantity_ByProduct = df_negative_quantity.groupby(['StockCode', 'Description'], as_index=False).size()
    df_negative_quantity_ByProduct = df_negative_quantity_ByProduct.sort_values('size', ascending=False)
    list_of_negative_prducts = df_negative_quantity_ByProduct.StockCode.to_list()
    return(list_of_negative_prducts)

##check products
list_misleading_products = ['M']  #list of products for further analising
#list_misleading_products = ['M']  #list of products for further checking
list_adjusting_products = [] # list of products which sales null or more than null by country (we can ignore it)
list_of_negative_products = checkQuantity(df_1)
for product in list_of_negative_products:
    if product in list_misleading_products or product in list_adjusting_products:
        continue
    #print(product)
    product_D = df_1.loc[df_1.StockCode == product,:]
    product_country = product_D.groupby('Country').agg({'Quantity':sum, 'Sales': sum})
    if (product_country.Quantity >= 0).all() & (product_country.Sales >= 0).all():
        list_adjusting_products.append(product)
    else:
        list_misleading_products.append(product)

print('Lenth adjustment list is ', len(list_adjusting_products))
print('Lenth misleading list is ', len(list_misleading_products))
print('Done!!')

# %%

## analise list_misleading_products
df_misleading_products = df_1.loc[df_1.StockCode.isin(list_misleading_products)].copy()
df_misleading_products_country = df_misleading_products.groupby(['StockCode', 'Description','Country']).agg({'Quantity':sum, 'Sales': sum}).reset_index()
df_misleading_products_country_negativeSales = df_misleading_products_country.loc[df_misleading_products_country.Sales < 0]
# %%
##get a list of non-products values (discounts, postage etc)
list_non_products = []
for code in list_misleading_products:
    if re.search('\D*', code).group():
        list_non_products.append(re.search('\D*', code).group())
print(list_non_products)
        
##===================================================================================
 # %%
##get a df without non_products items
df_2 = df_1.loc[~df_1.StockCode.isin(list_non_products)].copy()
df_2['InvoiceDate'] = pd.to_datetime(df_2['InvoiceDate']).dt.date
df_2.to_csv('data_ready.csv', sep = '\t', index=False)
print('dataframe is created')

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# %%
def created_server():
    quoted = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=DESKTOP-R3MHCGP\SQLEXPRESS;DATABASE=Pandas_test")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    print('Created engine ', engine)
    return engine
engine = created_server()
df_2.to_sql('dat_new', schema='dbo', con = engine,index=False, if_exists='replace')
print('Done ')
#=======================================================================
# %%
result = engine.execute('SELECT count(*) FROM [dbo].[dat_new]')
result1 = result.fetchall()
print(result1)

#####################################################################################333
# %%

## NaN customer by Country
df_null_custom_country = df_2.loc[df_fromFile.CustomerID.isnull(),['Country', 'CustomerID']]
print(df_null_custom_country.groupby('Country').size())
#================================================================
# %%
##checking countryies
country = df_2.Country.unique()
noCountry_totalSales = df_2.groupby('Country').get_group('Unspecified')['Sales'].sum()
noCountry_totalRows = df_2.groupby('Country').get_group('Unspecified')

print('Total sales for Unspecified country: ', noCountry_totalSales)
print('Total rows for Unspecified country: ',len(noCountry_totalRows))

#========================================================================================================
# %%
df_product_price = df_2[['StockCode','Description']].drop_duplicates()
print(len(df_product_price))
df_product_pricegr = df_product_price.groupby('StockCode').size()
print(len(df_product_pricegr > 1))
df_product_price1 = df_product_pricegr[df_product_pricegr>1].to_frame().merge(df_product_price, on ='StockCode', how = 'left')
print(df_product_price1)

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++=
# %%
## create a table sales by country
country_group = df_2.groupby(by='Country')['Sales'].sum().reset_index()
print(country_group.head())

## create a table sales by country, month-year
mounth_country_group = df_2.loc[~df_2.Country.str.contains('Unspecified'),:].groupby(by=['Country','monthYear'])['Sales'].sum().reset_index()
print(mounth_country_group.head())

## create a table sales by country, quater-year
quater_country_group = df_2.loc[~df_2.Country.str.contains('Unspecified'),:].groupby(by=['Country','YearQuater'])['Sales'].sum().reset_index()
print(quater_country_group.head())

print('The tables are created')

# %%

##create a table cumulative sales by month by country 
df_cumsum = df_2.groupby(by=['Country','monthYear'])['Sales'].sum().groupby(['Country']).cumsum().reset_index()
print(df_cumsum.shape)

# %%
##create a table sales by month-year
mounth_sales_group = df_2.groupby('monthYear')['Sales'].sum().reset_index()
print(mounth_sales_group.head())

##create a table sales by quater-year
quater_sales_group = df_2.groupby('YearQuater')['Sales'].sum().reset_index()
print(quater_sales_group.head())
# %%
## create a table sales by product by country by month
country_product_month_group = df_2.groupby(by=['Country', 'StockCode', 'monthYear'])['Sales'].sum().reset_index()
print(country_product_month_group.head())

## create a table sales by product by country by quater
country_product_quater_group = df_2.groupby(by=['Country', 'StockCode', 'YearQuater'])['Sales'].sum().reset_index()
print(country_product_quater_group.head())

## create a table sales by product by month
product_month_group = df_2.groupby(by=['StockCode', 'monthYear'])['Sales'].sum().reset_index()
print(product_month_group.head())

## create a table sales by product by month
product_quater_group = df_2.groupby(by=['StockCode', 'YearQuater'])['Sales'].sum().reset_index()
print(product_month_group.head())



# %%
