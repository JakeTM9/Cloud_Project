from app import db
from pandas import DataFrame
import pandas as pd
import sys
import csv
import json
import pandas as pd
import sys, getopt, pprint
import os

def standardDatapull(household):
    Collection1 = db.households
    Collection2 = db.transactions
    Collection3 = db.products

    #GET CURSORS
    houseHoldCursor = Collection1.find({"HSHD_NUM":household})
    transactionCursor = Collection2.find({"HSHD_NUM":household})
    productCursor = Collection3.find()
    

    ##TURN CURSOR INTO LIST, THEN INTO DATAFRAME
    list_houseHoldCursor = list(houseHoldCursor)
    houseHoldDF = DataFrame(list_houseHoldCursor)
    list_transactionCursor = list(transactionCursor)
    transactionDF = DataFrame(list_transactionCursor)
    list_allProducts = list(productCursor) ##IMPORT ALL PRODUCTS BECAUSE QUERYING THE DB FOR EACH PRODUCT NUMBER WITH SINGLE RETURNS TAKES 2 MINUTES
    AllProductsDF = DataFrame(list_allProducts)

    ##FORMAT HOUSEHOLD AND TRANSACTION DFS
    #houseHoldDF = houseHoldDF[["HSHD_NUM"]]
    #transactionDF = transactionDF[["BASKET_NUM", "PURCHASE_","PRODUCT_NUM"]]
    transactionDF = transactionDF.drop('HSHD_NUM', axis=1)
    finalDF = pd.concat([transactionDF, houseHoldDF ], axis = 1)
    finalDF["HSHD_NUM"] = finalDF["HSHD_NUM"][0]
    finalDF["L"] = finalDF["L"][0]
    finalDF["AGE_RANGE"] = finalDF["AGE_RANGE"][0]
    finalDF["MARITAL"] = finalDF["MARITAL"][0]
    finalDF["INCOME_RANGE"] = finalDF["INCOME_RANGE"][0]
    finalDF["HOMEOWNER"] = finalDF["HOMEOWNER"][0]
    finalDF["HSHD_COMPOSITION"] = finalDF["HSHD_COMPOSITION"][0]
    finalDF["HH_SIZE"] = finalDF["HH_SIZE"][0]
    finalDF["CHILDREN"] = finalDF["CHILDREN"][0]
    finalDF = finalDF[['HSHD_NUM'] + [col for col in finalDF.columns if col != 'HSHD_NUM' and col != '_id']]

    #finalDF[["L","AGE_RANGE","MARITAL","INCOME_RANGE","HOMEOWNER","HSHD_COMPOSITION","HH_SIZE","CHILDREN"]] = finalDF[["L","AGE_RANGE","MARITAL","INCOME_RANGE","HOMEOWNER","HSHD_COMPOSITION","HH_SIZE","CHILDREN"]][0].astype(str)
    #print(finalDF,file=sys.stderr)

    ##QUERYING PRODUCTS BY EACH PRODUCT NUMBER FROM ABOVE DF
    productNumberDF = transactionDF[["PRODUCT_NUM"]]
    productsDF = DataFrame(columns=["_id","PRODUCT_NUM", "DEPARTMENT", "COMMODITY", "BRAND_TY", "NATURAL_ORGANIC_FLAG"])
    list_productNumbers = []
    for index, row in productNumberDF.iterrows():
        entry = AllProductsDF.loc[AllProductsDF["PRODUCT_NUM"] == int(row["PRODUCT_NUM"])]
        productsDF = productsDF.append([entry])
    productsDF = productsDF[["DEPARTMENT", "COMMODITY"]]

    ##MUST REINDEX BECAUSE DUPLICATE INDEXES IN PRODUCTSDF
    productsDF.index = pd.RangeIndex(len(productsDF.index))
    productsDF.index = range(len(productsDF.index))

    ##FINALLY COMBINE ALL
    finalDF = pd.concat([finalDF, productsDF], axis = 1)
    finalDF["PURCHASE_"] = pd.to_datetime(finalDF.PURCHASE_, infer_datetime_format=True)
    finalDF["PURCHASE_"]= finalDF["PURCHASE_"]
    finalDF = finalDF.sort_values(by=["BASKET_NUM","PURCHASE_","PRODUCT_NUM","DEPARTMENT","COMMODITY"])
    return finalDF

def getSpendData(): #never gonna be used
    Collection1 = db.households
    Collection2 = db.transactions
    Collection3 = db.products

    #GET CURSORS
    houseHoldCursor = Collection1.find({"HSHD_NUM":10})
    transactionCursor = Collection2.find({"HSHD_NUM":10})
    productCursor = Collection3.find()
    

    ##TURN CURSOR INTO LIST, THEN INTO DATAFRAME
    list_houseHoldCursor = list(houseHoldCursor)
    houseHoldDF = DataFrame(list_houseHoldCursor)
    list_transactionCursor = list(transactionCursor)
    transactionDF = DataFrame(list_transactionCursor)
    list_allProducts = list(productCursor) ##IMPORT ALL PRODUCTS BECAUSE QUERYING THE DB FOR EACH PRODUCT NUMBER WITH SINGLE RETURNS TAKES 2 MINUTES
    AllProductsDF = DataFrame(list_allProducts)

    ##FORMAT HOUSEHOLD AND TRANSACTION DFS
    houseHoldDF = houseHoldDF[["HSHD_NUM"]]
    transactionDF = transactionDF[["BASKET_NUM", "PURCHASE_","SPEND"]]
    finalDF = pd.concat([houseHoldDF, transactionDF], axis = 1)
    finalDF["HSHD_NUM"] = finalDF["HSHD_NUM"][0]
    #print(finalDF,file=sys.stderr)
    finalDF["PURCHASE_"] = pd.to_datetime(finalDF.PURCHASE_, infer_datetime_format=True)
    finalDF = finalDF.sort_values(by="PURCHASE_")
    finalDF = finalDF[["PURCHASE_","SPEND"]]
    finalDF.index = pd.RangeIndex(len(finalDF.index))
    finalDF.index = range(len(finalDF.index))
    finalDF['id'] = finalDF.index
    finalDF = finalDF.groupby(finalDF['PURCHASE_']).aggregate({'SPEND':'sum'})#.aggregate(aggregation_functions)
    finalDF['PURCHASE_'] = finalDF.index
    finalDF['PURCHASE_'] = finalDF['PURCHASE_'].astype(str)
    spendValues = finalDF['SPEND'].values.tolist()
    dateValues = finalDF['PURCHASE_'].values.tolist()
    #print(spendValues,file=sys.stderr)
    #print(dateValues,file=sys.stderr)
    return finalDF

## hacky pd dataframe code to mimick the above code. I can make some impovements to it when I add the rest of the colums and sort them proper
def standardDatapullFiles(household, path, householdsFileName, transactionFileName, productFileName):
    #header= [ "_id", "HSHD_NUM", "L", "AGE_RANGE", "MARITAL", "INCOME_RANGE", "HOMEOWNER", "HSHD_COMPOSITION", "HH_SIZE", "CHILDREN"]
    
    uploads_dir = os.path.join(path, 'uploads')

    houseHoldDF = pd.read_csv(os.path.join(uploads_dir,householdsFileName), encoding = 'ISO-8859-1')
    houseHoldDF = houseHoldDF[[col for col in houseHoldDF.columns if col != '_id']]
    houseHoldDF = houseHoldDF.loc[houseHoldDF[houseHoldDF.columns[0]] == household]
    houseHoldDF = houseHoldDF.rename(columns={houseHoldDF.columns[0]:"HSHD_NUM",houseHoldDF.columns[1]:"L",houseHoldDF.columns[2]:"AGE_RANGE",houseHoldDF.columns[3]:"MARITAL",houseHoldDF.columns[4]:"INCOME_RANGE",houseHoldDF.columns[5]:"HOMEOWNER",houseHoldDF.columns[6]:"HSHD_COMPOSITION",houseHoldDF.columns[7]:"HH_SIZE",houseHoldDF.columns[8]:"CHILDREN"})
    #print(houseHoldDF,file=sys.stderr)
    transactionDF = pd.read_csv(os.path.join(uploads_dir,transactionFileName), encoding = 'ISO-8859-1')
    transactionDF = transactionDF[[col for col in transactionDF.columns if col != '_id']]
    transactionDF = transactionDF.rename(columns={transactionDF.columns[0]:"BASKET_NUM",transactionDF.columns[1]:"HSHD_NUM",transactionDF.columns[2]:"PURCHASE_",transactionDF.columns[3]:"PRODUCT_NUM",transactionDF.columns[4]:"SPEND",transactionDF.columns[5]:"UNITS",transactionDF.columns[6]:"STORE_R",transactionDF.columns[7]:"WEEK_NUM",transactionDF.columns[8]:"YEAR"})
    transactionDF = transactionDF.loc[transactionDF[transactionDF.columns[1]] == household]
    houseHoldDF = houseHoldDF[[col for col in houseHoldDF.columns if col != 'HSHD_NUM']]
    finalDF = pd.concat([transactionDF, houseHoldDF ], axis = 1)
    #print(finalDF,file=sys.stderr)
    #print(finalDF.iloc[0]["L"])
    finalDF["L"] = finalDF.iloc[0]["L"]
    finalDF["AGE_RANGE"] = finalDF.iloc[0]["AGE_RANGE"]
    finalDF["MARITAL"] = finalDF.iloc[0]["MARITAL"]
    finalDF["INCOME_RANGE"] = finalDF.iloc[0]["INCOME_RANGE"]
    finalDF["HOMEOWNER"] = finalDF.iloc[0]["HOMEOWNER"]
    finalDF["HSHD_COMPOSITION"] = finalDF.iloc[0]["HSHD_COMPOSITION"]
    finalDF["HH_SIZE"] = finalDF.iloc[0]["HH_SIZE"]
    finalDF["CHILDREN"] = finalDF.iloc[0]["CHILDREN"]
    finalDF = finalDF.iloc[1: , :]
    finalDF = finalDF[['HSHD_NUM'] + [col for col in finalDF.columns if col != 'HSHD_NUM' and col != '_id']]
    #print(finalDF,file=sys.stderr)
    productDF = pd.read_csv(os.path.join(uploads_dir,productFileName), encoding = 'ISO-8859-1')
    productDF = productDF.rename(columns={productDF.columns[0]:"PRODUCT_NUM",productDF.columns[1]:"DEPARTMENT",productDF.columns[2]:"COMMODITY",productDF.columns[3]:"BRAND_TY",productDF.columns[4]:"NATURAL_ORGANIC_FLAG"})
    allProductDF = productDF
    productDF = productDF.iloc[0:0]
    productNumberDF = finalDF[["PRODUCT_NUM"]]
    for index, row in productNumberDF.iterrows():
        entry = allProductDF.loc[allProductDF["PRODUCT_NUM"] == int(row["PRODUCT_NUM"])]
        productDF = productDF.append([entry])
    productDF = productDF[["DEPARTMENT","COMMODITY"]]
    productDF.index = pd.RangeIndex(len(productDF.index))
    productDF.index = range(len(productDF.index))
    finalDF.index = pd.RangeIndex(len(productDF.index))
    finalDF.index = range(len(productDF.index))
    #print(productDF,file=sys.stderr)
    finalDF = pd.concat([productDF, finalDF], axis = 1)
    finalDF = finalDF[['HSHD_NUM','BASKET_NUM','PURCHASE_','PRODUCT_NUM','DEPARTMENT','COMMODITY'] + [col for col in finalDF.columns if col != 'HSHD_NUM' and col != 'BASKET_NUM'and col != 'PURCHASE_' and col != 'PRODUCT_NUM' and col != 'DEPARTMENT' and col != 'COMMODITY']]
    #print(finalDF,file=sys.stderr)
    finalDF["PURCHASE_"] = pd.to_datetime(finalDF.PURCHASE_, infer_datetime_format=True)
    finalDF["PURCHASE_"] = finalDF["PURCHASE_"]
    finalDF = finalDF.sort_values(by=["BASKET_NUM","PURCHASE_","PRODUCT_NUM","DEPARTMENT","COMMODITY"])
    return finalDF