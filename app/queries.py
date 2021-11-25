from app import db
from pandas import DataFrame
import pandas as pd

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
    houseHoldDF = houseHoldDF[["HSHD_NUM"]]
    transactionDF = transactionDF[["BASKET_NUM", "PURCHASE_","PRODUCT_NUM"]]
    finalDF = pd.concat([houseHoldDF, transactionDF], axis = 1)
    finalDF["HSHD_NUM"] = finalDF["HSHD_NUM"][0]

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
    return finalDF