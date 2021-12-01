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

## hacky pd dataframe code to mimick the above code. I can make some impovements to it when I add the rest of the colums and sort them proper
def standardDatapullFiles(household, path, householdsFileName, transactionFileName, productFileName):
    print(household)
    #header= [ "_id", "HSHD_NUM", "L", "AGE_RANGE", "MARITAL", "INCOME_RANGE", "HOMEOWNER", "HSHD_COMPOSITION", "HH_SIZE", "CHILDREN"]
    uploads_dir = os.path.join(path, 'uploads\\')
##################################
    houseHoldDF = pd.read_csv(uploads_dir + householdsFileName, encoding = 'ISO-8859-1')
    houseHoldColNames =[]
    for col in houseHoldDF.columns:
        houseHoldColNames.append(col)
    houseHoldDF = houseHoldDF.loc[houseHoldDF[houseHoldColNames[0]] == household]
    print(houseHoldDF,file=sys.stderr)
    ##JUST HOUSEHOLD
#########################################
    
    transactionDF = pd.read_csv(uploads_dir + transactionFileName, encoding = 'ISO-8859-1')
    transactionColNames =[]
    for col in transactionDF.columns:
        transactionColNames.append(col)
    #print(transactionColNames,file=sys.stderr)
    transactionDF = transactionDF.loc[transactionDF[transactionColNames[1]] == household]
    transactionDF = transactionDF[[transactionColNames[0], transactionColNames[2], transactionColNames[3]]]
    transactionDF["HSHD_NUM"] = household
    transactionDF = transactionDF[["HSHD_NUM", transactionColNames[0], transactionColNames[2], transactionColNames[3]]]
    print(transactionDF,file=sys.stderr)
    #HSHLD, ["BASKET_NUM", "PURCHASE_","PRODUCT_NUM"]
##########################################
    productDF = pd.read_csv(uploads_dir + productFileName, encoding = 'ISO-8859-1')
    print(productDF,file=sys.stderr)
    productColNames =[]
    for col in productDF.columns:
        productColNames.append(col)
    productNumberDF = transactionDF[[transactionColNames[3]]]
    print(productNumberDF,file=sys.stderr)
    filteredProductDF = pd.DataFrame(columns = productColNames)
    print(filteredProductDF,file=sys.stderr)
    for index,row in productNumberDF.iterrows():
        entry = productDF.loc[productDF[productColNames[0]] == row[productColNames[0]]]
        filteredProductDF = filteredProductDF.append(entry, ignore_index=True)
    filteredProductDF = filteredProductDF[[productColNames[1],productColNames[2]]]
    ##DEPARTMENT, COMMODITY
    print(filteredProductDF,file=sys.stderr)
    #transactionDF = transactionDF.rename(columns={'HSHD_NUM': 'HSHD_NUM', transactionColNames[0]: 'BASKET_NUM', transactionColNames[2]: 'PURCHASE', transactionColNames[3]: 'PRODUCT_NUM'})
    transactionDF.index = pd.RangeIndex(len(filteredProductDF.index))
    transactionDF.index = range(len(filteredProductDF.index))

    print(transactionDF,file=sys.stderr)
    finalDF = pd.concat([transactionDF,filteredProductDF], axis = 1)
    print(finalDF,file=sys.stderr)
    finalDFColNames = []
    for col in finalDF.columns:
        finalDFColNames.append(col)
    return finalDF,finalDFColNames




    #Collection1 = db.householdsTemp
    #Collection2 = db.transactionsTemp
    #Collection3 = db.productsTemp
    #x = Collection1.delete_many({})
    #x = Collection2.delete_many({})
    #x = Collection3.delete_many({})
    #print(houseHoldDF,file=sys.stderr)
    #Collection1.insert_one({"_id":1})
    
    #make_json(uploads_dir + householdsFileName, uploads_dir + householdsFileName[:-3] + '.json')

    #csvfile = open(uploads_dir + householdsFileName, 'r')
    #df = pd.read_csv(uploads_dir + householdsFileName, encoding = 'ISO-8859-1')
    #data = df.to_json()
    #print(data,file=sys.stderr)  
    #header= [ "_id", "HSHD_NUM", "L", "AGE_RANGE", "MARITAL", "INCOME_RANGE", "HOMEOWNER", "HSHD_COMPOSITION", "HH_SIZE", "CHILDREN"]
    
        

    #householdsDF = pd.read_csv(StringIO(householdsDF), sep='\s+', error_bad_lines=False)
    #print(householdsDF,file=sys.stderr)
    #index = householdsDF["HSHD_NUM"] == household
    #householdsDF = householdsDF["HSHD_NUM"]
    #print(householdsDF,file=sys.stderr)

    
    return 