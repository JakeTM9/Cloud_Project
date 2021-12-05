from werkzeug.utils import secure_filename
import sys
import os
import csv
from pandas import DataFrame
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
#print(os.path.abspath(os.getcwd()))
pd.options.mode.chained_assignment = None  # default='warn'
houseHoldsDF = pd.read_csv("../../instance/uploads/400_households.csv", encoding = 'ISO-8859-1') 
houseHoldsDF = houseHoldsDF[[col for col in houseHoldsDF.columns if col != '_id']]
houseHoldsDF = houseHoldsDF.rename(columns={houseHoldsDF.columns[0]:"HSHD_NUM",houseHoldsDF.columns[1]:"L",houseHoldsDF.columns[2]:"AGE_RANGE",houseHoldsDF.columns[3]:"MARITAL",houseHoldsDF.columns[4]:"INCOME_RANGE",houseHoldsDF.columns[5]:"HOMEOWNER",houseHoldsDF.columns[6]:"HSHD_COMPOSITION",houseHoldsDF.columns[7]:"HH_SIZE",houseHoldsDF.columns[8]:"CHILDREN"})
#print(houseHoldsDF)
transactionsDF = pd.read_csv("../../instance/uploads/400_transactions.csv", encoding = 'ISO-8859-1')
transactionsDF = transactionsDF[[col for col in transactionsDF.columns if col != '_id']]
transactionsDF = transactionsDF.rename(columns={transactionsDF.columns[0]:"BASKET_NUM",transactionsDF.columns[1]:"HSHD_NUM",transactionsDF.columns[2]:"PURCHASE_",transactionsDF.columns[3]:"PRODUCT_NUM",transactionsDF.columns[4]:"SPEND",transactionsDF.columns[5]:"UNITS",transactionsDF.columns[6]:"STORE_R",transactionsDF.columns[7]:"WEEK_NUM",transactionsDF.columns[8]:"YEAR"})
#print(transactionsDF)
productsDF = pd.read_csv("../../instance/uploads/400_products.csv", encoding = 'ISO-8859-1')
productsDF = productsDF.rename(columns={productsDF.columns[0]:"PRODUCT_NUM",productsDF.columns[1]:"DEPARTMENT",productsDF.columns[2]:"COMMODITY",productsDF.columns[3]:"BRAND_TY",productsDF.columns[4]:"NATURAL_ORGANIC_FLAG"})
#print(productsDF)
allHouseHoldNumbersDF = houseHoldsDF[["HSHD_NUM"]]
#print(allHouseHoldNumbersDF)

##get has children dataframe and get no children dataframe
column_names = ["HSHD_NUM", "L", "AGE_RANGE", "MARITAL", "INCOME_RANGE", "HOMEOWNER", "HSHD_COMPOSITION", "HH_SIZE", "CHILDREN"]
houseHoldsWithChildren = pd.DataFrame(columns = column_names)
houseHoldsWithoutChildren = pd.DataFrame(columns = column_names)
for index, row in allHouseHoldNumbersDF.iterrows():
    if houseHoldsDF.iloc[index]["CHILDREN"] == "1" or houseHoldsDF.iloc[index]["CHILDREN"] == "2" or houseHoldsDF.iloc[index]["CHILDREN"] == "3+":
        
        houseHoldsWithChildren = pd.concat([houseHoldsWithChildren, houseHoldsDF.loc[[index]]], axis = 0, ignore_index=True)
        #totalHouseHoldSpendingPerDay = pd.concat([totalHouseHoldSpendingPerDay, transactionsForHouseHoldDF], axis = 0, ignore_index=True)
    else:
        houseHoldsWithoutChildren = pd.concat([houseHoldsWithoutChildren, houseHoldsDF.loc[[index]]], axis = 0, ignore_index=True)
print(houseHoldsWithChildren)
print(houseHoldsWithoutChildren)

##Get all categories
categories = []
categories_children = []
categories_no_children = []
for index, row in productsDF.iterrows():
    try:
        categories.index(productsDF.iloc[index]["COMMODITY"].strip())
    except:
        categories.append(productsDF.iloc[index]["COMMODITY"].strip())
print(categories)

categoryValuesChildren = {i : 0 for i in categories}
categoryValuesNoChildren = {i : 0 for i in categories}

for index, row in houseHoldsWithChildren.iterrows():
    houseHoldNumber = houseHoldsWithChildren.iloc[index]["HSHD_NUM"]
    transactionsForHouseHoldDF = transactionsDF.loc[transactionsDF["HSHD_NUM"] == houseHoldNumber]
    for index, row in transactionsForHouseHoldDF.iterrows():
        product = productsDF.loc[productsDF["PRODUCT_NUM"] == int(row["PRODUCT_NUM"])]
        categoryValuesChildren[product.iloc[0]["COMMODITY"].strip()] += row['SPEND']


for key, value in categoryValuesChildren.items():
    categoryValuesChildren[key] = value / len(houseHoldsWithChildren)
    
averageSpentByCategoryChildrenDF = pd.DataFrame.from_dict(categoryValuesChildren, orient='index')
print(averageSpentByCategoryChildrenDF)

###############################

houseHoldNumber = houseHoldsWithoutChildren.iloc[0]["HSHD_NUM"]
transactionsForHouseHoldDF = transactionsDF.loc[transactionsDF["HSHD_NUM"] == houseHoldNumber]
for index, row in houseHoldsWithoutChildren.iterrows():
    houseHoldNumber = houseHoldsWithoutChildren.iloc[index]["HSHD_NUM"]
    transactionsForHouseHoldDF = transactionsDF.loc[transactionsDF["HSHD_NUM"] == houseHoldNumber]
    for index, row in transactionsForHouseHoldDF.iterrows():
        product = productsDF.loc[productsDF["PRODUCT_NUM"] == int(row["PRODUCT_NUM"])]
        categoryValuesNoChildren[product.iloc[0]["COMMODITY"].strip()] += row['SPEND']

for key, value in categoryValuesNoChildren.items():
    categoryValuesNoChildren[key] = value / len(houseHoldsWithoutChildren)
    
averageSpentByCategoryNoChildrenDF = pd.DataFrame.from_dict(categoryValuesNoChildren, orient='index')
print(averageSpentByCategoryNoChildrenDF)

comparativeSpentByCategory = pd.concat([averageSpentByCategoryChildrenDF, averageSpentByCategoryNoChildrenDF], axis = 1)

print(comparativeSpentByCategory)
comparativeSpentByCategory.to_excel("childrenVsNoChildrenAverageSpendingByCategory.xlsx", index = True, header=True)

#for index, row in houseHoldsWithChildren.iterrows():
    #print("hi")
    #houseHoldNumber = houseHoldsWithChildren.iloc[index]["HSHD_NUM"]
    #transactionsForHouseHoldDF = transactionsDF.loc[transactionsDF["HSHD_NUM"] == houseHoldNumber]
    #product = "kek"
    #for index, row in transactionsForHouseHoldDF.iterrows():
        #allProductDF.loc[allProductDF["PRODUCT_NUM"] == int(row["PRODUCT_NUM"])]
        #product = productsDF.loc[productsDF["PRODUCT_NUM"] == int(row["PRODUCT_NUM"])]
        #categoriesNoChildrenDF[[transactionsForHouseHoldDF.iloc[index]["COMMODITY"]]]
        #print(product.iloc[0]["COMMODITY"].strip())
        #categoriesChildrenDF[product.iloc[0]["COMMODITY"].strip()]
        








