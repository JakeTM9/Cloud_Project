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

column_names = ["PURCHASE_", "SPEND", "HSHD_NUM"]
totalHouseHoldSpendingPerDay = pd.DataFrame(columns = column_names)

##TODO: get average household spending per month given household

#gets total spending from all household per day
for index, row in allHouseHoldNumbersDF.iterrows():
    houseHoldNumber = allHouseHoldNumbersDF.iloc[index]["HSHD_NUM"]
    transactionsForHouseHoldDF = transactionsDF.loc[transactionsDF["HSHD_NUM"] == houseHoldNumber]
    transactionsForHouseHoldDF["PURCHASE_"] = pd.to_datetime(transactionsForHouseHoldDF.PURCHASE_, infer_datetime_format=True)
    transactionsForHouseHoldDF = transactionsForHouseHoldDF.sort_values(by="PURCHASE_")
    transactionsForHouseHoldDF.index = pd.RangeIndex(len(transactionsForHouseHoldDF.index))
    transactionsForHouseHoldDF.index = range(len(transactionsForHouseHoldDF.index))
    transactionsForHouseHoldDF = transactionsForHouseHoldDF.groupby(transactionsForHouseHoldDF['PURCHASE_'], as_index=False).aggregate({'SPEND':'sum', 'HSHD_NUM':'first'})
    totalHouseHoldSpendingPerDay = pd.concat([totalHouseHoldSpendingPerDay, transactionsForHouseHoldDF], axis = 0, ignore_index=True)
    totalHouseHoldSpendingPerDay = totalHouseHoldSpendingPerDay.sort_values(by="PURCHASE_")
    totalHouseHoldSpendingPerDay.index = pd.RangeIndex(len(totalHouseHoldSpendingPerDay.index))
    totalHouseHoldSpendingPerDay.index = range(len(totalHouseHoldSpendingPerDay.index))
    totalHouseHoldSpendingPerDay = totalHouseHoldSpendingPerDay.groupby(totalHouseHoldSpendingPerDay['PURCHASE_'], as_index=False).aggregate({'SPEND':'sum', 'HSHD_NUM':'first'})
print(totalHouseHoldSpendingPerDay)
#divides every total by 400 (number of households) (days where no households spent any money will be omitted, though I dont think there are any)
averageHouseHoldSpendingPerDay = totalHouseHoldSpendingPerDay
for index, row in averageHouseHoldSpendingPerDay.iterrows():
    averageHouseHoldSpendingPerDay.loc[index,'SPEND'] = averageHouseHoldSpendingPerDay.iloc[index]['SPEND'] / len(houseHoldsDF)
#print(len(houseHoldsDF))
print(averageHouseHoldSpendingPerDay)

averageHouseHoldSpendingPerDay.to_excel("averageHouseHoldSpendingPerDay.xlsx", index = False, header=True)

#sns.lmplot(x='PURCHASE_',y='SPEND',data=averageHouseHoldSpendingPerDay,fit_reg=True)




totalHouseHoldSpendingPerDay = houseHoldsDF
averageHouseHoldSpendingPerDay = houseHoldsDF
houseHoldSpendingPerDay = houseHoldsDF