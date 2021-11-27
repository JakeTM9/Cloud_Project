from flask import render_template, request, url_for, redirect
from pymongo import mongo_client
from app import app, db
from app import queries
from werkzeug.utils import secure_filename
import sys
import os
from pandas import DataFrame
import pandas as pd
# use decorators to link the function to a url
@app.route('/', methods=["GET", "POST"])
def login():
    if request.method == 'POST':
        email = request.form["email"]
        username = request.form["username"]
        password = request.form["password"]
        return redirect(url_for('home'), email = email, username = username, password = password)
    return render_template('login.html')

@app.route('/home', methods=['GET', 'POST'])
def home():
    email = request.form.get('email')
    username = request.form.get('username')
    password = request.form.get('password')
    return render_template('home.html', email = email, username = username, password = password)

@app.route('/datapullHousehold10')
def datapullHousehold10():
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
    return render_template('datapullHousehold10.html', household_df = finalDF)

@app.route('/datapullCustomInput', methods=['GET', 'POST'])
def datapullCustomInput():
    if request.method == 'POST':
        desiredHousehold = request.form["desiredHousehold"]
        return redirect(url_for('datapullCustom'), desiredHousehold = desiredHousehold)
    return render_template('datapullCustomInput.html')

@app.route('/datapullCustom', methods=['GET', 'POST'])
def datapullCustom():
    desiredHousehold = request.form.get('desiredHousehold')
    finalDF = queries.standardDatapull(int(desiredHousehold))
    return render_template('datapullCustom.html', household_df = finalDF, desiredHousehold = desiredHousehold)



@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    return render_template('upload.html')

@app.route('/create', methods=["POST"])
def create():
    mydb = mongo_client["newatabase"]
    myCollection = mydb["newData"]
    for f in request.files.getlist('myfile'):
        if 'myfile1' in request.files:
            myFile = request.files['myfile1']
            mongo_client.save_file(myFile.filename, myFile)
        if 'myfile2' in request.files:
            myFile = request.files['myfile2']
            mongo_client.save_file(myFile.filename, myFile)
        if 'myfile3' in request.files:
            myFile = request.files['myfile3']
            mongo_client.save_file(myFile.filename, myFile)
            #mongo.db.myFiles.insert({''})

        
    return 'Upload completed.'


@app.route('/one')
def one():
    return "Q1 will be here"  # return a string

@app.route('/two')
def two():
    return "Q2 will be here"  # return a string
# start the server with the 'run()' method
if __name__ == '__main__':
    app.run(debug=True)
