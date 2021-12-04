from flask import render_template, request, url_for, redirect
from pymongo import mongo_client
from app import app, db
from app import queries
from werkzeug.utils import secure_filename
import sys
import os
import csv
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
    finalDF = queries.standardDatapull(10)
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

@app.route('/customerEngagement', methods = ['GET', 'POST'])
def customerEngagement():
    
    finalDF = queries.getSpendData()
    #finalDF = finalDF
    #print(finalDF,file=sys.stderr)
    return render_template("customerEngagement.html", finalDF = finalDF)

@app.route('/demographicFactorsCustomerEngagement', methods = ['GET', 'POST'])
def demographicFactorsCustomerEngagement():
    #do stuff
    return render_template("demographicFactors.html")

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    return render_template("upload.html")

uploads_dir = os.path.join(app.instance_path, 'uploads')
os.makedirs(uploads_dir, exist_ok=True)

@app.route('/create', methods=['GET', 'POST'])
def create():
    if 'myfile1' in request.files:
        households = request.files['myfile1']
        households.save(os.path.join(uploads_dir, secure_filename(households.filename)))
        householdsFileName = secure_filename(households.filename)
    if 'myfile2' in request.files:
        transactions = request.files['myfile2']
        transactions.save(os.path.join(uploads_dir, secure_filename(transactions.filename)))
        transactionsFileName = secure_filename(transactions.filename)
    if 'myfile3' in request.files:
        products = request.files['myfile3']
        products.save(os.path.join(uploads_dir, secure_filename(products.filename)))
        productsFileName = secure_filename(products.filename)
    desiredHousehold = request.form['desiredHousehold']
    path = app.instance_path
    if('myfile3' in request.files and 'myfile2' in request.files and 'myfile1' in request.files and desiredHousehold != "" and request.method == 'POST'):
        return redirect(url_for('datapullCustomFiles',  desiredHousehold = desiredHousehold, path = path, householdsFileName = householdsFileName, transactionsFileName = transactionsFileName, productsFileName = productsFileName))
    return 'Please Try Again with Proper CSV Files'

@app.route('/datapullCustomFiles', methods=['GET', 'POST'])
def datapullCustomFiles():
    desiredHousehold = request.args.get('desiredHousehold')
    path = request.args.get('path')
    householdsFileName = request.args.get('householdsFileName')
    transactionsFileName = request.args.get('transactionsFileName')
    productsFileName = request.args.get('productsFileName')
    finalDF = queries.standardDatapullFiles( int(desiredHousehold), path, householdsFileName, transactionsFileName, productsFileName)
    return render_template('datapullCustomFiles.html',desiredHousehold = desiredHousehold, finalDF = finalDF)

@app.route('/one')
def one():
    return "Q1 will be here"  # return a string

@app.route('/two')
def two():
    return "Q2 will be here"  # return a string
# start the server with the 'run()' method
if __name__ == '__main__':
    app.run(debug=True)
