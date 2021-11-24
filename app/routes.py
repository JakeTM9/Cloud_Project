from flask import render_template
from app import app, db
import sys
# use decorators to link the function to a url
@app.route('/')
def home():
    return "Hello, World!"  # return a string

@app.route('/welcome')
def welcome():
    return render_template('welcome.html')  # render a template
@app.route('/login', methods=["GET", "POST"])
def login():

    return render_template('login.html')
@app.route('/datapull')
def datapull():
    Collection1 = db.households
    Collection2 = db.transactions
    Collection3 = db.products
    cursor = Collection1.find({"HSHD_NUM":10})
    cursor2 = Collection2.find({"HSHD_NUM":10})
    cursor3 = Collection3.find({"PRODUCT_NUM":cursor2[0]["PRODUCT_NUM"]})
    #print(cursor[0]['_id'], file=sys.stderr)
    return render_template('datapull.html', c1 = cursor[0])

@app.route('/one')
def one():
    return "Q1 will be here"  # return a string

@app.route('/two')
def two():
    return "Q2 will be here"  # return a string
# start the server with the 'run()' method
if __name__ == '__main__':
    app.run(debug=True)
