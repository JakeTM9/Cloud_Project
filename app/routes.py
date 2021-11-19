from flask import render_template
from app import app 
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
    return render_template('datapull.html')

@app.route('/one')
def one():
    return "Q1 will be here"  # return a string

@app.route('/two')
def two():
    return "Q2 will be here"  # return a string
# start the server with the 'run()' method
if __name__ == '__main__':
    app.run(debug=True)
