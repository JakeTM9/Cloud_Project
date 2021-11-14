# import the Flask class from the flask module
#hello
from flask import Flask, render_template

# create the application object
app = Flask(__name__)

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
# start the server with the 'run()' method
if __name__ == '__main__':
    app.run(debug=True)
