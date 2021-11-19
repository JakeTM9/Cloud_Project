# import the Flask class from the flask module
#hello
from flask import Flask, render_template
from flask_pymongo import PyMongo

# create the application object
app = Flask(__name__)

#mongoDB stuff
app.config["SECRET_KEY"] = "88182cd7668f7d16d76980db74ef48911dc30f41"
app.config["MONGO_URI"] = "mongodb+srv://user:TestOne1@cluster0.4igh6.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
mongodb_client = PyMongo(app)
db = mongodb_client.db
collection = db.households

emp_rec1 = { ##testing manually inputting data, having trouble loading a csv into mongodb
        "name":"Mr.Geek",
        "eid":24,
        "location":"delhi"
        }

#This throws a network timeout error for me. Can you guys give it a shot
#rec_id1 = collection.insert_one(emp_rec1)



from app import routes
