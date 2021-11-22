from __init__ import *
Collection1 = db.households
#https://www.geeksforgeeks.org/python-mongodb-query/
cursor = Collection1.find({"HSHD_NUM":10})
print("The Record of Household 10 is: ")
print(cursor)
# for record in cursor: 
#     print(record) 
      