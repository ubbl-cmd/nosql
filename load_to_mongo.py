import pymongo
import pprint
import csv

def read_from_csv(fname):
    students = None
    with open(fname, mode='r') as file:
        csv_reader = csv.DictReader(file)
        students = [row for row in csv_reader]
    return students


def load_to_mongo(mycol, students, verbose=True):
    for x in mycol.find():
        pprint.pprint(x)
    
    for student in students:
        x = mycol.find_one({"id": student["id"]})
        if x:
            print(f"student {student['name']} found!, updating")
            mycol.update_one({"id": student["id"]}, {"$set": student})
            continue
        x = mycol.insert_one(student)
    for x in mycol.find():
        pprint.pprint(x)


if __name__ == "__main__":
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")


    mydb = myclient["developer_google_com_exports"]
    dblist = myclient.list_database_names()
    if "developer_google_com_exports" in dblist:
        print("The database exists.")
    
    mycol = mydb["students"]

    load_to_mongo(mycol, read_from_csv("fname.csv"))

