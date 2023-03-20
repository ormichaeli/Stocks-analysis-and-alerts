from flask import Flask, render_template, request
from pymongo import MongoClient

app = Flask(__name__)

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client["stocks_db"]
col = db["users"]


@app.route("/", methods=["GET", "POST"])
def user_registration():
    data = request.form.to_dict()
    if len(data) > 0:
        # Add an indicator for active alert
        data.update({'is_active': 1})
        # Save registration form in MonogoDB
        col.insert_one(data)
    return render_template("registration.html")


if __name__ == "__main__":
    app.run(debug=True, host='localhost', port=5000)