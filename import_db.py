# Add new DB
import functions.records as r

print("CSV File: ", end="")
csv_file = input()

print("DB Type: ", end="")
db_type = input()

print("Source: ", end="")
source = input()

new_db = r.Records()
new_db.Add(csv_file, db_type, source)