# Upload and add new transactions
import functions.transactions

t = functions.transactions.Transactions()

t.Upload("~/Downloads/EXPORT_CALL_REPORT - NEW.csv", True)
t.Add('BP')