import csv
import pandas
import pygsheets

gsheet = pygsheets.authorize(service_file='./usau-club-nationals-volunteers-61aca3eb7368.json')
sheet = gsheet.open('USAU Nationals 2023 Volunteers')

try:
    worksheet = sheet.worksheet("title", "all_jobs")
except Exception:
    pass

worksheet = worksheet.get_as_df()
with open("all_jobs.csv", "w") as outfile:
    worksheet.to_csv(path_or_buf=outfile,index=False)

