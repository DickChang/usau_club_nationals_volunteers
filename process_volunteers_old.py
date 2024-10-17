import csv
import time
import pandas
import pygsheets

# returns time difference in hours
# only works on "sortable" timestamp formats
def calc_time_diff(t1,t2):
    t1_minutes = t1.tm_hour * 60 + t1.tm_min
    t2_minutes = t2.tm_hour * 60 + t2.tm_min
    if t1_minutes > t2_minutes:
        diff = t1_minutes - t2_minutes
    else:
        diff = t2_minutes - t1_minutes
    return (diff/60)

def convert_time(t, sortable):
    if sortable:
        return time.strptime(t,"%I:%M %p")
    else:
        return time.strftime("%I:%M %p", t)

def compare_jobs(j1, j2):
    if(j1["Shift start date"] == j2["Shift start date"] and
        j1["Job Location"].casefold() == j2["Job Location"].casefold() and
        j1["Shift start time"] == j2["Shift start time"] and
        j1["Job"].casefold() == j2["Job"].casefold()):
        return True
    else:
        return False


#file = "usau-nationals - volunteers.csv" # csv containing all the volunteer signup info
file = "usau-nationals - volunteers_2018.csv" # csv containing all the volunteer signup info

volunteers = pandas.read_csv(file)

volunteers.insert(12, "Shift Duration", "", allow_duplicates=True)
#volunteers.insert(30, "T-Shirt", "", allow_duplicates=True)
for v in volunteers.index:
    # adjust the fricking case abomination
    volunteers.at[v, "Job Location"] = volunteers.at[v, "Job Location"].title()
    volunteers.at[v, "Job"] = volunteers.at[v, "Job"].title()

    # remove NaN from requests
    if volunteers.at[v, "Requests?"] != volunteers.at[v, "Requests?"]:
        volunteers.at[v, "Requests?"] = ""

    # change the timestamp to a sortable format
    volunteers.at[v, "Shift start time"] = convert_time(volunteers.at[v, "Shift start time"], True)
    volunteers.at[v, "Shift end time"] = convert_time(volunteers.at[v, "Shift end time"], True)

    # calculate the shift duration
    volunteers.at[v, "Shift Duration"] = calc_time_diff(volunteers.at[v, "Shift start time"], volunteers.at[v, "Shift end time"])

volunteers = volunteers.rename(columns={"Shirt size": "T-Shirt"})

# count the number of t-shirts needed

tshirt_count = volunteers.groupby('T-Shirt')['Volunteer Identifier'].nunique()
tshirt_count = pandas.DataFrame({"Size": tshirt_count.index, "Count": tshirt_count.array})

# count the number of unique volunteers per day
unique_v = (volunteers.groupby(['Shift start date','Job Location'], as_index=False)['Volunteer Identifier']).nunique()
unique_v = unique_v.rename(columns={"Volunteer Identifier": "Unique Volunteers"})

# count the number of hours worked per volunteer
hours_worked = (volunteers.groupby(['Volunteer Identifier','First Name', 'Last Name', 'Requests?'], as_index=False)['Shift Duration']).sum()
num_worked_shifts = (volunteers.groupby(['Volunteer Identifier'], as_index=False)['Shift Duration']).count()
num_worked_shifts = num_worked_shifts.rename(columns={"Shift Duration": "Num Shifts"})
hours_worked = hours_worked.merge(num_worked_shifts, on="Volunteer Identifier", how="outer")
hours_worked = hours_worked.sort_values(["Shift Duration"], ascending = (False))
hours_worked = hours_worked.rename(columns={"Shift Duration": "Hours Worked"})
hours_worked.insert(len(hours_worked.columns), 'Requests', hours_worked['Requests?'])
hours_worked = hours_worked.drop(columns=['Volunteer Identifier', 'Requests?'])
avg_hours_worked = (hours_worked.loc[:,"Hours Worked"].sum()) / len(hours_worked)
avg_shifts_worked = (hours_worked.loc[:,"Num Shifts"].sum()) / len(hours_worked)

# sort the volunteers in an order that makes sense for us
key_columns = ["Shift start date",
    "Job Location",
    "Shift start time",
    "Job"]
volunteers = volunteers.sort_values(
    key_columns,
    ascending = (True, True, True, True))

# change the timestamp to a readable format
volunteers['Shift start time'] = volunteers.apply(lambda row : convert_time(row['Shift start time'], False), axis = 1)
volunteers['Shift end time'] = volunteers.apply(lambda row : convert_time(row['Shift end time'], False), axis = 1)

volunteers = (volunteers.reset_index()).drop(columns="index")


##########
# upload to google sheets
##########
gsheet = pygsheets.authorize(service_file='./google_api_key.json')
#sheet = gsheet.open('USAU Nationals 2021 Volunteers')
#sheet = gsheet.open('USAU Nationals 2019 Volunteers')
sheet = gsheet.open('USAU Nationals 2018 Volunteers')

try:
    #worksheet = sheet.worksheet("title", "master_list_autogen")
    #sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "stats_autogen")
    sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "volunteers_autogen")
    sheet.del_worksheet(worksheet)
except Exception:
    pass

# add the raw volunteers data in a tab
worksheet = sheet.add_worksheet("volunteers_autogen")
worksheet.set_dataframe(volunteers, pygsheets.Address("A1"))
worksheet.adjust_column_width(start=1, end=40, pixel_size=None)
worksheet.update_dimensions_visibility(start=1, end=2, dimension="COLUMNS", hidden=True) # Event, Event Location
worksheet.update_dimensions_visibility(start=5, end=7, dimension="COLUMNS", hidden=True) # Job Description, Job Notes, Job Rate
worksheet.update_dimensions_visibility(start=10, dimension="COLUMNS", hidden=True) # Shift End Date
worksheet.update_dimensions_visibility(start=12, end=24, dimension="COLUMNS", hidden=True) # lots
worksheet.update_dimensions_visibility(start=27, end=29, dimension="COLUMNS", hidden=True) # lots
worksheet.update_dimensions_visibility(start=35, dimension="COLUMNS", hidden=True)
worksheet.update_dimensions_visibility(start=37, dimension="COLUMNS", hidden=True)
worksheet.frozen_rows = 1

######
# add some stats
worksheet = sheet.add_worksheet("stats_autogen", rows=400)

# number of unique volunteers
worksheet.update_value("A1", "Unique Volunteers")
worksheet.update_value("B1", len(pandas.unique(volunteers['Volunteer Identifier'])))

# daily unique volunteers
worksheet.update_value("A3", "Daily Unique Volunteers")
worksheet.set_dataframe(unique_v, "A4")

# t-shirt counts
worksheet.update_value("E3", "T-Shirts")
worksheet.set_dataframe(tshirt_count, "E4")

# hours worked per volunteer
worksheet.update_value("A20", "Hours worked\nper volunteer")
worksheet.set_dataframe(hours_worked, "A21")
output_row = 14+len(hours_worked)
output_row += 2

worksheet.update_value("B"+str(output_row), "Average")
worksheet.update_value("C"+str(output_row), avg_hours_worked)
worksheet.update_value("D"+str(output_row), avg_shifts_worked)


worksheet.adjust_column_width(start=1, end=4, pixel_size=None)
