import csv
import time
from datetime import datetime
import pandas
import pygsheets
from functools import reduce

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
    if(j1["Date"] == j2["Date"] and
        j1["Job Location"].casefold() == j2["Job Location"].casefold() and
        j1["Start Time"] == j2["Start Time"] and
        j1["Task"].casefold() == j2["Task"].casefold()):
        return True
    else:
        return False

def compare_date_job(j1, j2):
    # expecting columns 0 and 1 to be [Date][Location] for both parameters
    j1_date = datetime.strptime(j1[0], "%Y/%m/%d")
    j2_date = datetime.strptime(j2[0], "%Y/%m/%d")
    if(j1_date < j2_date):
        return 0 # return 0(false) meaning that it needs to increment and continue
    elif(j1_date > j2_date):
        return 1 # return 1(true) meaning that it's found the spot to insert
    else:
        if(j1[1] < j2[1]):
            return 0 # meaning that it needs to increment and continue
        elif(j1[1] > j2[1]):
            return 1 # return 1(true meaning that it's found the spot to insert)
        else:
            print("ERROR: Duplicate [Day][Job Location] found.")
            exit()

file = "usau-nationals - volunteers.csv" # csv containing all the volunteer signup info
#file = "usau-nationals - volunteers_2019.csv" # csv containing all the volunteer signup info

volunteers = pandas.read_csv(file)
volunteers = volunteers.dropna(subset = ['Who']) #drop all the jobs that aren't filled. the dropped rows here could be used to create or verify the master_list
volunteers = (volunteers.reset_index()).drop(columns="index")

volunteer_ids = []

#volunteers.insert(12, "Shift Duration", "", allow_duplicates=True)
#volunteers.insert(30, "T-Shirt", "", allow_duplicates=True)
volunteers.insert(0, "Event", "", allow_duplicates=True)
volunteers.insert(6, "Job Location", "", allow_duplicates=True)
volunteers.insert(len(volunteers.columns), "Signup Date", "", allow_duplicates=True)
volunteers.insert(len(volunteers.columns), "Signup Time", "", allow_duplicates=True)
volunteers.insert(len(volunteers.columns), "ID", "", allow_duplicates=True)
foundError = False
for v in volunteers.index:
    # Add "Event Field"
    volunteers.at[v, "Event"] = "USAU Club Nationals"

    # Parse out the location and add field
    if "Polo Fields" in volunteers.at[v, "Task"]:
        volunteers.at[v, "Job Location"] = "Polo Fields"
    elif "Stadium" in volunteers.at[v, "Task"]:
        volunteers.at[v, "Job Location"] = "Stadium"
    elif "Hilton" in volunteers.at[v, "Task"]:
        volunteers.at[v, "Job Location"] = "Hilton"
    else:
        print("ERROR no location for this job: "+volunteers.at[v, "Task"])
        foundError = True
    
    # Parse out Date and Time from combined signup datetime
    signup_time_gmt = (volunteers.at[v,"Signup Time (GMT)"]).split()
    volunteers.at[v, "Signup Date"] = signup_time_gmt[0]
    volunteers.at[v, "Signup Time"] = signup_time_gmt[1]

    # Fill in names if someone didn't fill in their full name
    if(pandas.isna(volunteers.at[v,"First Name"])):
       volunteers.at[v,"First Name"] = " "
    if(pandas.isna(volunteers.at[v,"Last Name"])):
       volunteers.at[v,"Last Name"] = " "
    #if(pandas.isna(volunteers.at[v,"First Name"]) and not pandas.isna(volunteers.at[v,"Last Name"])):
    #   volunteers.at[v,"First Name"] = " "
    #elif(not pandas.isna(volunteers.at[v,"First Name"]) and pandas.isna(volunteers.at[v,"Last Name"])):
    #   volunteers.at[v,"Last Name"] = " "
    if(pandas.isna(volunteers.at[v,"Email"])):
       print("ERROR: No email address provided for " + volunteers.at[v,"Who"])
       volunteers.at[v,"Email"] = "NONE"
       #print("EXITING")
       #exit()

    v_hash = hash(volunteers.at[v,"Email"]+":"+volunteers.at[v,"Who"]+":"+volunteers.at[v,"First Name"]+":"+volunteers.at[v,"Last Name"])
    if(str(v_hash) not in volunteer_ids):
        volunteer_ids.append(str(v_hash))
    volunteers.at[v,"ID"] = str(v_hash)

    if(volunteers.at[v, "Spots/Items"] != 1):
        print("\nWARNING: An individual has signed up for 2 people in one job...")
        print("  "+volunteers.at[v, "Task"] + " " + volunteers.at[v, "Date"] + " " + volunteers.at[v, "Start Time"] + " " + volunteers.at[v, "Who"] + " " + volunteers.at[v, "Email"])

if(foundError):
    print("Found an error... exiting now.")
    exit()

# count the number of t-shirts needed
#tshirt_count = volunteers.groupby('Shirt size')['Who'].nunique()
tshirt_count = volunteers.groupby('Shirt size')['ID'].nunique()
tshirt_count = pandas.DataFrame({"Size": tshirt_count.index, "Count": tshirt_count.array})

# count the number of hours worked per volunteer
#hours_worked = (volunteers.groupby(['Who','First Name', 'Last Name'], as_index=False)['Hours tracking']).sum()
hours_worked = (volunteers.groupby(['ID','First Name', 'Last Name'], as_index=False)['Hours tracking']).sum()
#num_worked_shifts = (volunteers.groupby(['Who'], as_index=False)['Hours tracking']).count()
num_worked_shifts = (volunteers.groupby(['ID'], as_index=False)['Hours tracking']).count()
num_worked_shifts = num_worked_shifts.rename(columns={"Hours tracking": "Num Shifts"})
#hours_worked = hours_worked.merge(num_worked_shifts, on="Who", how="outer")
hours_worked = hours_worked.merge(num_worked_shifts, on="ID", how="outer")
hours_worked = hours_worked.sort_values(["Hours tracking"], ascending = (False))
hours_worked = hours_worked.rename(columns={"Hours tracking": "Hours Worked"})
avg_hours_worked = (hours_worked.loc[:,"Hours Worked"].sum()) / len(hours_worked)
avg_shifts_worked = (hours_worked.loc[:,"Num Shifts"].sum()) / len(hours_worked)
hours_worked.insert(len(hours_worked.columns), "Comment", "", allow_duplicates=True)
for i, row in hours_worked.iterrows():
    #comments = volunteers.query("Who=='"+row['Who']+"'")["Comment"]
    comments = volunteers.query("ID=='"+str(row['ID'])+"'")["Comment"]
    comments = comments.dropna()
    comments = "\n".join(comments)
    row['Comment'] = comments
    hours_worked.loc[i]=row
hours_worked = hours_worked.drop(columns=['ID'])

# change the timestamp to a sortable format
volunteers['Start Time'] = volunteers.apply(lambda row : convert_time(row['Start Time'], True), axis = 1)
volunteers['End Time'] = volunteers.apply(lambda row : convert_time(row['End Time'], True), axis = 1)
# sort the volunteers in an order that makes sense for us
key_columns = ["Date",
    "Start Time", #
    "Job Location",
    #"Start Time",
    "Task",
    "First Name"]
key_columns_2 = ["Date",
    "Start Time", #
    "Job Location",
    #"Start Time",
    "Task"]
volunteers = volunteers.sort_values(
    key_columns,
    ascending = (True, True, True, True, True))
# change the timestamp to a readable format
volunteers['Start Time'] = volunteers.apply(lambda row : convert_time(row['Start Time'], False), axis = 1)
volunteers['End Time'] = volunteers.apply(lambda row : convert_time(row['End Time'], False), axis = 1)

volunteers = (volunteers.reset_index()).drop(columns=["index","Check-in Time (GMT)","Signup Time (GMT)"])

# Re order the columns
volunteers = volunteers[['Event', 'Task', 'Desc', 'Job Location', 'Quantity', 'Date',
                         'Start Time', 'End Time', 'Hours tracking', 'ID', 'Email', 'Who', 'First Name', 'Last Name',
                         'Spots/Items', 'Assigner(s)', 'Requester', 'Comment', 'Phone', 'Shirt size', 'Signup Date', 'Signup Time']]

#####
# read in all_jobs
# MUST update the all_jobs tab of the sheet and then run get_jobs.py
#####
all_jobs_raw = pandas.read_csv(filepath_or_buffer="all_jobs.csv")
#all_jobs_columns = all_jobs_raw.columns.delete(-1)
all_jobs = pandas.DataFrame(columns=all_jobs_raw.columns)
for i, r in all_jobs_raw.iterrows():
    new_r = (r.drop(labels="Spots")).to_frame().T
    new_r_list = [all_jobs]
    for spots in range(r["Spots"]):
        #all_jobs = all_jobs._append(new_r, ignore_index=True) # append is deprecated. fix by using concat
        new_r_list.append(new_r)
    all_jobs = pandas.concat(new_r_list, axis=0, ignore_index=True)
all_jobs = (all_jobs.reset_index()).drop(columns="index")
#print(all_jobs)

# change the timestamp to a sortable format
all_jobs['Start Time'] = all_jobs.apply(lambda row : convert_time(row['Start Time'], True), axis = 1)
all_jobs['End Time'] = all_jobs.apply(lambda row : convert_time(row['End Time'], True), axis = 1)
all_jobs = all_jobs.sort_values(
    key_columns_2,
    ascending = (True, True, True, True))
# calculate the shift duration
all_jobs["Duration"] = all_jobs.apply(lambda row: calc_time_diff(row["Start Time"], row["End Time"]), axis = 1)
# change the timestamp to a readable format
all_jobs['Start Time'] = all_jobs.apply(lambda row : convert_time(row['Start Time'], False), axis = 1)
all_jobs['End Time'] = all_jobs.apply(lambda row : convert_time(row['End Time'], False), axis = 1)

# create the master list by aligning the job list with the volunteer registrations
# kind of gross, but it works. not sure if there's a better way to do this
# if volunteers and all_jobs were hashed appropriately, we could do a dataframe outer merge instead
temp_v = volunteers.copy()
master_list = pandas.DataFrame(columns=volunteers.columns)
#for index_j in range(len(all_jobs)):
for i, r in all_jobs.iterrows():
    if(len(temp_v)==0):
        #master_list = master_list._append(all_jobs.loc[index_j]) # append is deprecated. fix by using concat
        master_list = pandas.concat([master_list, r.to_frame().T], axis=0, ignore_index=True)
        continue
    found = False
    for index_v, row in temp_v.iterrows():
        #if(compare_jobs(row, all_jobs.loc[index_j])):
        if(compare_jobs(row, r)):
            #row['Duration'] = all_jobs.loc[index_j]['Duration']
            row['Duration'] = r['Duration']
            #master_list = master_list._append(row) # append is deprecated. fix by using concat
            master_list = pandas.concat([master_list, row.to_frame().T], axis=0, ignore_index=True)
            temp_v = temp_v.drop(index_v)
            found = True
            break
    if(found==False):
        #master_list = master_list._append(all_jobs.loc[index_j]) # append is deprecated. fix by using concat
        master_list = pandas.concat([master_list, r.to_frame().T], axis=0, ignore_index=True)

if(len(temp_v) != 0):
    print("ERROR: Not all volunteers were matched to a job")
    pandas.set_option('display.max_colwidth', None)
    pandas.set_option('display.max_columns', None)
    print(temp_v[['Task', 'Job Location', 'Date', 'Start Time', 'Email', 'First Name', 'Last Name']])

# sort the master list in an order that makes sense for us
#master_list = master_list.fillna(value="") # generates warning about mixed data types. not sure if needed.
master_list['Job Location'] = master_list.apply(lambda row : row['Job Location'].title(), axis = 1)
master_list['Start Time'] = master_list.apply(lambda row : convert_time(row['Start Time'], True), axis = 1)
master_list['End Time'] = master_list.apply(lambda row : convert_time(row['End Time'], True), axis = 1)
master_list = master_list.sort_values(
    key_columns,
    ascending = (True, True, True, True, True))
# calculate the shift duration
master_list["Duration"] = master_list.apply(lambda row: calc_time_diff(row["Start Time"], row["End Time"]), axis = 1)
master_list['Start Time'] = master_list.apply(lambda row : convert_time(row['Start Time'], False), axis = 1)
master_list['End Time'] = master_list.apply(lambda row : convert_time(row['End Time'], False), axis = 1)

# get all the email addresses based on signup date/time
emails = volunteers.copy()
#emails = (emails.groupby(['Who','First Name', 'Last Name', 'Signup Time', 'Email'], as_index=False)['Signup Date']).min()
#emails = emails.drop(columns=['Who'])
emails = (emails.groupby(['ID','First Name', 'Last Name', 'Signup Time', 'Email'], as_index=False)['Signup Date']).min()
emails = emails.drop(columns=['ID'])
emails = emails[['First Name', 'Last Name', 'Signup Date', 'Signup Time', 'Email']]

#emails['Signup Time'] = emails.apply(lambda row : convert_time(row['Signup Time'], True), axis = 1)
emails = emails.sort_values(
    ["Signup Date","Signup Time"],
    ascending = (True, True))
#emails['Signup Time'] = emails.apply(lambda row : convert_time(row['Signup Time'], False), axis = 1)
emails = emails.drop_duplicates(subset=['Email'])

#####
# Calculate the filled slots stats
all_days_and_locations = (master_list.groupby(['Date','Job Location'], as_index=False))
#for day_and_location in all_days_and_locations.groups:
    #print(day_and_location)
    #print(all_days_and_locations.get_group(day_and_location).columns)
    #print(all_days_and_locations.get_group(day_and_location))
all_days_and_locations_agg = all_days_and_locations.agg(['count'])
#print(all_days_and_locations_agg)
#print(all_days_and_locations_agg.columns)
filled_slots_per_day = all_days_and_locations_agg[[('Event','count')]]
filled_slots_per_day = [x for x in filled_slots_per_day[('Event','count')]]
#print(filled_slots_per_day)

total_slots_per_day = all_days_and_locations_agg[[('Task','count')]]
total_slots_per_day = [x for x in total_slots_per_day[('Task','count')]]
#print(total_slots_per_day)

# calculate the unique volunteers per day and location
all_days_and_locations_unique = all_days_and_locations.agg(['unique'])
unique_ids_per_day_and_location = all_days_and_locations_unique[[('ID','unique')]]
#print(unique_ids_per_day_and_location)
unique_volunteers = []
for x in unique_ids_per_day_and_location[('ID','unique')]:
    removed_nan = []
    for y in x:
        if y == y:
            removed_nan.append(y)
    #print(removed_nan)
    unique_volunteers.append(len(removed_nan))
#print(unique_volunteers)
###

unfilled_slots_stats = [total_slots_per_day[i]-filled_slots_per_day[i] for i in range(len(total_slots_per_day))]
#print(unfilled_slots_stats)

# sum up all the columns
total_slots_per_day.append(reduce(lambda a, b: a+b, total_slots_per_day))
filled_slots_per_day.append(reduce(lambda a, b: a+b, filled_slots_per_day))
unfilled_slots_stats.append(reduce(lambda a, b: a+b, unfilled_slots_stats))

# calculate the percents
filled_percentage = [filled_slots_per_day[i]/total_slots_per_day[i] for i in range(len(total_slots_per_day))]
#print(filled_percentage)

# make one matrix for all the slot stats
filled_slots_stats = list(zip(filled_slots_per_day,unfilled_slots_stats,total_slots_per_day,filled_percentage))
#print(filled_slots_stats)

# Calculate the filled slots stats
#####


###############################################################################################################################################################
################################################# upload to google sheets
###############################################################################################################################################################
gsheet = pygsheets.authorize(service_file='./google_api_key.json')
sheet = gsheet.open('USAU Nationals 2025 Volunteers')
#sheet = gsheet.open('USAU Nationals 2023 Volunteers')
#sheet = gsheet.open('USAU Nationals 2022 Volunteers')
#sheet = gsheet.open('USAU Nationals 2021 Volunteers')
#sheet = gsheet.open('USAU Nationals 2019 Volunteers')

try:
    #worksheet = sheet.worksheet("title", "master_list_autogen")
    #sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "volunteers_by_signup_autogen")
    sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "checkin_autogen")
    sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "stats_autogen")
    sheet.del_worksheet(worksheet)
    #worksheet = sheet.worksheet("title", "volunteers_autogen")
    #sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "emails_autogen")
    sheet.del_worksheet(worksheet)
except Exception:
    pass

# add the raw volunteers data in a tab
#worksheet = sheet.add_worksheet("volunteers_autogen")
#worksheet.set_dataframe(volunteers, pygsheets.Address("A1"))
#worksheet.adjust_column_width(start=1, end=40, pixel_size=None)
#worksheet.update_dimensions_visibility(start=1, end=2, dimension="COLUMNS", hidden=True) # Event, Event Location
#worksheet.update_dimensions_visibility(start=5, end=7, dimension="COLUMNS", hidden=True) # Job Description, Job Notes, Job Rate
#worksheet.update_dimensions_visibility(start=10, dimension="COLUMNS", hidden=True) # Shift End Date
#worksheet.update_dimensions_visibility(start=12, end=24, dimension="COLUMNS", hidden=True) # lots
#worksheet.update_dimensions_visibility(start=27, end=30, dimension="COLUMNS", hidden=True) # lots
#worksheet.update_dimensions_visibility(start=36, dimension="COLUMNS", hidden=True)
#worksheet.update_dimensions_visibility(start=38, dimension="COLUMNS", hidden=True)
#worksheet.frozen_rows = 1

# add the raw volunteers data sorted by [signup date, signup time]
#volunteers['Signup Time'] = volunteers.apply(lambda row : convert_time(row['Signup Time'], True), axis = 1)
volunteers = volunteers.sort_values(
    ["Signup Date",
    "Signup Time",],
    ascending = (True, True))
#volunteers['Signup Time'] = volunteers.apply(lambda row : convert_time(row['Signup Time'], False), axis = 1)
worksheet = sheet.add_worksheet("volunteers_by_signup_autogen")
worksheet.set_dataframe(volunteers, pygsheets.Address("A1"))
worksheet.adjust_column_width(start=1, end=40, pixel_size=None)
worksheet.update_dimensions_visibility(start=1, dimension="COLUMNS", hidden=True) # Event
worksheet.update_dimensions_visibility(start=3, dimension="COLUMNS", hidden=True) # Description
worksheet.update_dimensions_visibility(start=5, dimension="COLUMNS", hidden=True) # Quantity
worksheet.update_dimensions_visibility(start=9, end=10, dimension="COLUMNS", hidden=True) # Hours Tracking, ID
worksheet.update_dimensions_visibility(start=12, dimension="COLUMNS", hidden=True) # Who
worksheet.update_dimensions_visibility(start=15, end=17, dimension="COLUMNS", hidden=True) # Spots/Items, Assigner, Requester
worksheet.update_dimensions_visibility(start=23, end=27, dimension="COLUMNS", hidden=True) # Spots/Items, Assigner, Requester
worksheet.frozen_rows = 1

# not sure how to do conditional formatting with a gradient curve
#worksheet.add_conditional_formatting(
#    'H2', 'H'+str(worksheet.rows),
#    'NUMBER_BETWEEN',
#    {'backgroundColor':{'red':1}},
#    ['1','5'])

######
# print out all the jobs+volunteers
#worksheet = sheet.add_worksheet("master_list_autogen")
#worksheet.set_dataframe(master_list, pygsheets.Address("A1"))
#worksheet.adjust_column_width(start=1, end=40, pixel_size=None)
#worksheet.update_dimensions_visibility(start=1, end=2, dimension="COLUMNS", hidden=True) # Event, Event Location
#worksheet.update_dimensions_visibility(start=5, end=7, dimension="COLUMNS", hidden=True) # Job Description, Job Notes, Job Rate
#worksheet.update_dimensions_visibility(start=10, dimension="COLUMNS", hidden=True) # Shift End Date
#worksheet.update_dimensions_visibility(start=12, end=29, dimension="COLUMNS", hidden=True) # lots
#worksheet.update_dimensions_visibility(start=35, dimension="COLUMNS", hidden=True)
#worksheet.update_dimensions_visibility(start=37, dimension="COLUMNS", hidden=True)
#worksheet.frozen_rows = 1

######
# print out the checkin sheet
worksheet = sheet.add_worksheet("checkin_autogen")
worksheet.set_dataframe(master_list, pygsheets.Address("A1"))
worksheet.adjust_column_width(start=1, end=40, pixel_size=None)
worksheet.update_dimensions_visibility(start=1, dimension="COLUMNS", hidden=True) # Event
worksheet.update_dimensions_visibility(start=3, dimension="COLUMNS", hidden=True) # Description
worksheet.update_dimensions_visibility(start=5, dimension="COLUMNS", hidden=True) # Quantity
worksheet.update_dimensions_visibility(start=9, end=12, dimension="COLUMNS", hidden=True) # Hours Tracking, ID, Email, Who
worksheet.update_dimensions_visibility(start=15, end=19, dimension="COLUMNS", hidden=True) # lots
worksheet.update_dimensions_visibility(start=21, end=27, dimension="COLUMNS", hidden=True) # lots
worksheet.frozen_rows = 1

######
# add some stats
worksheet = sheet.add_worksheet("stats_autogen", rows=400)
output_row=1

# print dates and job locations
all_days_and_locations_df = pandas.DataFrame([x for x in all_days_and_locations.groups], columns = ["Date", "Job Location"])
worksheet.set_dataframe(all_days_and_locations_df, "A"+str(output_row))

# number of unique volunteers
unique_volunteers_df = pandas.DataFrame(unique_volunteers, columns=["Unique Volunteers"])
worksheet.set_dataframe(unique_volunteers_df, "C"+str(output_row))

filled_slots_stats = pandas.DataFrame(filled_slots_stats)
filled_slots_stats.columns=["Filled Slots","Unfilled Slots","Total Slots","Filled Percent"]
worksheet.set_dataframe(filled_slots_stats, "D"+str(output_row))
output_row += len(unique_volunteers)+1
worksheet.update_value("A"+str(output_row), "All Days")
worksheet.update_value("B"+str(output_row), "All Locations")
worksheet.update_value("C"+str(output_row), len(pandas.unique(volunteers['Who']))) # total unique volunteers

# t-shirt counts
worksheet.update_value("I1", "T-Shirts")
worksheet.set_dataframe(tshirt_count, "I2")

# last updated datetime
# datetime object containing current date and time
now = datetime.now()
# YYYY/mm/dd H:M:S
dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
worksheet.update_value("I10", "Last Updated")
worksheet.update_value("J10", dt_string)

#output_row += len(unique_v) if (len(unique_v) > len(tshirt_count)) else len(tshirt_count)
output_row += 2

# hours worked per volunteer
worksheet.update_value("A"+str(output_row), "Hours worked\nper volunteer")
worksheet.set_dataframe(hours_worked, "A"+str(output_row+1))
output_row = 16+len(hours_worked)
#output_row += 2

worksheet.update_value("B"+str(output_row), "Average")
worksheet.update_value("C"+str(output_row), avg_hours_worked)
worksheet.update_value("D"+str(output_row), avg_shifts_worked)

worksheet.adjust_column_width(start=1, end=4, pixel_size=None)

# email addresses based on signup date/time
worksheet = sheet.add_worksheet("emails_autogen")
worksheet.set_dataframe(emails, pygsheets.Address("A1"))
worksheet.adjust_column_width(start=1, end=6, pixel_size=None)
worksheet.frozen_rows = 1