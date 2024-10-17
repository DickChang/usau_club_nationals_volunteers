import csv
import time
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
    if(j1["Shift start date"] == j2["Shift start date"] and
        j1["Job Location"].casefold() == j2["Job Location"].casefold() and
        j1["Shift start time"] == j2["Shift start time"] and
        j1["Job"].casefold() == j2["Job"].casefold()):
        return True
    else:
        return False


file = "usau-nationals - volunteers.csv" # csv containing all the volunteer signup info
#file = "usau-nationals - volunteers_2019.csv" # csv containing all the volunteer signup info

volunteers = pandas.read_csv(file)

volunteers.insert(12, "Shift Duration", "", allow_duplicates=True)
volunteers.insert(30, "T-Shirt", "", allow_duplicates=True)
for v in volunteers.index:
    # adjust the fricking case abomination
    volunteers.at[v, "Job Location"] = volunteers.at[v, "Job Location"].title()
    volunteers.at[v, "Job"] = volunteers.at[v, "Job"].title()

    # adjust the t-shirt columns
    tshirt = ""
    if volunteers.at[v, "(Tshirt size (unisex)) XS"] == "X":
        tshirt = "XS"
    elif volunteers.at[v, "(Tshirt size (unisex)) S"] == "X":
        tshirt = "S"
    elif volunteers.at[v, "(Tshirt size (unisex)) M"] == "X":
        tshirt = "M"
    elif volunteers.at[v, "(Tshirt size (unisex)) L"] == "X":
        tshirt = "L"
    elif volunteers.at[v, "(Tshirt size (unisex)) XL"] == "X":
        tshirt = "XL"
    elif volunteers.at[v, "(Tshirt size (unisex)) XXL"] == "X":
        tshirt = "XXL"
    elif volunteers.at[v, "(Tshirt size (unisex)) XXXL"] == "X":
        tshirt = "XXXL"
    else:
        tshirt = "NA" # default value
    volunteers.at[v, "T-Shirt"] = tshirt

    # remove NaN from requests
    if volunteers.at[v, "Requests? "] != volunteers.at[v, "Requests? "]:
        volunteers.at[v, "Requests? "] = ""

    # change the timestamp to a sortable format
    volunteers.at[v, "Shift start time"] = convert_time(volunteers.at[v, "Shift start time"], True)
    volunteers.at[v, "Shift end time"] = convert_time(volunteers.at[v, "Shift end time"], True)

    # calculate the shift duration
    volunteers.at[v, "Shift Duration"] = calc_time_diff(volunteers.at[v, "Shift start time"], volunteers.at[v, "Shift end time"])

# delete the extra tshirt columns
volunteers = volunteers.drop(columns=[
    '(Tshirt size (unisex)) XS',
    '(Tshirt size (unisex)) S',
    '(Tshirt size (unisex)) M',
    '(Tshirt size (unisex)) L',
    '(Tshirt size (unisex)) XL',
    '(Tshirt size (unisex)) XXL',
    '(Tshirt size (unisex)) XXXL'
    ])

# count the number of t-shirts needed
tshirt_count = volunteers.groupby('T-Shirt')['Volunteer Identifier'].nunique()
tshirt_count = pandas.DataFrame({"Size": tshirt_count.index, "Count": tshirt_count.array})

# count the number of unique volunteers per day
unique_v = (volunteers.groupby(['Shift start date','Job Location'], as_index=False)['Volunteer Identifier']).nunique()
unique_v = unique_v.rename(columns={"Volunteer Identifier": "Unique Volunteers"})

# TODO
# count the number of filled slots per day
#unique_per_day = pandas.DataFrame[volunteers['Shift start date'] == "2022-10-20" & volunteers['Job Location'] == "Polo Fields"]
#print(unique_per_day)

# count the number of hours worked per volunteer
hours_worked = (volunteers.groupby(['Volunteer Identifier','First Name', 'Last Name', 'Requests? '], as_index=False)['Shift Duration']).sum()
num_worked_shifts = (volunteers.groupby(['Volunteer Identifier'], as_index=False)['Shift Duration']).count()
num_worked_shifts = num_worked_shifts.rename(columns={"Shift Duration": "Num Shifts"})
hours_worked = hours_worked.merge(num_worked_shifts, on="Volunteer Identifier", how="outer")
hours_worked = hours_worked.sort_values(["Shift Duration"], ascending = (False))
hours_worked = hours_worked.rename(columns={"Shift Duration": "Hours Worked"})
hours_worked.insert(len(hours_worked.columns), 'Requests', hours_worked['Requests? '])
hours_worked = hours_worked.drop(columns=['Volunteer Identifier', 'Requests? '])
avg_hours_worked = (hours_worked.loc[:,"Hours Worked"].sum()) / len(hours_worked)
avg_shifts_worked = (hours_worked.loc[:,"Num Shifts"].sum()) / len(hours_worked)

# sort the volunteers in an order that makes sense for us
key_columns = ["Shift start date",
    "Job Location",
    "Shift start time",
    "Job",
    "First Name"]
key_columns_2 = ["Shift start date",
    "Job Location",
    "Shift start time",
    "Job"]
volunteers = volunteers.sort_values(
    key_columns,
    ascending = (True, True, True, True, True))

# change the timestamp to a readable format
volunteers['Shift start time'] = volunteers.apply(lambda row : convert_time(row['Shift start time'], False), axis = 1)
volunteers['Shift end time'] = volunteers.apply(lambda row : convert_time(row['Shift end time'], False), axis = 1)

volunteers = (volunteers.reset_index()).drop(columns="index")


#####
# read in all_jobs
#####
all_jobs_raw = pandas.read_csv(filepath_or_buffer="all_jobs.csv")
all_jobs_columns = all_jobs_raw.columns.delete(-1)
all_jobs = pandas.DataFrame(columns=all_jobs_columns)
for i, r in all_jobs_raw.iterrows():
    for spots in range(r["Spots"]):
        all_jobs = all_jobs.append(r.drop(labels="Spots"), ignore_index=True)
all_jobs = (all_jobs.reset_index()).drop(columns="index")

all_jobs['Shift start time'] = all_jobs.apply(lambda row : convert_time(row['Shift start time'], True), axis = 1)
all_jobs['Shift end time'] = all_jobs.apply(lambda row : convert_time(row['Shift end time'], True), axis = 1)
all_jobs = all_jobs.sort_values(
    key_columns_2,
    ascending = (True, True, True, True))
# calculate the shift duration
all_jobs["Shift Duration"] = all_jobs.apply(lambda row: calc_time_diff(row["Shift start time"], row["Shift end time"]), axis = 1)
all_jobs['Shift start time'] = all_jobs.apply(lambda row : convert_time(row['Shift start time'], False), axis = 1)
all_jobs['Shift end time'] = all_jobs.apply(lambda row : convert_time(row['Shift end time'], False), axis = 1)


# create the master list by aligning the job list with the volunteer registrations
# kind of gross, but it works. not sure if there's a better way to do this
# if volunteers and all_jobs were hashed appropriately, we could do a dataframe outer merge instead
temp_v = volunteers.copy()
master_list = pandas.DataFrame(columns=volunteers.columns)
for index_j in range(len(all_jobs)):
    if(len(temp_v)==0):
        master_list = master_list.append(all_jobs.loc[index_j])
        continue
    found = False
    for index_v, row in temp_v.iterrows():
        if(compare_jobs(row, all_jobs.loc[index_j])):
            row['Duration'] = all_jobs.loc[index_j]['Duration']
            master_list = master_list.append(row)
            temp_v = temp_v.drop(index_v)
            found = True
            break
    if(found==False):
        master_list = master_list.append(all_jobs.loc[index_j])
if(len(temp_v) != 0):
    print("ERROR: Not all volunteers were matched to a job")
    pandas.set_option('display.max_colwidth', None)
    pandas.set_option('display.max_columns', None)
    print(temp_v[['Job', 'Job Location', 'Shift start date', 'Shift start time', 'Email', 'First Name', 'Last Name']])

# sort the master list in an order that makes sense for us
master_list = master_list.fillna(value="")
master_list['Job Location'] = master_list.apply(lambda row : row['Job Location'].title(), axis = 1)
master_list['Shift start time'] = master_list.apply(lambda row : convert_time(row['Shift start time'], True), axis = 1)
master_list['Shift end time'] = master_list.apply(lambda row : convert_time(row['Shift end time'], True), axis = 1)
master_list = master_list.sort_values(
    key_columns,
    ascending = (True, True, True, True, True))
# calculate the shift duration
master_list["Shift Duration"] = master_list.apply(lambda row: calc_time_diff(row["Shift start time"], row["Shift end time"]), axis = 1)
master_list['Shift start time'] = master_list.apply(lambda row : convert_time(row['Shift start time'], False), axis = 1)
master_list['Shift end time'] = master_list.apply(lambda row : convert_time(row['Shift end time'], False), axis = 1)

# get all the email addresses based on signup date/time
emails = volunteers.copy()
emails = (emails.groupby(['Volunteer Identifier','First Name', 'Last Name', 'Signup Time', 'Email'], as_index=False)['Signup Date']).min()
emails = emails.drop(columns=['Volunteer Identifier'])
emails = emails[['First Name', 'Last Name', 'Signup Date', 'Signup Time', 'Email']]

emails['Signup Time'] = emails.apply(lambda row : convert_time(row['Signup Time'], True), axis = 1)
emails = emails.sort_values(
    ["Signup Date","Signup Time"],
    ascending = (True, True))
emails['Signup Time'] = emails.apply(lambda row : convert_time(row['Signup Time'], False), axis = 1)
emails = emails.drop_duplicates(subset=['Email'])

# Calculate the filled slots stats
slots_per_day = (master_list.groupby(['Shift start date','Job Location'], as_index=False).agg(['count']))[[('Event','count')]]
slots_per_day.columns = slots_per_day.columns.get_level_values(0) + '_' +  slots_per_day.columns.get_level_values(1)
slots_per_day = [x for x in slots_per_day["Event_count"]]

filled_slots_per_day = (volunteers.groupby(['Shift start date','Job Location'], as_index=False).agg(['count']))[[('Event','count')]]
filled_slots_per_day.columns = filled_slots_per_day.columns.get_level_values(0) + '_' +  filled_slots_per_day.columns.get_level_values(1)
filled_slots_per_day = [x for x in filled_slots_per_day["Event_count"]]

# get all the unique date/location combos and align the "filled_slots_per_day" data by filling the empty day/locations with 0
unique_dates_and_locations = master_list.groupby(['Shift start date','Job Location'], as_index=False).agg(['unique'])
unique_dates_and_locations = (unique_dates_and_locations.drop(unique_dates_and_locations.iloc[2:],axis = 1)).index
filled_dates_and_locations = volunteers.groupby(['Shift start date','Job Location'], as_index=False).agg(['unique'])
filled_dates_and_locations = (filled_dates_and_locations.drop(filled_dates_and_locations.iloc[2:],axis = 1)).index
unique_temp = volunteers.groupby(['Shift start date','Job Location'], as_index=False).agg(['unique'])
unique_temp = (unique_temp.drop(unique_temp.iloc[2:],axis = 1)).index
i=0
for row in unique_dates_and_locations:
    if(row == filled_dates_and_locations[i]):
        i+=1
        continue
    else:
        # found a day/location that didn't have anyone signed up.
        filled_slots_per_day.insert(i,0)
        unique_v.loc[i-0.5] = [row[0],row[1],0]
        unique_v = unique_v.sort_index().reset_index(drop=True)
#i=0
#do the same for unique volunteers per day/location
#for row in unique_dates_and_locations:
    #if(row == unique_temp[i]):
        #i+=1
        #continue
    #else:
        ## found a day/location that didn't have anyone signed up.
        #unique_v.loc[i-0.5] = [row[0],row[1],0]
        #unique_v = unique_v.sort_index().reset_index(drop=True)

unfilled_slots_stats = [slots_per_day[i]-filled_slots_per_day[i] for i in range(len(slots_per_day))]

# sum up all the columns
slots_per_day.append(reduce(lambda a, b: a+b, slots_per_day))
filled_slots_per_day.append(reduce(lambda a, b: a+b, filled_slots_per_day))
unfilled_slots_stats.append(reduce(lambda a, b: a+b, unfilled_slots_stats))

# calculate the percents
filled_percentage = [filled_slots_per_day[i]/slots_per_day[i] for i in range(len(slots_per_day))]

# make one matrix for all the slot stats
filled_slots_stats = list(zip(filled_slots_per_day,unfilled_slots_stats,slots_per_day,filled_percentage))

###############################################################################################################################################################
################################################# upload to google sheets
###############################################################################################################################################################
gsheet = pygsheets.authorize(service_file='./usau-club-nationals-volunteers-61aca3eb7368.json')
sheet = gsheet.open('USAU Nationals 2022 Volunteers')
#sheet = gsheet.open('USAU Nationals 2021 Volunteers')
#sheet = gsheet.open('USAU Nationals 2019 Volunteers')

try:
    #worksheet = sheet.worksheet("title", "master_list_autogen")
    #sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "stats_autogen")
    sheet.del_worksheet(worksheet)
    #worksheet = sheet.worksheet("title", "volunteers_autogen")
    #sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "volunteers_by_signup_autogen")
    sheet.del_worksheet(worksheet)
    worksheet = sheet.worksheet("title", "checkin_autogen")
    sheet.del_worksheet(worksheet)
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
volunteers['Signup Time'] = volunteers.apply(lambda row : convert_time(row['Signup Time'], True), axis = 1)
volunteers = volunteers.sort_values(
    ["Signup Date",
    "Signup Time",],
    ascending = (True, True))
volunteers['Signup Time'] = volunteers.apply(lambda row : convert_time(row['Signup Time'], False), axis = 1)
worksheet = sheet.add_worksheet("volunteers_by_signup_autogen")
worksheet.set_dataframe(volunteers, pygsheets.Address("A1"))
worksheet.adjust_column_width(start=1, end=40, pixel_size=None)
worksheet.update_dimensions_visibility(start=1, end=2, dimension="COLUMNS", hidden=True) # Event, Event Location
worksheet.update_dimensions_visibility(start=5, end=7, dimension="COLUMNS", hidden=True) # Job Description, Job Notes, Job Rate
worksheet.update_dimensions_visibility(start=10, dimension="COLUMNS", hidden=True) # Shift End Date
worksheet.update_dimensions_visibility(start=12, end=24, dimension="COLUMNS", hidden=True) # lots
worksheet.update_dimensions_visibility(start=27, end=30, dimension="COLUMNS", hidden=True) # lots
worksheet.update_dimensions_visibility(start=36, dimension="COLUMNS", hidden=True)
worksheet.update_dimensions_visibility(start=38, dimension="COLUMNS", hidden=True)
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
worksheet.update_dimensions_visibility(start=1, end=2, dimension="COLUMNS", hidden=True) # Event, Event Location
worksheet.update_dimensions_visibility(start=5, end=7, dimension="COLUMNS", hidden=True) # Job Description, Job Notes, Job Rate
worksheet.update_dimensions_visibility(start=10, dimension="COLUMNS", hidden=True) # Shift End Date
worksheet.update_dimensions_visibility(start=12, end=30, dimension="COLUMNS", hidden=True) # lots
worksheet.update_dimensions_visibility(start=32, dimension="COLUMNS", hidden=True) # lots
worksheet.update_dimensions_visibility(start=35, end=40, dimension="COLUMNS", hidden=True) # lots
worksheet.frozen_rows = 1

######
# add some stats
worksheet = sheet.add_worksheet("stats_autogen", rows=400)
output_row=1

# number of unique volunteers
worksheet.update_value("A"+str(output_row), "Unique Volunteers")
output_row += 1
worksheet.set_dataframe(unique_v, "A"+str(output_row))
filled_slots_stats = pandas.DataFrame(filled_slots_stats)
filled_slots_stats.columns=["Filled Slots","Unfilled Slots","Total Slots","Filled Percent"]
worksheet.set_dataframe(filled_slots_stats, "D"+str(output_row))
output_row += len(unique_v)+1
worksheet.update_value("A"+str(output_row), "All Days")
worksheet.update_value("B"+str(output_row), "All Locations")
worksheet.update_value("C"+str(output_row), len(pandas.unique(volunteers['Volunteer Identifier']))) # total unique volunteers

# t-shirt counts
worksheet.update_value("I1", "T-Shirts")
worksheet.set_dataframe(tshirt_count, "I2")

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