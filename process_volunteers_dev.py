from requests_html import HTMLSession # pip install requests-html
import csv
import time
import all_volunteer_slots2

def compare_rows(a,b):
    if( a[2] == b[2] and a[3] == b[3] and a[7] == b[7] and a[8] == b[8] ):
        return True
    return False

login = {
    'email': 'kdtemple@gmail.com',
    'password': 'VFR$o9lo9'
}
session = HTMLSession()
r = session.post('https://app.volunteerlocal.com/manage/', data=login)


params = {
    'go': 'volunteer_export',
    #'id': '57945',
    #'57945': 'on',
    #'export_event_id': "57945",
    #'export_event_id': [57945],
    #'standard_field': ["event","event_location"],
    #'volunteer_export_filter_job_list': "j57945",
    #'job_id': 'on'
}
#r = session.get('https://app.volunteerlocal.com/manage/?go=volunteer_export', params=params)
#r = session.post('https://app.volunteerlocal.com/manage/?go=volunteer_export', params=params)
#r = session.post('https://app.volunteerlocal.com/manage/?go=volunteer_export&id=57945', params=params)
#r = session.get('https://app.volunteerlocal.com/manage/?go=volunteer_export', params=params)
r = session.post('https://app.volunteerlocal.com/manage/', params=params)
#print(r.html.find('#export_event_id', first=True))
#print(r.html.find('#export_event_fields', first=True))
#r.html.render(sleep=1)
with open("test0.html", mode='w') as outfile:
    for row in r.html:
        outfile.write(row)

print(r.html)
#print(r.request.headers)
#print(r.request.body)
#print(r.request.url)
##print(r.text)
print("multiple option: " + str(r.html.search("USAU Club Nationals 2021")))
print("event name checkbox: " + str(r.html.search(" Event name")))
r2 = r.html.find('#export_event_fields', first=True)
if(r2 is not None):
    print("export event fields: " + str(r2.text))
r2 = r.html.find('#export_event_id', first=True)
if(r2 is not None):
    print("event id: " + str(r2.text))

r2 = r.html.find('#volunteer_export_filter_job_list', first=True)
if(r2 is not None):
    print("filter_job_list: " + str(r2.text))

#r.html.render()  # this call executes the js in the page

exit()





file = "usau-nationals - volunteers.csv"

mylist = []

with open(file, mode='r') as infile:
    reader = csv.reader(infile)
    for row in reader:
        mylist.append(row)

i=0
for field in mylist[1]:
    print(str(i)+"::"+field)
    i+=1

for row in mylist[1:]:
    print(row)
    row[8] = time.strptime(row[8],"%I:%M %p")
    row[4] = "\"" + row[4] + "\""
    row[37] = "\"" + row[37] + "\""
mylist[1:] = sorted(mylist[1:], key=lambda k: (k[7], k[8]))

all_jobs = all_volunteer_slots2.all_jobs
for row in all_jobs:
    row[0] = "USAU Nationals 2019"
    row[1] = "San Diego"
    row[4] = "\"" + row[4] + "\""
    row[8] = time.strptime(row[8],"%I:%M %p")
    for i in range(27):
        row.append("")
all_jobs = sorted(all_jobs, key=lambda k: (k[7], k[8]))

count = 1
final_list = []
final_list.append(mylist[0])
for i in range(len(all_jobs)):
    if( count >= len(mylist) or (not compare_rows(all_jobs[i], mylist[count])) ):
        #mylist.insert(i+1, all_jobs[i])
        final_list.append(all_jobs[i])
    else:
        final_list.append(mylist[count])
        count = count + 1
    final_list[-1][8] = time.strftime("%I:%M %p", final_list[-1][8])

print("----")
print(final_list[0][0])
print(all_jobs[0][0])
print(mylist[1][0])
print(final_list[-1])

with open("master_list.csv", mode='w') as outfile:
    for row in final_list:
        for i in range(38):
            outfile.write(str(row[i]) + ",")
        outfile.write("\n")

#with open("sorted_input.csv", mode='w') as outfile:
#    for row in mylist:
#        row[8] = time.strftime("%I:%M %p", row[8])
#        for i in range(38):
#            outfile.write(str(row[i]) + ",")
#        outfile.write("\n")


