import csv
import time
import all_volunteer_slots2

def compare_rows(a,b):
    if( a[2] == b[2] and a[3] == b[3] and a[7] == b[7] and a[8] == b[8] ):
        return True
    return False

file = "usau-nationals-2019 - VolunteerLocal.csv"

mylist = []

with open(file, mode='r') as infile:
    reader = csv.reader(infile)
    for row in reader:
        mylist.append(row)


for row in mylist[1:]:
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

print "----"
print final_list[0][0]
print all_jobs[0][0]
print mylist[1][0]
print final_list[-1]

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


