'''Reference:http://bottlepy.org/docs/dev/tutorial.html'''
'''Reference:http://stackoverflow.com/questions/28857520/unit-testing-bottle-py-application-that-uses-request-body-results-in-keyerror'''
'''Name:Vibha Dhar'''
''''''
'''Assignment-5'''


import boto
import boto.s3.connection
import time
import MySQLdb
import csv
from StringIO import StringIO
import sys
import random
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from bottle import route, request, response, template, run, error, app
from bottle import Bottle
from bottle import request, run, post, tob
from io import BytesIO
body = "abc"


#uploading to the bucket
access_key = 'XXXXXXXXXXXXXX'
secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
filename= "university.csv"

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
                )
bucket = conn.get_bucket('vibhabuck')

key = bucket.new_key(filename)

bottle = Bottle()



# connect to the table in dynamo DB
def connectingtable():
	con = boto.dynamodb2.connect_to_region('us-west-2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
	University = Table("University",connection = con)
	return University


#render page to upload file
@bottle.route('/upload_file')
def get_upload_page():
	print "Calling get_upload_page method"
	return template("Views/upload_file")


#Method for uploading

@bottle.route('/upload_file', method= 'POST')
def upload_file(filename):
	print filename
	start = time.time()
	key.set_contents_from_filename(filename)
	print "file uploaded"
	end = time.time()
	taken_time = end - start
	key.set_acl('public-read')

	print "File uploaded successfull \n time taken to upload file is : " + str(taken_time)





#Retrievng page for dynamic query using city as the field
@bottle.route('/dynamic_query',method = 'GET')
def query():
	University = connectingtable()
	values = University.scan()
	array = []
	for value in values:
		print value["CITY"]
		array.append(str(value["CITY"]))
		#print array
	return template("Views/dynamic_query", array = array)

#Random Query execution 


@bottle.route("/run_query", method = "POST")
def randomqueryexecution():
	CITY = request.forms.get('CITY', '')
	
	University = connectingtable()
	all_data = University.scan()
	print all_data
	city_result = []
	html = "<center><h2> Dynamic query result</h2></center>"
	html = "<h4> University in the city " + str(CITY) + " : <br></h4>" 	
	html += "<table style = 'border: 1px solid black'><tr><th>University id</th> <th>University name</th></tr>"
	for data in all_data:
		print data["CITY"]
		if data["CITY"] == str(CITY):
			html += "<tr><td>" + data["UNITID"] + "</td><td>"  + data["INSTNM"] + "</td><tr>"	 
	html += "</table>"
	return html
	

@bottle.route('/')
def home():
   return 'Hello!'

#Parsing the file in the bucket
def readingfile(filename,University):
    file_value = bucket.get_key(filename)
    csv_data = csv.reader(StringIO(file_value.read()),delimiter=',',quotechar = '"')
    print "file read successfull"
    result = insert(csv_data,University)
    return result

#Dynamo DB insertion in the following method
def insert(csv_data,University):
	csv_data.next()
	i =1;
	for row in csv_data:
		UNITID = str(row[0])
	        INSTNM = str(row[1])
		ADDR = str(row[2])
		CITY = str(row[3])
		STABBR = str(row[4])
		ZIP = str(row[5])
		FIPS = str(row[6])
		OBEREG = str(row[7])
		CHFNM=str(row[8])
		CHFTITLE=str(row[9])
		GENTELE=str(row[10])
		FAXTELE=str(row[11])
		EIN=str(row[12])
		OPEID=str(row[13])
		OPEFLAG=str(row[14])
		WEBADDR=str(row[15])
		University.put_item( data = { 'UNITID': UNITID,'INSTNM':INSTNM,'ADDR': ADDR,'CITY':CITY , 'STABBR': STABBR, 'ZIP' :ZIP,'FIPS':FIPS,'OBEREG':OBEREG,'CHFNM':CHFNM,'CHFTITLE':CHFTITLE,'GENTELE':GENTELE,'FAXTELE':FAXTELE,'EIN':EIN,'OPEID':OPEID,'OPEFLAG':OPEFLAG,
'WEBADDR': str(row[15]) })
		print "row inserted : " + str(i)
		i+=1
		if(i == 20):
			break


@bottle.route("/", method = 'GET')


def insert_data():
	University = connectingtable()
	#print con.describe_table('University')
	insert_process = readingfile(filename,University)
	x    = University.scan()
	html = "<table><tr><th>University id</th> <th>University name</th></tr>"
	for value in x:
		print value["UNITID"] + " : " + value["INSTNM"]
		html += "<tr><td>" + value["UNITID"] + "</td><td>" + value["INSTNM"] + "</td><tr>" 
	html += "</table>"
	return html



@error(404)
def error404(error):
	return '404 error !!!!!' 


if __name__ == '__main__':
    	bottle.run(debug=True)
	print "main called"
	##connect_table()
	#get_upload_page()
	#upload_file(filename)
	insert_data()
	#insert(csv_data,University)
run(host='ec2-52-25-214-194.us-west-2.compute.amazonaws.com', port=8080, reloader=True)





