Requirement / Business case

Write a program in Scala that downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz and uses Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace.  Package the application in a docker container.

Analysis

1.Write Scala utility to download the file
2.Write the following Scala api
a)API for parsing the log file
b)Since the requirement is to find the top N most frequent visitor/url for each day, lets keep only these three columns ‘visitor’, ‘date’, ‘url’ and drop the remaining columns
c)Additional api to pull the null records
d)All configurations should be bundled outside of the application code
e)API for getTopNVisitor & getTopNURL
f)API to write the output to the file system

Source Code Location


Class	Description	Location

com.util.parser.LogParser
	Main class having all utility methods
	https://github.com/raj-sparkworks/spark-artifacts/blob/main/scala/com/util/parser/LogParser.scala


com.util.parser.FileUtil
	A class to download the file from FTP location
	https://github.com/raj-sparkworks/spark-artifacts/blob/main/scala/com/util/parser/FileUtil.scala


log_parser.properties 
	This file is not packed with the main jar file and it is located external holding configuration information	https://github.com/raj-sparkworks/spark-artifacts/blob/main/scala/log_parser.properties




Artifacts / Dependencies

Artifacts	Description
thelogparser_2.11-0.1.jar
	The main artifacts having business use case 
config-1.3.2.jar
	Dependency jar for reading config file
log4j-api-2.11.2.jar
	Dependency jar for logger
log4j-core-2.11.2.jar
	Dependency jar for logger






Software Version

Software	Version	Description
Spark	2.3.4	
Scala	2.11.12	
SBT	0.13.18	
Platform	Google Cloud - DataProc Cluster
Image version : 1.4.61-debian10	


How to run ?

I have tested this application in GCP - DataProc Cluster and in Hortonworks Sandbox [Quickstart VM] and I believe it should run in any Spark cluster

1.Please download the following artifacts, dependencies and config files from the below git hub location

Artifacts	Location
thelogparser_2.11-0.1.jar
	https://github.com/raj-sparkworks/spark-artifacts/blob/main/main_jar/thelogparser_2.11-0.1.jar

config-1.3.2.jar
	https://github.com/raj-sparkworks/spark-artifacts/blob/main/dependencies/config-1.3.2.jar

log4j-api-2.11.2.jar
	https://github.com/raj-sparkworks/spark-artifacts/blob/main/dependencies/log4j-api-2.11.2.jar

log4j-core-2.11.2.jar
	https://github.com/raj-sparkworks/spark-artifacts/blob/main/dependencies/log4j-core-2.11.2.jar

log_parser.properties	https://github.com/raj-sparkworks/spark-artifacts/blob/main/scala/log_parser.properties


2.We can keep the dependencies, main jar file and the config file in different location or in same location [just for simplicity]. 

3.Please execute the following spark-submit command 
4.[Pls change the dependencies, main jar and config file location accordingly]




spark-submit --class com.util.parser.LogParser --files /home/rajkumar_sukumar/log_parser.properties --conf spark.driver.extraJavaOptions=-Dconfig.file=log_parser.properties --conf spark.executor.extraJavaOptions=-Dconfig.file=log_parser.properties --jars /home/rajkumar_sukumar/log4j-core-2.11.2.jar,/home/rajkumar_sukumar/log4j-api-2.11.2.jar,/home/rajkumar_sukumar/config-1.3.2.jar /home/rajkumar_sukumar/thelogparser_2.11-0.1.jar dev






5.Explanation about the parameters used in spark-submit
 
Parameter	Description
--class	Conveying Spark to load and run this main class
--files	Passing our external config files
--config	Passing our config file to driver/executors
--jar	Including all our dependencies. We can achieve by using --package, but it is not advisable in PROD.
dev	The argument which Spark looks in the properties file and loads the configuration accordingly


How to run unit test ?

1.Please download the project from this location 
https://github.com/raj-sparkworks/spark-artifacts/blob/main/TheLogParser.7z
2.Extract and import to IDE like IntelliJ
3.Go to Terminal and run sbt test
4.The test output will be displayed in the console



Package the application to Docker


Following are the steps to package the application in Docker image

1.Build our Spark Scala application

2.Create a Dockerfile
a)Get the docker base image [like openjdk:alpine]
b)Curl to install Spark, SBT
c)Copy the dependencies to Docker
d)Copy the application jar to Docker

2.Build the Dockerfile
a)Goto the location where we have Dockerfile
b)Run the below command
c)docker build --rm=true -t nasa/spark-log-parser .
d)Image will be build and we will get the hashcode

3.Run the Image
a)docker run --name spark-log-parser-0514 -e ENABLE_INIT_DAEMON=false --link spark-master:spark-master -d nasa/spark-log-parser
b)We will get the sample output in console
