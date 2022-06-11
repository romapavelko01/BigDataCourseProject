# BigDataCourseProject
**Authors:** ***Roman Pavelko***, ***Nazar Dobrovolskyi***

Hi!

Here is our  project for the Big Data course at UCU, where we solve the problem of analysis of streaming data
by using Cassandra and writing data into its tables, and further retrieving that data to answer number-specific questions, formed
as requests.

In the directory `DOCS` (as well as in the photo below) you can find the diagram of our Cassandra database and what tables we inserted data into, 
as well as more detailed explanation of their structure and purposes.

![DiagramImage](https://github.com/romapavelko01/BigDataCourseProject/blob/reading-part/DOCS/CassandraDiagram.png)


In order to run the code, you need to do that in the following order and way:

- Start the cassandra network and create tables in it by running `bash run-cassandra-cluster.sh`

- Start reading streaming data and writing it into Cassandra by running `bash write-cassandra.sh`

- Once finished with Cassandra, run `bash shutdown-cassandra.sh`

