Wiki Page Ranking Algorithm


Project Report


I. Steps Taken to Develop the code

a) Configuring Hadoop in Local System

b) Creating Hadoop Plugin for Eclipse

c) Commenced coding each mapreduce job in the local hadoop environment.

d) Used XMLInputFormat for parsing XML file as suggested in class. 

e) Completed Map and Reduce Tasks for generating outlink graph.

f) Completed Map and Reduce Tasks for calculating the total number of pages.

g) Initialized the page Rank of every page = 1/N using another Map Reduce Job

h) Calculated page rank for 8 iterations using the formula specified in the Project PDF 

i) The outputs of all the reduce steps of the jobs specified in the previous step is written to a directory tmp/ in the same bucket.

j) After the last job is completed, the required output files are aggregated and moved to the results directory in the same bucket.rate


II. Difficulties Faced during the project

a) We coded the job initially using the older API version org.apache.hadoop.mapred.* As a result of which we were not able to chain jobs sequentially. 

Solution: We migrated the entire job to the newer API version org.apache.hadoop.mapreduce.*. The job.waitForCompletion method of the newer API solved this problem for us.

b) For the job mentioned in Step g),h) and i) we had to use the data in the output of Step f). We set a static variable that incremented itself whenever the reduce function of Step f) was called. This approach did not work. We understood that files would solve the problem at hand but the file kept on throwing an exception indicating us to use the following overloaded static method of the class 

Filesystem.get(URI uri,Configuration conf).

After implementhing this we used java.util.Scanner to read the contents of the file. This did not work and after doing some research we came to know that java.io.BufferedReader is the right way forward. When implemented in the mapper function this would not work. 

Solution: The entire code mentioned above had to be written in the main part of the program just before the Step g). An integer value was obtained and this integer value was set as a configuration variable using 

Configuration conf=new Configuration();
conf.setInt(String variableName,int variableValue)

In the Mapper function, this configuration variable was retrieved by

Context context;
context.getConfiguration.getInt(variableNme,default);


III. Learnings

a) Setting up Hadoop in Local System

b) Copying files to HDFS from local system

c) Writing a Map Reduce Job in local system

d) Difference between a local Hadoop map reduce job and EMR Job in Awazon AWS.

e) The different errors and exceptions that were handled.
