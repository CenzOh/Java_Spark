# Project 6 Lessons 19-20

### Quick Troubleshoot
Import Maven project worked for me.

### Reading the files
`com.jobreadyprogrammer.spark` main package for our spark apps<br>
`Application.java` instructor written app. We go through ingesting reddit comments and find what is the most common word by filtering out common words<br>
`ApplicationBreakdown.java` my version of the code. I wrote a bunch of comments here per usual.<br>
`WordUtils.java` file that contains massive array of boring or stop words we dont want to see when we find most common word in the reddit comments file.<br>
`RC_2011-12.zst` JSON file with all the reddit comments for that particualr year and month. It was not working on my part for some reason, including the file here anyway.<br>

### lecture 19 Using spark to analyze reddit comments
This sounds super random, but we will be working with more practical real world data. You can grab the data at:<br>
`files.pushshift.io/reddit/`<br>
Clicking on comments tab, there is a lot of comment data we can grab. Goes all the way back to 2005.<br>
We will not be working with the file sizes of something like 20 gbs. WE will go with one that is bigger than what we worked it but not too big.<br>
We will take a file from 2007, we can pick any of the months doesnt really matter. I picked deccembers comments.<br>
The file is called `RC_2007-12.zst`. We can also look at another one from 2011. I picked `RC_2011-12.zsr`, another december comments.<br>

To create the jar file for this project. Right click project > maven clean > rigth click again > maven install > jar file gets created in the target folder.<br>

### lecture 20 running reddit spark app on EMR cluster
EMR - Elastic Map reduce. On Amazon Web Services (AWS) website. AWS not free, will document details instead of actually doing it.<br>
After creting an account > login > on homepage > Services > we will go to the S3 service<br>
Create a bucket > `spark-course-bucket`> click next > leave everything to default > create bucket.<br>
This is a distributed folder, it will be run on multiple servers. The cluster will have access in a distributeed manner, this i sknown as S3.<br>
Click the bucket > upload > select the reddit comments files > next and leave defaults > create.<br>

After successful upload, we can see the file is 3.6 GBs. Upload one more thing, the jar file from last lecture. It is located in target folder<br>
It should be called `project6-0.0.1-SNAPSHOT.jar`. Once you find it, to get exact location, right click > propertiies > shows location<br>
Upload on AWS > select the jar file. Its a small file. Remember to configure the redditFile URL to point the bucket and file NOT local direcotry and REMOVE setting the master to local.<br>
Next, click on services in AWS > click on EMR under analytics > we are brought to clusters. <br>
Activate a cluster with create cluster > name is `sparkCourseCluster` > you can see where the S3 folder will be located, it will be in a log folder.<br>
Software config, defualt is latest emr release > pick config enviornment for Spark, it contians hadoop, yarn, and ganglia, and zeppelin. <br>
Now we can shoose instance type. M3.xlarge is general option. Bigger it gets, more expensive. Can leave it at default. <br>
Num of isntances, 3 by defaault. 1 is master, 2 is core or slave nodes. <br>
Security access, choose a key pair, so we must crate a new one. CLick on the link that tells you steps on how to do it<br>
Save key locally on machine. After we create our key, the cluster will start and have the nodes provisioned <br>

Once finish, server will be up and running. Status changes from starting to waiting. Can take about 10-15 minutes worst case.<br>
Master and slaves should also be running. COnnect to it now > click on SSH > command is given to paste in Linux terminal.<br>
But first > click on security group > select checkbox for master group > specify rules to connect to cluster > click edit > bottom, add rule.<br>
> change the drop down to say SSH > select my IP option so it can connect. You can change it to anywhere so it has access to the key created.<br>
Only thing is this option for anywhere is NOT recommended in production. Now copy the command from the SSH link.<br>
Open up your terminal > go to your spark course directory > paste command > change where coursekey is, if its in the spakr course directory, just leave it as -coursekeypem<br>
After you hit enter you should see you enter the master node of EMR cluster.<br>

Copy over jar file from S3 bucket > go to S3 bucket on AWS > click bucket name > click jar file > copy part of the link so we can see where it is located `spark-course-bucket/project6.0.0.1-SNAPSHOT.jar`<br>
Type in in the terminal `aws s3 cp s3://spark-course-bucket/project6-0.0.1-SNAPSHOT.jar .` the . means copy to the currnet directory<br>
run this with `spark-submit project6-0.0.1-SNAPSHOT.jar`<br>
Now, if we timed this, you can see that running on cluster takes 3 and a half minutes.<br>
Part of result, ill include most interesting words:
```
+-------+-----+
|  value|count|
+-------+-----+
|looking|82000|
|   post|79823|
|  found|71015|
|talking|70072|
|  looks|67604|
+-------+-----+
```
Running the same program on a larger cluster with 6 instances will show that the time improves so this time it takes less than 2 minutes<br>
# Terminate the cluster when you are done with it
So in AWS EMR part of site, when finished with the cluster MAKE SURE TO **TERMINATE** IT!! PLEASE

Now lets do an analysis of which is better. Run on larger instance type? Coss more money, short time. Use more workers? Multiplies price per hour so also expensive but also faster. Lets see in table form:<br>

Instance type | memory and CPU | number of nodes | time taken to complete job | EMR price per hour |
--- | --- | --- | --- | ---
m3.xlarge | 15 gb, 4vCPU | 2 workers, 1 master | 3 min 20 sec | 0.07 * 3 = *$0.21/hr*
m3.xlarge | 15 gb, 4vCPU | 5 workers, 1 master | 1 min 53 sec | 0.07 * 6 = *0.42/hr*
m3.2xlarge | 30 gb, 8vCPU | 2 workers, 1 master | 2 min 23 sec | 0.14 * 3 = *$0.42/hr*
m3.2xlarge | 30 gb, 8vCPU | 5 workers, 1 master | 1 min 30 sec | 0.14 * 6 = *$0.84/hr*

We can run that cluster per hour and it costs 21 cents for the version we did. Can run as many jobs as we want in that hour and still only costs 21 cents.<br>
Note that using m3.2xlarge doesnt really save that much time. SO we should push our limits with m3.xlarge first. Utilize as many nodes for our job as possible. Cheaper too than using more expensive nodes.<br>

### Lecture 21, instructions to config spark stand alone cluster.
We can run spark in our own stand alone cluster.<br>
Instructions:<br>
https://spark.apache.org/downloads.html`<br>
Standalone cluster comes bundled in with spark, Easy to config and use. Was built and optimized specifically with spark. No extra funcs with unnecessaru options.<br>
Spark standlone cluster is simple and fast! Start a spark standaline cluster BEFORE submitting app to spark shell. Didnt need to do this since java maven libraries distribution started a local spark cluster every time we ran our app in eclipse<br>
Master connection URL: `spark://master_hostname:port` this is the idea. We can also specify multiple address: `spark://master1_hostname:port1, master2_hostname:port2`<br>
Startup scripts most convenient way to start spark. Proper enviornment and load spark default config. For scripts to run correctly. Spark should be installed at same location on ALL NODES on teh cluster!<br>
Spark provides 3 scripts, we can find them in `SPARK_HOME/bin` directory:<br>
- start-master.sh starts master process
- start-slaves.sh starts all defined worker processes
- start-all.sh starts both master and worker processes

Counterpart scripts for stopping processes are `stop-master.sh`, `stop-slaves.sh`, and `stop-all.sh`. Dont deploy spark in widnwos prod, for prod use LINUX ONLY. That is what spark is meant for.<br>