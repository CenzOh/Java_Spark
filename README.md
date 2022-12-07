# Udemy Java Spark course:
https://cglearning.udemy.com/course/apache-spark-for-java-developers/learn/lecture/12297260#overview

### Technologies Used:

- Java 1.8.0.123
- Apache Spark 2
- Maven 3
- Eclipse IDE

### Reading the files
`learningspark` first java spark project example
`sparkwithjava` all the projects we will go through in the course

### Get Started

First create a folder for this course. I did the following:<br>
`C:\users\cenzo\JavaSpark\`<br>
Next, open up Eclipse. 
Will be asked to create a directory, make sure to create the directory in a `workspace` folder.<br>
Path should be: `JavaSpark\workspace\`<br>
Click ok.<br>

Empty workspace, click New > Other > Maven > Mave Project<br>
Click checkbox next to use Default Workstation Location as well as Create Simple Project.<br>
Define the Maven project. Group ID is org name. The example uses `com.jobreadyprogrammer`<br>
Artifact ID, name of project. Example is `learningspark`.<br>
Click FInish.<br>

### Possible Errors
For the longest time, I have been running into issues when creating the Maven project.<br>
The error seemed to be I had a proxy enabled when working on this project on a separate system.<br>
On my personal system, I simply had to download Maven, update the Path variable to point to Maven and all ran well.<br>

Assuming everything was created successfully, we may proceed.<br>
Take note of `pom.xml` file. This is the key to Maven! We will not dive deep in Maven, but it is still vital.<br>
What does pom.xml do? All programming libraries we will use spark app will be defined here!<br>
Update this pom with this exact pom:<br>
`https://github.com/imtiazahmad007/sparkwithjava/blob/master/pom.xml`
After copy and paste, save the pom file.<br>
You will notice a new section was created, `Maven Dependencies`. This has all of our libraries we will use.<br>
IF YOU DO NOT SEE, refresh the workspace. Press `F51 or right click the project, `learningspark`, and select refresh.<br>
This import has the version of Spark we will use (and was defined in the pom under properties).<br>
We do not have to download Spark on our own, the Maven dependencies have done the hardwork for us.

Notice that the `JRE System Library` may say 1.5. We have 1.8, tell Eclipse to recognize 1.8.<br>
Right click `learningspark` > Build Path > Config Learning Path > Libraries > Select drop down menu and choose Java SE 1.8<br>
Apply and close, save. JRE should now say JavaSE 1.8.

Another note is that all the source code is on the following GitHub rep:<br>
`https://github.com/imtiazahmad007/sparkwithjava` <br>
Take this project and unzip into a folder called 'sparkwithjava'.<br>
Put this inside the workspace folder. So directory is `workspace/sparkwithjava` I also have `learningspark` folder under workspace<br>

Open up Eclipse, select the workspace to be `workspace/sparkwithjava`
Next, in package explorer > right click > Import (or File > Import) > Maven > Existing Maven Project<br>
Now select the root directory location. Select via browse and we can select with project folder to import.<br>
For the first example, import project 1 > This should recognize the `Pom.xml` file > Click FInish.<br>
After it imports, we can see the project on the top left and expand it!<br>

Note, we have to do this Maven Import for every project we want to import. Ensure it recognizes the Pom and correctly imports.