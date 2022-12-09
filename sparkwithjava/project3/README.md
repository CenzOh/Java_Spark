# Project 3 Lecture 9, 10, 11

### Lecture 9, reduce log entries inthe console.
Good idea to follow the log entries at beginning of learning. If it becomes too much, disable it with the following:
Imports:
```
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
```
Methods:
```
Logger.getLogger("org").setLevel(Level.ERROR);
Logger.getLogger("akka").setLevel(Level.ERROR);
```
Now we will only see error messages and console prints and ignores all the INFO messages.

NOTE:
When I first created a new project and renamed it 'project3' I recieved an error.
FOudn out the issue is that `Maven Dependencies` was not imported. 
Even the `.classpath` file had nothing in it. 
Easiest workaround is the following:
- Delete project3 in the package explorer so we can import project3 instead.
- File > Import > Maven Project.
- Select the file path for project3.
- Finish importing, now we should have JRE auto configged to SE 1.8 (If not, refer to Project 1 and 2s README) and Maven dependencies should be here

### Idea of Project3
We will go over a real world scenario of combining two DIFFERENTLY formatted data.
Diff file format and schema definition.
We will parse and pick certain fields to consist of combined data with new schema definition.
Think SQL Union operation. One data stack on top of another to create one combined table. 
We are using realworld public available data of city parks in USA.

Durham city data: `https://opendurham.nc.gov/explore/dataset/city-parks/`
Lots of other data sets in this link as well!!
Philly link: `https://wwww.opendataphilly.org/dataset/parks-and-recreation-assets/`

Both files contain a bunch of parks. The Durham one is a JSON structure. Dataset ID, record id. Fields contains many other fields within it. 
If need to access the address or zip code or if it has tennis court, we have to acces them through fields. Fields.zip for example.
Geo shape filed contains coordinates of various points in the park. This is one field we will not need.

Next up, Philly one is a CSV. Can look at in excel. Col names are super differnt than the Durham file. 
This is Philly recreations, each record is not necessarily a park. THe ones that are literally say park.
We will be filtering for records that only contain parks. Combine this with the JSON document. 

### HOW TO READ files
`src/main/java/com.jobreadyprogrammer.spark` where all these driver files are located.<br>
`application.java` this file was given to me with all code written. Theres so much to cover that I created a second file just for comments like in project1<br>
`application2.java` this is the java file where I code the solution myself / follow tutorial and provide my comments and outptu<br>
`applicationTest.java` didnt get up to it yet WIP<br>
`src/main/resources` location of our files that we will be reading<br>
`durham-parks.json` JSON file that has parks within Durham North Carolina<br>
`philadelphia_recreations.csv` similarly, CSV file of parks in Philadelphia Pennsylvania <br>