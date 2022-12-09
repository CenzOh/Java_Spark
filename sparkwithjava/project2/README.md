# Project 2 Lecture 8
I created this with the following:

- Set directory as workspace to be `sparkwithjava`
- Next, create a new java project. Name is 'project2'. Exact name of this project.

Now, ensure we are using correect version of Java (SE 8).
- Right click project2
- Build path
- Configure build path
- Libraries
- JRE should be 1.8. If there is an issue, click on Edit
- Alternate JRE, Installed JREs, select 1.8.

### How to read the files
`src/main/java/com.jobreadyprogrammer.spark` includes all our java files that I would describe as the driver files<br>
`application.java` the main java, this calls our three methods that we define in the other files here. Forgot to separate no comment and all comment files. SO these files have tons of comments. May come back and create no comment files if readability is hard.<br> 
`inferCSVSchema.java` this first file is reading a CSV and letting spark infer the schema or structure of the CSV.<br>
`defineCSVSchema.java` in this file, we explicitly tell spark what the schema of the csv file is.<br>
`jsonLinesParser.java` since json files can have one row of data on multiple lines, we specify that and how JSON files are being read by spark<br>