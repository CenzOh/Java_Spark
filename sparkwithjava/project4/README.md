# Project 4 Lessons 12 - 13

### Quick Troubleshoot
Again, please refer to outer most `README.md` for extra help. When creating new project named proejct 4 I ran into errors. I fixed this by doing Import Maven project and selecting project 4.<br>
Also when running the app, right click on Application.java (the file with the main function), Run configs > JAva Application > Explains how to make a new config. Click the button on the top left to create a new Run Config.<br>
Add the package and class as the path `com.jobreadyprogrammer.spark.Application.java`.<br>

### Reading the files
`com.jobreadyprogrammer.spark` main spark driver files <br>
`application.java` driver program, where main fcn is and where we run the java app<br>
`ArrayToDataset.java` first example we go over in lecture 12, looking at turning an array into a dataset and then how to turn DSs into DFs<br>
`ArrayToDatasetBreakdown.java` my version with my wwritten code and comments.<br>

### Lecture 12 Converting datasets and dataframes
The difference between dataframes and user defined data set is very small, but important to understnad.<br>
Using the dataset (DS) class with the row parameter is known as a dataframe (DF). `Dataset<Row>`. <br>
This works well for injesting STRUCTURED files such as JSON, XML, databse tables, CSVs, right into the DF.<br>
We can then run the fancy spark sql functions.<br>

Now, what if we wanted to use our Plain Old Java objects (POJO)? Our own classes? We dont want to create a POJO that is
Spark specific, would be bad design decision. What we would do is use same dataset class, but make parameter your user defined type.<br>
Ex - `Dataset<Car>`. Now take note, that this CAR dataset is NOT a dataframe!!! Making this distinction is important.<br>

Terminology | Code | Tungsten memory optimizations | Type safety
--- | --- | --- | --- 
Dataset | Dataset<Car> | No | Yes
Dataframe | Dataset<Row> | Yes | No

DF is more efficinet than user defined DS. DF uses query optimizer and uses tungsten, under the hood container. Increases speed of various operations.<br>
We won't get these optimizations using a user defined set.<br>
Ultimate goal - convert user defined DSs BACK to DFs for running various ops. We will see how.<br>
Now hop into `project 4`. Look at `Application.java` first under `com.jobreadyprogrammer.spark`<br>

### Lecture 13 Map Reduce Transformation Functions
Looking