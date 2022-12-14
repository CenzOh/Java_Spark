# Project 4 Lessons 12 - 15

### Quick Troubleshoot
Again, please refer to outer most `README.md` for extra help. When creating new project named proejct 4 I ran into errors. I fixed this by doing Import Maven project and selecting project 4.<br>
Also when running the app, right click on Application.java (the file with the main function), Run configs > JAva Application > Explains how to make a new config. Click the button on the top left to create a new Run Config.<br>
Add the package and class as the path `com.jobreadyprogrammer.spark.Application.java`.<br>

### Reading the files
`com.jobreadyprogrammer.spark` main spark driver files <br>
`application.java` driver program, where main fcn is and where we run the java app<br>
`ArrayToDataset.java` lesson 12, first example we go over in lecture 12, looking at turning an array into a dataset and then how to turn DSs into DFs<br>
`ArrayToDatasetBreakdown.java` my version with my wwritten code and comments.<br>
`CsvToDatasetHouseToDataframe.java` lecture 14, exactly what is says we are doing, but this is the instructor version.<br>
`CsvToDatasetHouseToDataframeBreakdown.java` my version / wrote my own code. <br>
`WordCount.java` used in lecture 15, instructor code.<br>
`WordCountBreakdown.java` my version of the code.<br>

`com.jobreadyprogrammer.pojos` package where we define our Plain Old Java Objects (POJO) we only do this for the house dataset
`House.java` lecture 14, instructor code. Just defining the variables for the house object reading the house CSV<br>
`HouseBreakdown.java` my version of the code<br>

`com.jobreadyprogrammer.mappers` package where we define mapping functions for two of our examples<br>
`HouseMapper.java` lecture 14, implementing mapping for the house csv example to break down the date variable<br>
`HouseMapperBreakdown.java` my code version<br>
`LineMapper.java` lecture 15, implementation for shakespeare txt file so we can break down lines of text / rows into single words per row.<br>
`LineMapperBreakdown.java` my code version<br>


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
Looking at `ArrayToDataset.java` still. We will talk about `map()` function. This fcn takes one element and produces an output of one element.<br>
Input A -> Map() -> Output 1<br>
Ex - map alphabets from A-z to number representation, we have a fcn that inputs letter and outputs the number.<br>
Howeverm the `reduce()` fcn is a bit differnt. It takes multiple inputs and produces one output.<br>
Inputs 2, 2, 2, 2 ----> reduce() --> output 8.<br>
Ex - ten items going into the reduce, outputs one item. <br>
Fundamental fcns from hadoop and in spark. Occurs under the hood / behind the scenes. Pretty simple stuff.<br>
Normally, in advancements of API of spark we dont have to think of it at that granualr of a level but still important to understand the concepts.<br>
Now lets hop into `ArrayToDataset.java`.

### Lecture 14, using datasets with user defined POJOs.
We are still working with project 4. This time open up `CsvToDatasetHouseToDataframe.java` file.<br>
We also create the `com.jobreadyprogrammer.pojos` package and `House.java` class.<br>
We create a SECOND package called `com.jobreadyprogrammer.mappers` and in it we make `HouseMapper.java` <br>
Also take note, typically when we work with data sets like suer defiend types, we want to convert that back to a DF before doing real processing<br>
Why? THis is because of this ting called `tungsten`. Memory management container in spark. Introduced in 2014 as project tungsten, focuses on improving efficiency of memory and CPU for spark apps<br>

### Lesson 15, using datasets with unstructured textual data
In this lesson, we will be looking at `wordCount.java` and the `shakespeare.txt` files.<br>
Our goal is to find most common word shakespeare uses. For us to do this, we can ingest the text file as lines of the literature per row.<br>
However to find occurences of the word itself, we need to break down even further. We will be using new fcn, `flatMap()`.<br>
flatmap takes ONE input and produces MANY outputs.<br>
1 Input ---> flatMap() ---> Outputs A, B, C, D, E, etc.
