# Project 5 Lessons 16-18

### Quick Troubleshoot
File > Import > Existing Maven Project > Project 5. THis way worked for me. Not normal import, and not create new project.<br>
Next, make sure everything is correct with run configs. Right click, run as, run configs. Edit the path and which project to look at<br>

### Reading the files
`src/main/java/com.jobreadyprogrammer.spark` package for our driver / main programs<br>
`Applciation.java` instructor version, main driver program, joining and filtering students and grades<br>
`ApplicationBreakdown.java` my version<br>
`CustomersAndProducts.java` instructor version, looking at joining toegether three dataframes<br>
`CustomersAndProductsBreakdown.java` my version<br>
`src/main/resources` package location of all csvs<br>
`grade_chart.csv` and `students.csv` both files used in app.java and lesson 16
`customers.csv`, `products.csv`, and `purchases.csv` used in customersAndProducts.java in lesson 17-18


### lecture 16 joining dataframes and using filter transformations
We will be looking at `grad_chart.csv` and `students.csv` and attempt to join both dataframes together.<br>
first look at both csvs. In grade chart, we can see how GPA translates to a letter grade.<br>
In student csv, we see their ID, their name, state they belong to, GPA, their favorite book title, and if they ar working or not<br>
Take note, that GPA column in this DF contains all numerical GPAs. If we want to figure out what letter grade they will get, join it with grade chart csv.<br>

### Lesson 17 aggregate transformation
After joining the three dataframes together, notice how we have a numeric col (with product price) and we have names repeating.<br>
So for every product that was puchased, we can see the product name and price they paid. Ex - smith bought 4 products so we can see he bought 2 kleenex and 2 ham<br>
How can we compute the amount he spent for each of the given products? Or compute total amount spent? <br>
We can use sum function similar to SQL. FInd avg? we have SQL aggregate fcn. So we can take joined DF and do groupby.<br>

### Lesson 18 More transformations
Ok so take note that joining and dropping columns are TRANSFORMATIONS. THe show method is an ACTION.<br>
To demonstrate this, we set the joinedData group by and aggreagate lin eto another df. We also include a breakpoint in the sysout message<br>
We can run as debug by right clicking > debug as > java app > select yes to switch to debug mode.<br>
Our code has reached a break point we have a different looking view. It logs up to this point and now stopped logging and is now pasued.<br>
Top left, click the jump arrow called `step over`. I t will step over and jump to the next line to execute<br>
When we run the spark code, it does NOT log anything but it did complete. We went through a series of transformations, and nothing was logged.<br>
The code did not run. Instead, a recpie occurred. A receipte containing instructions is generated<br>
The recpie it was being added to is the `DAG, Directed ACyclic Graph`.<br>
Its a graph that goes in one direction and CAN NOT be rewinded. So this forms a series of instructions exectuing in step by step<br>
When we hit aggDf.show(), more output and logging continues.<br>
If you read the log you can see theres a dag scheduler entry, unstaged, seven jobs, seven finished, more tasks ocurring. AFter 12 job,s it shows the table.<br>
This is known as lazy execution, spark is being lazy. But this is a good thing. Its because of a service called Catalyst.<br>
The catalyst looks at the list of instructions and figures out best way to run the isntructions. MAy skip a few steps and not execute to be more efficient.<br>
Think of something like breadthfirst search in a tree fashion. <br>
Example of what spark sees and does:<br>
NO EXECUTE - A -> B -> D -> E<br>
NO EXECUTE - A -> D -> E<br>
EXECUTE - A - C - E<br>
EXECUTE - A - E<br>
catalyst skips steps B and D. ANother example is, if we write aggDf.drop num of purcahses and most exp purchase BEFORE the show an after we set aggFy,<br>
The steps will be added to the instructions. Catalyst optimizer will look at this and will know to NOT computate the num of purcahses and most exp purchase since we said we will be dropping it later<br>
So it skips those steps. Save time. <br>
Ok another example is assign aggdf to another df lets say to initialDf. Then we do a loop and assign to aggDf the union of aggDf with initialDf.<br>
```
Dataset<Row> initialDf = aggDf;
for(int i = 0; i < 500; i++){
	aggDf = aggDf.union(initialDf);
}
```
This has to be executed 500 times and size of aggDf will icnrease 500x because we used union. Instead of doing show here what if we run it on joinedData?
`joinedData.show()`. Catalyst optimizer will know we dont ahv eto do ANYTHING with aggDf so it will skip that step too!<br>
Spark being lazy in this way is not bad, it skips unneccessary steps to optimize.<br>
So to recap, transformations are just instructions of how to change the DF while the action is the thing that performs the isntructions that have been added to the dag<br>.
if you hover over .show() you can see a group and its under actions. another example is .collect() which collects all the rows in this DF<br>
ANother action is .collectAsList() this collects all the elements in a list datastructure. Shift click and it will take you to the dataset.class<br>