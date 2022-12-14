# Project 5 Lessons 16

### Quick Troubleshoot
File > Import > Existing Maven Project > Project 5. THis way worked for me. Not normal import, and not create new project.<br>
Next, make sure everything is correct with run configs. Right click, run as, run configs. Edit the path and which project to look at<br>

### Reading the files
`src/main/java/com.jobreadyprogrammer.spark` package for our driver / main programs<br>
`Applciation.java` instructor version, main driver program
`ApplicationBreakdown.java` my version


### lecture 16 joining dataframes and using filter transformations
We will be looking at `grad_chart.csv` and `students.csv` and attempt to join both dataframes together.<br>
first look at both csvs. In grade chart, we can see how GPA translates to a letter grade.<br>
In student csv, we see their ID, their name, state they belong to, GPA, their favorite book title, and if they ar working or not<br>
Take note, that GPA column in this DF contains all numerical GPAs. If we want to figure out what letter grade they will get, join it with grade chart csv.<br>

