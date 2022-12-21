# Project 8 Lessons 25-31

### Quick Troubleshoot
Import Maven project worked for me.

### Reading the files
`com.jobreadyprogrammer.spark` main package for the spark apps<br>
`...Breakdown.java` my version of the code <br>
`LinearMarketingVsSales.java` example of using linear regression to predict sales number given how much money was spent on marketing. We change up some values and add a bad day col to show how linear regression can have multiple features<br>
`LogisticRegressionExample.java` logistic regression example using cryotherapy example and finding binary output of whether or not treatement was succesful or not based on input<br>
`KMeansClustering.java` example of using k means clustering with data based on annual spending and dividing into a number of clusters or groups<br>

### lecture 25 machine learning resourcess
This section we wil learn how to use spark with ML (machine learning) algorithms on our data.<br>
Sparks datasets and dataframe api is standard to write ml apps. Older RDD APi is going to be deprecated.<br>
ML is a pretty vast topic but in this section we'll cover 3 popular ML algorutms.<br>
We'll be using the `Spark MLlib`. For a deeper dive into ML instructor reccommends:<br>
Book called `An intro to Statistical Learning` available for free with this link:<br>
https://www-bcf.usc.edu/~gareth.ISL.ISLR%20First%20Printing.pdf` <br>

### lecture 26 linear regression overview
What is linear regression? FUndamental tech to analyze relationship between two variables and predicitng outcome based on historical data<br>
A good example is to think of a 2D line char with sales as Y axis and advertising spend as x axis.
Sales	<br>
|...../<br>
|..../<br>
|.../<br>
|../<br>
L_/_____________<br>
Advertising Spent<br>

Thats kind how how you would want to look at it. So what does this all this mean? <br>
To break down the avdertising and sales example, there is a relationship between these two variables.<br>
The more money the company spends on advertising the higher their sales. THis is natural. Here is the data in tabular form:<br>
Advertising Spend (X) | Total Sales (Y)
--- | ---
30K | 280K
40K | 279K
40K | 220K
27K | 168K
50K | 250K
60K | 382K
70K | 450K
90K | 505K

You get the idea. Sales is DEPENDENT on amount spent on advertising. Of course there are other factors like customer retention, cpmpetitors, economic conditions.<br>
But we will keep this simple. Sales is dependent variable (Y) and advertising budgget is INDEPENDENT variable (X).<br>
We can sscale advertisign budget as high, sales will move up and down dependning on $ spent on advertising.<br>
If we graph the data, we would have a scatterplot. THe trend is generally positively correlated. <br>
We want to have the ability to gain future predictability. We would draw in a trend line that bests fit the data.<br>
A poorly drawn trend line will not be even or connect well with dataa points distributed between the lines.<br>
Best trend line, has LOWEST vertical distance between points. THis is called residuals.<br>

. - data point<br>
| - residual<br>
----- - trend line<br>

Minimuze residuals to find best fitted trend line. Many techniques to find it such as total sum of squares. Method of least squares.<br>
Math formulas to minimize distance. We wont go behind the mathematics. Instead, we can now use a tool like excel to figure out the trend line.<br>
We can use the model to predict sales based on any advertising amount.<br>
Lets say we predict how much would sales be if we had advertising budget of 150K? <br>
Based on our data, the trend line predicts our sales would be 800K!<br>
In excel, the trend continued, the line updated and the graph expanded. <br>
This is called simple linear regression using two variables. <br>
Lets change the values AGAIN, this time if we sepnt 1 M on advertising, the graph expands and predicts sales would be 5 M.<br>

### 27 spark java linear regression
refer to file `LinearMarketingVsSales.java` file<br>

### 28 overview of log regression
log reg does NOT fall in cateogry of regression algorithms, it is known as a classification algorithm <br>
Examples of classification problems:<br>
Prediction of whether an email is spam or NOT spam based on contents of that email (think true false)<br>
PRedict if student will be accepted at university or rejected based on entrance exam score<br>
Someone is obese or not based on weight.<br>

You get the idea. FOr summary: Classification predicts yes or no. Regression predicts specific numerical value.<br>
Log reg has dependent variable Y, takes on binary value of 1 or 0 (true or false)<br>
The outcome of 1 or 0  is determiend based on probability percentage.<br>
Ex, for the student getting accepted to university, if probability is 0.7, the log regressio would qualify as 1 since 0.7 is closer to 1 than 0<br>
If it was 30 percent (0.3), would be predicted outcome as 0 since 0.3 is clsoer to 0 than 1.<br>
If graphed, the curve will NOT be straightline like linear regression. <br>
THis is because the quation used to graph a log reg is different than one used to graph linear reg.<br>

Y axis student passing test<br>
|.........---------<br>
|......../<br>
--------/------------- - below line, fail, above line, pass<br>
|....../<br>
|...../<br>
__---/________________ <br>
X axis hours studying<br>

Idea is that this is NOT LINEAR. More so exponential, so when you get close to the top, will not greatly increase even more. Peaks at some point<br>

### 29 spark logistic regression
refer to file `LogisticRegressionExample.java`

### lecture 30 overview of k means clustering
overview of k means cluster regarding spark. <br>
k means clustering aims to partition N number of observation into k clusters. bunchf of data points.<br>
k = 1 means one group of data. If we change k to 3, how do we change it into three clusters? <br>
We can eye ball it and group based on which looks close, areas with lots of whitespace are in diff group.<br>
For computer, this is hard! <br>
Understand domain, example shows 4 clusters but makes more sense to make it 3 clusters. We can choose our k value<br>

In another example, we will split into 2 clusters. Two points will be randomly placed on graph for two clusters<br>
Lets say identify as blue or red. Refered to as centroids. Algorithm identifies all observations CLOSEST to centroids.<br>
Could split in a diagonal fashion. Then algorithm moves centroid to the center point within cluster. Take all observations that fall in the cluster, and get the average.<br>
Moving towards gravity of that cluster. Then create new boundary and find out which observations are closest to the cenroid now? <br>
Maybe the cluster gets divided a bit more horizontally! Then we can get average AGAIn and move centorid to that location again and recompute<br>
Even better divsion. Algorithm ends when centroid is cluosest to all nodes in clsuter. <br>

If k = 3, it would be a similar computation. Random place centroid, identify observations close to it. GO down iteration and move centroid into average position<br>
Figure out average of the cluster to the point where it is at the perfect position. <br>
within sum of squared errors. <br>
Unsupervised learnign algorithm, we are NOT given a label column. Just features. cluster observations into groups with most common features.<br>
Datapoints with similar features are clustered together.<br>

### lecture 31 spark java k means clustering
refer to `KmeansClustering.java`.