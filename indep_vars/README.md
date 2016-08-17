# Configure Independent Variables
pySpark ML analysis tools, such as GLM or OLS, require a particular formatting of the independent variables.  The tools they offer for this are powerful and flexible, but require the use of an excessive amount of obscure-looking code to accomplish what most social scientists will be used to achieving in one or two simple lines of Stata or SAS code.

The standard formatting requires two columns from a dataframe: a **dependent variable column** usually referred to as 'label', and an **independent variable column** usually referred to as 'features'.  The dependent variable is simply a column of numerical data; the column for the independent variables must be a *vector*.

This function, `build_indep_vars.py` is meant to help automate most of this task.  It takes in as arguments a pySpark dataframe, a list of column names for the independent variables, and an optional list of any independent variables that are categorical.  It then handles in the background getting the data in the proper format and expanding the categorical variable columns into multiple columns of dummy variables.  When completed it returns the original dataframe with a new column added to it named 'indep_vars' that contains the properly formatted vector.  

An example:

df = spark.read.csv('s3://ui-spark-data/diamonds.csv', inferSchema=True, header=True, sep=',')  
df = build_indep_vars(df, ['carat', 'clarity'], categorical_vars=['clarity'])  
df.show(5)  
  
+-----+---------+-----+-------+-----+-----+-----+----+----+----+--------------------+  
|carat|      cut|color|clarity|depth|table|price|   x|   y|   z|          indep_vars|  
+-----+---------+-----+-------+-----+-----+-----+----+----+----+--------------------+  
| 0.23|    Ideal|    E|    SI2| 61.5| 55.0|  326|3.95|3.98|2.43|(6,[0,1],[0.23,1.0])|  
| 0.21|  Premium|    E|    SI1| 59.8| 61.0|  326|3.89|3.84|2.31|(6,[0,2],[0.21,1.0])|  
| 0.23|     Good|    E|    VS1| 56.9| 65.0|  327|4.05|4.07|2.31|(6,[0,4],[0.23,1.0])|  
| 0.29|  Premium|    I|    VS2| 62.4| 58.0|  334| 4.2|4.23|2.63|(6,[0,2],[0.29,1.0])|  
| 0.31|     Good|    J|    SI2| 63.3| 58.0|  335|4.34|4.35|2.75|(6,[0,4],[0.31,1.0])|  
+-----+---------+-----+-------+-----+-----+-----+----+----+----+--------------------+  
only showing top 5 rows  
  
glr = GeneralizedLinearRegression(family='gaussian', link='identity', labelCol='price', featuresCol='indep_vars')  
model = glr.fit(df)  
transformed = model.transform(df)  