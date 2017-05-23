# Configure Independent Variables
pySpark ML analysis tools, such as GLM or OLS, require a particular formatting of the independent variables.  The tools they offer for this are powerful and flexible, but require the use of an excessive amount of obscure-looking code to accomplish what most social scientists will be used to achieving in one or two simple lines of Stata or SAS code.

The standard formatting requires two columns from a dataframe: a **dependent variable column** usually referred to as 'label', and an **independent variable column** usually referred to as 'features'.  The dependent variable is simply a column of numerical data; the column for the independent variables must be a *vector*.

This function, `build_indep_vars.py` is meant to help automate most of this task.  It takes in as arguments a pySpark dataframe, a list of column names for the independent variables, and an optional list of any independent variables that are categorical.  It then handles in the background getting the data in the proper format and expanding the categorical variable columns into multiple columns of dummy variables.  When completed it returns the original dataframe with a new column added to it named 'indep_vars' that contains the properly formatted vector.  

An example:

`df = spark.read.csv('s3://ui-spark-social-science-public/data/diamonds.csv', inferSchema=True, header=True, sep=',')`  
`df = build_indep_vars(df, ['carat', 'clarity'], categorical_vars=['clarity'])`  

`glr = GeneralizedLinearRegression(family='gaussian', link='identity', labelCol='price', featuresCol='indep_vars')`  
  
`model = glr.fit(df)`  
`transformed = model.transform(df)`  