def build_indep_vars(df, independent_vars, categorical_vars=None, keep_intermediate=False):

    """
    Data verification
    df               : DataFrame
    independent_vars : List of column names
    categorical_vars : None or list of column names, e.g. ['col1', 'col2']
    """
    assert(type(df) is pyspark.sql.dataframe.DataFrame), 'pypark_glm: A pySpark dataframe is required as the first argument.'
    assert(type(independent_vars) is list), 'pyspark_glm: List of independent variable column names must be the third argument.'
    for iv in independent_vars:
        assert(type(iv) is str), 'pyspark_glm: Independent variables must be column name strings.'
        assert(iv in df.columns), 'pyspark_glm: Independent variable name is not a dataframe column.'
    if categorical_vars:
        for cv in categorical_vars:
            assert(type(cv) is str), 'pyspark_glm: Categorical variables must be column name strings.'
            assert(cv in df.columns), 'pyspark_glm: Categorical variable name is not a dataframe column.'
            assert(cv in independent_vars), 'pyspark_glm: Categorical variables must be independent variables.'

    """
    Code
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
    from pyspark.ml.regression import GeneralizedLinearRegression

    if categorical_vars:
        string_indexer = [StringIndexer(inputCol=x, 
                                        outputCol='{}_index'.format(x))
                          for x in categorical_vars]

        encoder        = [OneHotEncoder(dropLast=True, 
                                        inputCol ='{}_index' .format(x), 
                                        outputCol='{}_vector'.format(x))
                          for x in categorical_vars]

        independent_vars = ['{}_vector'.format(x) if x in categorical_vars else x for x in independent_vars]
    else:
        string_indexer, encoder = [], []

    assembler = VectorAssembler(inputCols=independent_vars, 
                                outputCol='indep_vars')
    pipeline  = Pipeline(stages=string_indexer+encoder+[assembler])
    model = pipeline.fit(df)
    final = model.transform(df)

    if not keep_intermediate:
        fcols = [c for c in final.columns if '_index' not in c[-6:] and '_vector' not in c[-7:]]
        final = final[fcols]

    return final