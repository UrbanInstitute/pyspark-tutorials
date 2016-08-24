"""
Program written by Jeff Levy (jlevy@urban.org) for the Urban Institute, last revised 8/24/2016.
Note that this is intended as a temporary work-around until pySpark improves its ML package.

Tested in pySpark 2.0.

"""

def build_indep_vars(df, independent_vars, categorical_vars=None, keep_intermediate=False, summarizer=True):

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
    df = model.transform(df)

    #for building the crosswalk between indicies and column names
    if summarizer:
        param_crosswalk = {}

        i = 0
        for x in independent_vars:
            if '_vector' in x[-7:]:
                xrs = x.rstrip('_vector')
                dst = df[[xrs, '{}_index'.format(xrs)]].distinct().collect()

                for row in dst:
                    param_crosswalk[int(row['{}_index'.format(xrs)]+i)] = row[xrs]
                maxind = max(param_crosswalk.keys())
                del param_crosswalk[maxind] #for droplast
                i += len(dst)
            elif '_index' in x[:-6]:
                pass
            else:
                param_crosswalk[i] = x
                i += 1
        """
        {0: 'carat',
         1: u'SI1',
         2: u'VS2',
         3: u'SI2',
         4: u'VS1',
         5: u'VVS2',
         6: u'VVS1',
         7: u'IF'}
        """
        make_summary = Summarizer(param_crosswalk)


    if not keep_intermediate:
        fcols = [c for c in df.columns if '_index' not in c[-6:] and '_vector' not in c[-7:]]
        df = df[fcols]

    if summarizer:
        return df, make_summary
    else:
        return df

class Summarizer(object):
    def __init__(self, param_crosswalk):
        self.param_crosswalk = param_crosswalk
        self.precision = 4
        self.screen_width = 57
        self.hsep = '-'
        self.vsep = '|'

    def summarize(self, model, show=True, return_str=False):
        coefs = list(model.coefficients)
        inter = model.intercept
        tstat = model.summary.tValues
        stder = model.summary.coefficientStandardErrors
        pvals = model.summary.pValues

        #if model includes an intercept:
        if len(coefs) == len(tstat)-1:
            coefs.insert(0, inter)
            x = {0:'intercept'}
            for k, v in self.param_crosswalk.items():
                x[k+1] = v
        else:
            x = self.param_crosswalk

        assert(len(coefs) == len(tstat) == len(stder) == len(pvals))

        p = self.precision
        h = self.hsep
        v = self.vsep
        w = self.screen_width

        coefs = [str(round(e, p)).center(10) for e in coefs]
        tstat = [str(round(e, p)).center(10) for e in tstat]
        stder = [str(round(e, p)).center(10) for e in stder]
        pvals = [str(round(e, p)).center(10) for e in pvals]

        lines = ''
        for i in range(len(coefs)):
            lines += str(x[i]).rjust(15) + v + coefs[i] + stder[i] + tstat[i] + pvals[i] + '\n'

        labels = ''.rjust(15) + v + 'Coef'.center(10) + 'Std Err'.center(10) + 'T Stat'.center(10) + 'P Val'.center(10)
        pad    = ''.rjust(15) + v

        output = """{hline}\n{labels}\n{hline}\n{lines}{hline}""".format(
                    hline=h*w, 
                    labels=labels,
                    lines=lines)
        if show:
            print(output)
        if return_str:
            return output