import functools, os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pprint
from IPython.display import display, Markdown
import pandas as pd
#spark = SparkSession.builder.getOrCreate()
#sc = SQLContext(spark)

class Preprocessor:
    def __init__(self, DFS=None, JOINS=None):
        self.DFS = DFS
        self.JOINS = JOINS
        
    def readAndCombineData(self):
        spark = SparkSession \
        .builder \
        .appName("MAP-21") \
        .getOrCreate()
        sc = SQLContext(spark)
        # Set working directory to one folder up
        os.chdir("../")
        PWD, DIRS = os.getcwd(), "/Data/Inputs/"
        DIR = PWD + DIRS
        
        # Don't forget about this df
        truckpct = spark.read.csv(DIR+'truck_pct.csv', header=True)

        # create dict of directories that will
        # hold dataframes that share common identifiers
        self.DFS = {
            'TMC/': [], 
            'TMC_YEAR/': [], 
            'TMC_YEAR_PERIOD/':[]
        }

        # Get full path to the inputs and use a formatted
        # string to get the three other directories in a loop
        DIR += "{}"

        # The column names that each directory will join on
        # resulting in one table in each dir
        self.JOINS = [["TMC"], ["TMC", "YEAR"], ["TMC", "YEAR", "PERIOD"]]

        '''
        1. Outer loop through each directory
        2. Inner loop through the files in each 
        dir and read it into a spark dataframe
        3. Append the dataframe to the values list
        within its respective directory (key)
        4. Get out of the inner loop and pop the
        first data frame out of this list and
        save it. This will be the starting df
        to perform the joins.
        5. Create another inner loop that will
        sequentially join the datasets together
        based on their shared identifier.
        6. Remove any duplicate columns that may have occurred.
        '''
        j = 0
        for i in self.DFS:
            display(Markdown(f"### Listing All Tables in the {i.rstrip('/')} Directory"))
            print("Reading in the following files:")
            for file in os.listdir(DIR.format(i)):
                print(file)
                df = spark.read.csv(DIR.format(i)+file, header=True)
                self.DFS[i].append(df)
            print("\nSchema of the dataframes within the list:")
            pprint.pprint(self.DFS[i])
            display(Markdown(f"### Joining All Tables by the {i.rstrip('/')} Identifier"))
            start_df = self.DFS[i].pop()
            for df in self.DFS[i]:
                temp = start_df
                joined_df = temp.join(df, on=self.JOINS[j])
                temp = joined_df
                start_df = temp
            self.DFS[i].append(start_df)
            self.DFS[i] = self.DFS[i][-1]
            display(Markdown(f"#### The dataset has {len(self.DFS[i].columns)} columns and {self.DFS[i].count()} rows"))
            cols_new = [] 
            seen = set()
            for c in self.DFS[i].columns:
                cols_new.append('{}_dup'.format(c) if c in seen else c)
                seen.add(c)
            self.DFS[i] = self.DFS[i].toDF(*cols_new).select(*[c for c in cols_new if not c.endswith('_dup')])
            pprint.pprint(self.DFS[i])
            display(Markdown("---"))
            j+=1

        display(Markdown(f"### Combing a Final Table"))
        all_tmc_year = self.DFS['TMC_YEAR/'].join(self.DFS['TMC/'], self.JOINS[0])
        all_tmc_year_period = self.DFS['TMC_YEAR_PERIOD/'].join(all_tmc_year, self.JOINS[1], how="outer")
        all_data = all_tmc_year_period.join(truckpct, ['TMC', 'PERIOD'])
        display(Markdown(f"#### The dataset has {len(all_data.columns)} columns and {all_data.count()} rows"))
        display(Markdown(f"---"))
        
        display(Markdown(f"### Separating the actual and trainable data from the forecasted data"))
        trainableData = all_data.filter(all_data.YEAR < 2021)
        forcastedData = all_data.filter(all_data.YEAR > 2020)
        trainableData.show(n=10)
        DIR = PWD + DIRS
        targetData = spark.read.csv(DIR+'NON_FORECASTED/rel_unrel.csv', header=True)
        trainableData = trainableData.join(targetData, ["TMC", "YEAR"])
        dropCols = ('obs AMP rel_unrel', 'obs MIDD rel_unrel', 'obs PMP rel_unrel', 'obs WE rel_unrel', 'obs rel_unrel WD')
        trainableData = trainableData.drop(*dropCols)
        trainableData.show(10)
        display(Markdown(f"#### The trainable dataset has {len(trainableData.columns)} columns and {trainableData.count()} rows"))
        display(Markdown(f"---"))
        train = trainableData.toPandas()
        forecast = forcastedData.toPandas()
        train.to_csv("Data/Final_Data/trainableData.csv")
        forecast.to_csv("Data/Final_Data/forecastedData.csv")
        spark.stop()
        return trainableData, forcastedData

    def splitData(self, trainableDataSpark):
        train, test = trainableDataSpark.randomSplit(weights=[0.9, 0.1], seed=314)
        return train, test