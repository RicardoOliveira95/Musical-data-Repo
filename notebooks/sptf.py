import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,col
from pyspark.sql.types import IntegerType
from typing import cast
import unicodedata
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from pyspark.ml.feature import Imputer,VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import FloatType,IntegerType

#Init spark
spark=SparkSession.builder.appName('Test').getOrCreate()

# File location and type
file_location = "/FileStore/tables/Spotify_df.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

#The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)

df_pyspark=spark.read.option('header',True).csv(file_location,inferSchema=True)
#df_pyspark.printSchema();
#display(df_pyspark)

#colunas=list(df_pyspark.schema.names)
#for coluna in colunas:
    #if(str(coluna).startswith("track")):
        #final_df=df_pyspark.withColumnRenamed(str(coluna),str(coluna).replace("track","song"))  

final_df=df_pyspark.withColumnRenamed('track_name','Name')
final_df=final_df.withColumnRenamed('track_id','ID')
final_df=final_df.withColumnRenamed('track_artist','Artist')
final_df=final_df.withColumnRenamed('track_album_name','Album')
final_df=final_df.withColumnRenamed('track_album_release_date','Album_release_date')
final_df=final_df.withColumnRenamed('track_popularity','popularity')

final_df=final_df.na.fill("Missing value")
#final_df.printSchema()
#display(final_df)

#order by date
final_pds=final_df.toPandas()
#text=final_pds.at[3,"lyrics"]

# Convert to unicode
#text_to_unicode = str(text, "utf-8")           

# Convert back to ascii
#text_fixed = unicodedata.normalize('NFKD',text_to_unicode).encode('ascii','ignore')

final_vals_pds=final_pds[["danceability","speechiness","energy","mode","acousticness","key","instrumentalness","liveness","valence","tempo","duration_ms","popularity"]]
final_dec_vals_pds=final_vals_pds[["danceability","speechiness","energy","acousticness","instrumentalness","liveness","valence"]]
count=0
final_track_pds=final_pds[["ID","Name","Artist","Album_release_date","popularity","lyrics"]]
top5=final_pds["Artist"].value_counts().head()
print("TOP 5 Artistas")
print(top5)
#top5=final_pds["Album"].value_counts().head()
#print(top5)
print(final_pds["language"].value_counts().head())

#final_pds.info()
#TEST FOR DUPLICATED VALUES
for bool in final_pds.duplicated():
    if bool:
        print("DUPLICATED VALUE")

#################Funcoes auxiliares###################
def is_num(s):
    try:
        float(s)
    except ValueError:
        return False
    else:
        return True

# initializing bad_chars_list
bad_chars =[';',':','!','*','ë','ª','¨','ë','¥','´','ë','©','´','¯','¿','ê','³','ì','‹','¶','ì','','€','Œ','¡','œ','¤','°','”','°','†','¹','—','¬','¼']
 
# using filter() to
# remove bad_chars
def translate(text):
    text = ''.join((filter(lambda i: i not in bad_chars, text)))
    return text

###################################################################
#print(translate(text))
cols=list(final_vals_pds)
pop=[]
pop1=[]
pop2=[]
keys=[]
tempos=[]
dur=[]

for ind in final_vals_pds.index:
    for col in cols:
        #WHITESPACES
        final_vals_pds.at[ind,col].replace(" ","")
        final_vals_pds.at[ind,col]=final_vals_pds.at[ind,col].replace(",",".")
        #final_vals_pds.at[i,j]=final_vals_pds.at[i,j].replace(".",",")
        if(not is_num(final_vals_pds.at[ind,col])):
            final_vals_pds.at[ind,col]="0"  #DEFAULT NO_VALUE

#display(final_vals_pds)
        
validModes=0
totalModes=len(final_vals_pds)
totalDuration=0
validDurations=0
for col in final_vals_pds:
    ind=0
    if(col=="mode"):
        colSerieM=final_pds[col]
        for cell in colSerieM:
            if(cell=="0" or cell=="1"):
                validModes+=1
    if(col=="duration_ms"):
        colSerieD=final_pds[col]
        for cell in colSerieD:
            if(cell.isdigit()):
                totalDuration+=int(cell)
                validDurations+=1
                if(final_vals_pds.loc[ind,'popularity'].isdigit()):
                    dur.append(int(cell))
                    pop.append(int(final_vals_pds.loc[ind,'popularity']))
            ind+=1
    if(col=="key"):
        colSerieD=final_pds[col]
        for cell in colSerieD:
            if(cell.isdigit()and int(cell)>0 and int(cell)<10):
                totalDuration+=int(cell)
                validDurations+=1
                if(final_vals_pds.loc[ind,'popularity'].isdigit()):
                    keys.append(int(cell))
                    pop1.append(int(final_vals_pds.loc[ind,'popularity']))
            ind+=1
    if(col=="tempo"):
        colSerieD=final_pds[col]
        for cell in colSerieD:
            if(cell.isdigit()and int(cell)>0 and int(cell)<300):
                totalDuration+=int(cell)
                validDurations+=1
                if(final_vals_pds.loc[ind,'popularity'].isdigit() and int(final_vals_pds.loc[ind,'popularity'])>0):
                    tempos.append(int(cell))
                    pop2.append(int(final_vals_pds.loc[ind,'popularity']))
            ind+=1

time=totalDuration/validDurations
secs=int((time/1000)%60)
mins=int((time/(1000*60))%60)
perc=validModes/totalModes*100

#print(final_vals_pds)
#print("Average duration (ms): ",time)
#print("Average duration (mins): ",mins,":",secs)
#print("Percentage  of valid modes: ",perc)
final_vals_pds=final_vals_pds[["danceability","energy","popularity"]]

sparkDF=spark.createDataFrame(final_vals_pds)
#sparkDF.select(['danceability','energy','popularity']).show()

#print(count)
#sparkDF.withColumn("popularity",col("popularity").cast(IntegerType())).withColumn("danceability",col("danceability").cast(FloatType())).withColumn("energy",col("energy").cast(FloatType())).printSchema()
spark_df=sparkDF.selectExpr("cast(popularity as int) popularity","cast(danceability as float) danceability","cast(energy as float) energy")
#print("//DATAFRAME//")
spark_df.show()

pds_df=spark_df.toPandas()

ind=0
for col in pds_df:
    if(col=='popularity'):
        print(pds_df.loc[ind,col])
        if(pds_df.loc[ind,col]==0):
            print("ZERO")
        ind+=1

#AI
vecAssembler=VectorAssembler(inputCols=['danceability','energy'],outputCol="Independent features")
ai_df=vecAssembler.transform(spark_df)
final_ai=ai_df.select("Independent features","popularity")
final_ai.show()
#Linear reg function
train_data,test_data=final_ai.randomSplit([0.75,0.25])
regressor=LinearRegression(featuresCol="Independent features",labelCol="popularity")
regressor=regressor.fit(train_data)
print(regressor.coefficients,regressor.intercept)
pred_results=regressor.evaluate(test_data)
pred_results.predictions.show()

dtset_final=pd.concat([final_track_pds,final_vals_pds], axis = 1)
#display(dtset_final)
colors=np.random.rand(len(pop))
plt.scatter(pop,dur,c=colors,alpha=0.5)
plt.xlabel("Popularity")
plt.ylabel("Duration")
plt.show()
plt.close()

colors=np.random.rand(len(pop1))
plt.scatter(pop1,keys,c=colors,alpha=0.5)
plt.xlabel("Popularity")
plt.ylabel("Key")
plt.show()
plt.close()

colors=np.random.rand(len(pop2))
plt.scatter(pop2,tempos,c=colors,alpha=0.5)
plt.xlabel("Popularity")
plt.ylabel("Tempo")
plt.show()

#############################GOLD#####################################
final_gld=final_df.drop('ID','playlist_id','playlist_name','playlist_genre','playlist_subgenre','lyrics','track_album_id')
final_gld1=spark.createDataFrame(final_pds)
final_gld1= final_gld1.selectExpr(
    'cast(Artist as string) Artist',
    'cast(popularity as int) popularity',
    'cast(energy as float) energy',
    'cast(danceability as float) danceability',
    'cast(duration_ms as int) duration_ms',
    'cast(key as int) key',
    'cast(Album as string) Album',
)
final_gld1.printSchema()

#Create a view or table
#temp_table_name = "spotify"
#df.createOrReplaceTempView(temp_table_name)

#%sql
#select * from `lastfm_csv`

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.

# To do so, choose your table name and uncomment the bottom line.

#permanent_table_name = "spotify"
#df.write.format("parquet").saveAsTable(permanent_table_name)