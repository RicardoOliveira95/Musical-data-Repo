import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import asc,desc,col,udf
from pyspark.sql.types import DoubleType, IntegerType, DateType
import matplotlib.pyplot as plt

#Init spark
spark=SparkSession.builder.appName('Test').getOrCreate()

# File location and type
file_location = "/FileStore/tables/soundcloud_df.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

#dataframe=pd.read_csv(file_location)
#dataframe_final=dataframe.rename(columns={'num of Plays':'# of Plays','num of Likes':'# of Likes','num of Comments':'# of Likes'})

#################Funcoes auxiliares###################
#jan=0;feb=0;mar=0;apr=0;may=0;jun=0;jul=0;aug=0;sep=0;out=0;nov=0;dec=0;
months={
    'Jan': 0,
    'Feb': 0,
    'Mar': 0,
    'Apr': 0,
    'May': 0,
    'Jun': 0,
    'Jul': 0,
    'Aug': 0,
    'Sep': 0,
    'Oct': 0,
    'Nov': 0,
    'Dec': 0
}
years={
    '12':0,
    '13':0,
    '14':0,
    '15':0,
    '16':0,
    '17':0,
    '18':0,
    '19':0,
    '20':0,
    '21':0
}

def is_num(s):
    try:
        int(s)
    except ValueError:
        return False
    else:
        return True

def switch_demo(argument):
    switcher = {
        "01": 'Jan',
        "02": 'Feb',
        "03": 'Mar',
        "04": 'Apr',
        "05": 'May',
        "06": 'Jun',
        "07": 'Jul',
        "08": 'Aug',
        "09": 'Sep',
        "10": 'Oct',
        "11": 'Nov',
        "12": 'Dec',
        "2012": '12',
        "2013": '13',
        "2014": '14',
        "2015": '15',
        "2016": '16',
        "2017": '17',
        "2018": '18',
        "2019": '19',
        "2020": '20',
        "2021": '21'
    }
    return switcher.get(argument, "Invalid month")

df_pyspark5=spark.read.option('header',True).csv(file_location,inferSchema=True)
final_df=df_pyspark5.withColumnRenamed('# of Plays','numPlays')
final_df=final_df.withColumnRenamed('# of Likes','numLikes')
final_df=final_df.withColumnRenamed('# of Comments','numComments')
final_df=final_df.withColumnRenamed('Username','Artist')
final_df=final_df.withColumnRenamed('Dates Created','Date')
final_df.printSchema()

#final_df.printSchema()

##SILVER OPERATIONS
#replace nan values
final_df=final_df.na.fill("0")

#order by date
final_pds=final_df.toPandas()
final_dates_pds=final_pds[['Date','numPlays','numLikes','numComments','Followers','Following']]
final_dt=final_pds[['Date','numPlays','numLikes','numComments','Artist','Followers','Following','Genres']]
count=0

for (row,col) in final_dates_pds['Date'].iteritems():
    #print(col)
    if(col=="Missing value" or col=="0"):
        count+=1
    if(len(col)<5 or col[0]!="2"):
        #print(row,col)
        final_dates_pds['Date'].at[row]="1995/03/17"
        final_pds['Date'].at[row]="1995/03/17"
    else:
        final_dates_pds['Date'].at[row]=col[0:10]
        final_pds['Date'].at[row]=col[0:10]

start_date = "2019-1-1"
end_date = "2019-12-31"

df_apple=pd.to_datetime(final_dates_pds['Date'])
#print(df_apple)
after_start_date = df_apple >= start_date
before_end_date = df_apple <= end_date
between_two_dates = after_start_date & before_end_date
filtered_dates = final_dates_pds.loc[between_two_dates]

#print(final_dates_pds)

for (row,col) in filtered_dates['Date'].iteritems():
    #print(col)
    if(col=="Missing value" or col=="0"):
        count+=1
    if(len(col)<5 or col[0]!="2"):
        #print(row,col)
        filtered_dates['Date'].at[row]="1995/03/17"
    else:
        filtered_dates['Date'].at[row]=col[0:10]
        #print(col[0:4])
        if(not switch_demo(col[0:4])=="Invalid month"):
            years[switch_demo(col[0:4])]+=1
        if((not switch_demo(col[5:7])=="Invalid month") and col[0:4]=="2019"):
            #print(months[switch_demo(col[5:7])]+1)
            months[switch_demo(col[5:7])]+=1

print(filtered_dates.head())
lst=months.items()
a,b=zip(*lst)
plt.plot(a,b)
plt.show()

#print(months)
final_df=spark.createDataFrame(final_pds)
final_df=final_df.sort(desc("Date"))
#final_df=final_df.sort(asc("num Of Plays"))
#final_df=final_df.sort(asc("num Of Likes"))
#final_df=final_df.sort(asc("num Of Comments"))
final_df1=final_df.select(['Date','Artist','Followers','Following'])
#print("Invalid dates: ",count,' ',final_df.count())
#print(final_dates_pds)

##STATS
final_df_stats_flwers=final_pds['Followers']
final_df_stats_flwing=final_pds['Following']
df_new_col=[];

cols=list(final_df_stats_flwers);
flwing=[];
flwers=[]
flwRat=[]

maxFlwrs=0
maxFlwng=0
minFlwrs=0
minFlwng=0
for i in range(len(final_pds)):
    if(final_df_stats_flwers[i].isnumeric() and final_df_stats_flwing[i].isnumeric() and int(final_df_stats_flwers[i])!=0 and int(final_df_stats_flwing[i])!=0):
        flwing.append(int(final_df_stats_flwing[i]));
        flwers.append(int(final_df_stats_flwers[i]));
        flwRat.append(int(final_df_stats_flwers[i])/int(final_df_stats_flwing[i]))
        
        if(int(final_df_stats_flwers[i])>maxFlwrs):
            maxFlwrs=int(final_df_stats_flwers[i])
        if(int(final_df_stats_flwers[i])<minFlwrs):
            minFlwrs=int(final_df_stats_flwers[i])
        if(int(final_df_stats_flwing[i])>maxFlwng):
            maxFlwng=int(final_df_stats_flwing[i])
        if(int(final_df_stats_flwing[i])<minFlwng):
            minFlwng=int(final_df_stats_flwing[i])
        
        if(not (int(final_df_stats_flwers[i])==0 or int(final_df_stats_flwing[i])==0)):
            df_new_col.append(int(final_df_stats_flwers[i])/int(final_df_stats_flwing[i]))
        else:
            df_new_col.append(0)
    else:
        df_new_col.append(0)
print("max: (followers) ",maxFlwrs," (following) ",maxFlwng,"min: (followers) ",minFlwrs," (following)",minFlwng),
final_pds1=final_df1.toPandas()
final_pds = final_pds.assign(flwRatio = df_new_col)  ##juntar coluna

#print(df2)
d={'followers': flwers,'following': flwing};
df3=pd.DataFrame(data=d);
x1=df3['followers'].mean();
x2=df3['followers'].median();
y1=df3['following'].mean();
y2=df3['followers'].median();
#print(df3['followers'].mean(),df3['following'].mean(),df3['followers'].median(),df3['following'].median(),df3['followers'].std(),df3['following'].std());
z=['mean','median'];
df3=pd.DataFrame({'followers': [x1,x2],
                 'following': [y1,y2]},index=z);
df3.plot.bar(rot=0);
final_pds_gnr=pd.DataFrame(final_pds['Genres'].str.strip(),columns=['Genres'])
final_pds_plays=pd.DataFrame(final_pds['numPlays'].str.strip(),columns=['numPlays'])
final_pds_gnr_plays=pd.concat([final_pds_gnr,final_pds_plays],axis=1)
final_pds_gnr_plays=final_pds_gnr_plays[final_pds_gnr_plays.Genres!="Missing Value"]

gens=final_pds_gnr_plays['Genres'].unique().tolist() ##contar generos

ind=0
for col in final_pds_gnr_plays:  
    #print(final_pds_gnr_plays.at[ind,'Genres'])
    ind+=1

for ind in final_pds_gnr_plays.index:
    for col in cols:
        if(col=='numPlays'):
            #print(final_pds_gnr_plays.at[ind][col])
            if(not final_pds_gnr_plays.at[ind][col].isnumeric()):
                final_pds_gnr_plays.at[ind][col]=0
                final_pds.at[ind][col]=0
            else:
                final_pds_gnr_plays.at[ind][col]=int(final_pds_gnr_plays.at[ind][col])
                final_pds.at[ind][col]=int(final_pds.at[ind][col])
            ind+=1

print(final_pds_gnr_plays.head())
#print(final_pds_gnr_plays.groupby('Genres')['numPlays'].unique())

#############################GOLD####################################

final_gld=final_df.drop('Track URLs','Social links','Emails','URL')
final_gld1=spark.createDataFrame(final_pds)
func = udf(lambda x: datetime.strptime(x, '%d-%m-%Y'), DateType())
final_gld1= final_gld1.selectExpr(
    'to_date(Date, \'dd-MM-yyyy\') Date',
    'cast(Artist as string) Artist',
    'cast(numPlays as int) numPlays',
    'cast(numComments as int) numComments',
    'cast(numLikes as int) numLikes',
    'cast(Followers as int) Followers',
    'cast(Following as int) Following',
    'cast(flwRatio as float) flwRatio',
)
#final_gld=final_gld.selectExpr("cast(numLikes as int) numLikes","cast(numPlays as int) numPlays","cast(numComments as int) numComments,cast(Date as date) date")
final_gld1.printSchema()
######################################################################
#print("Generes musicais mais tocados")
#print(final_pds_gnr_plays.groupby('Genres')['numPlays'].value_counts())
#final_pds_gnr_plays=final_pds_gnr[['Genres','num of Plays']]
#print(final_pds_gnr_plays.head())

# The applied options are for CSV files. For other file types, these will be ignored.
#df = spark.read.format(file_type) \
    #.option("inferSchema", infer_schema) \
    #.option("header", first_row_is_header) \
    #.option("sep", delimiter) \
    #.load(file_location)

#display(final_pds)
# Create a view or table
#temp_table_name = "soundcloud"
df.createOrReplaceTempView(temp_table_name)

#%sql
#select * from `lastfm_csv`

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.

# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "soundcloud"
final_gld1.write.format("parquet").saveAsTable(permanent_table_name)