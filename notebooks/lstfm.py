import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# File location and type
file_location = "/FileStore/tables/lastfm.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#display(df)
print(df.printSchema())

df_p=df.toPandas()
df_p_cnts=df_p['country']
df_new_col=[]

##Continents
Antarctica=['Antartica']
eur_countries=['Germany','France','United Kingdom','Finland','Portugal','Italy','Austria','Greece','Netherlands','Norway','Poland','Ukraine','Estonia',
'Spain','Lithuania','Czech Republic','France','Turkey','Slovakia','Belgium','Romania','Switzerland','Israel','Iceland','Armenia','Virgin Islands, British','Faroe Islands','Luxembourg','Bosnia and Herzegovina','Georgia','Hungary','Belarus','Netherlands Antilles','Gibraltar','Norfolk Island','Slovenia','Andorra','Bulgaria','Croatia','Denmark','Malta','Latvia','Serbia','Ireland','French Polynesia','Albania','Christmas Island','Monaco','United States Minor Outlying Islands','Tunisia','Montenegro','Moldova','Macedonia','Cyprus']
amr_countries=['United States','Canada','Canada','Mexic','Uruguay','Brazil','Argentina','Chile','Virgin Islands, U.s.','Northern Mariana Islands','New Caledonia','Nicaragua','Paraguay','Cayman Islands','Heard Island and Mcdonald Islands','Honduras','Seychelles','Chad','Bolivia','Barbados','El Salvador','Lesotho','Maldives','Peru','Colombia','Saint Vincent and the Grenadines','American Samoa','Philippines','Guatemala','Panama','Jamaica','Costa Rica','Venezuela',',Puerto Rico','Ecuador','Cuba'] 
asian_countries=['Korea','Japan','Iran, Islamic Republic of','Thailand','Russian Federation','Singapore','Sri Lanka','Mauritius','Dominica','Tuvalu','Cambodiav','China','India','Syrian Arab Republic','Indonesia','United Arab Emirates','Holy See (Vatican City State)','Nauru','Pakistan','Qatar','Antigua and Barbuda','Nepal','Bangladesh','Viet Nam','Kazakhstan','Afghanistan','Malaysia','Taiwan','Hong Kong']
ocn_countries=['Australia','British Indian Ocean Territory','New Zealand']
afr_countries=['Ghana','Bhutan','Guam','Trinidad and Tobago','Suriname','French Southern Territories','Lebanon','Cape Verde','Montserrat','Wallis and Futuna','Brunei Darussalam','Mongolia','Gambia','Nigeria','Cocos (Keeling) ,Islands','Morocco','Western Sahara','Zimbabwe','Sao Tome and Principe','Oman','Congo, the Democratic Republic of the Bermuda','Tajikistan','Equatorial Guinea','Tokelau','South Georgia and the South Sandwich Islands','Angola','South Africa','Algeria','Djibouti','Niue','Dominican Republic','Egypt','Burkina Faso','Uganda','Togo','Saudi Arabia']

paises=df_p['country'].unique().tolist() ##contar paises
#for country in range(len(paises)):
    #print(paises[country])
eur=0
ame=0
asia=0
afr=0
ocn=0;
for item in range(len(df_p)):
    #print(df_p_cnts[item])
    if(df_p_cnts[item] in eur_countries):  ##este codigo e incompleto
        df_new_col.append("EUR")  #apenas para os paises europeus e americanos
        eur+=1
    elif(df_p_cnts[item] in amr_countries):
        df_new_col.append("AME")
        ame+=1
    elif(df_p_cnts[item] in asian_countries):
        df_new_col.append("ASIA")
        asia+=1
    elif(df_p_cnts[item] in afr_countries):
        df_new_col.append("AFR")
        afr+=1
    else:
        df_new_col.append("OCN")
        ocn+=1

df2 = df_p.assign(region = df_new_col)  ##juntar coluna
#print(df2)
continents=[eur,ame,asia,afr,ocn]
df3=pd.DataFrame({'continents': continents},
                index=['Europe','America','Asia','Africa','Oceania'])
fig=df3.plot.pie(y='continents',explode=(0,0.1,0,0,0),autopct='%1.1f%%',shadow=True,figsize=(5,5));
top5=df2['country'].value_counts().head()
top5countries=df2['country'].value_counts().head().tolist()
print(top5countries)
print(df2.head())
plt.show()
#SAVE plot as IMG

#plt.savefig("/dbfs/FileStore/tables/df3")
#plt.rcParams["figure.figsize"]=[700000,10000]
#df3=pd.DataFrame({'followers': [x1,x2],
                 #'following': [y1,y2]},index=z);
plt.close()
top5.plot.bar(rot=0);
#plt.savefig("/dbfs/FileStore/tables/df3.png")
# Create a view or table

##GOLD
final_ds=spark.createDataFrame(df2)
final_ds=final_ds.drop("User")
final_ds.printSchema()

temp_table_name = "lastfm"

df.createOrReplaceTempView(temp_table_name)

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "lastfm"
#GOLD

final_ds.write.format("parquet").saveAsTable(permanent_table_name)