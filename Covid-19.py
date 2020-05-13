# Databricks notebook source
# MAGIC %md
# MAGIC ##Setting up the Environment##
# MAGIC 
# MAGIC In the first part I will get a picture of the current databricks environment and download the datasets from an amazon S3 bucket.
# MAGIC I will remove any current .csv files in the home directory to ensure no previous downloads have persisted.

# COMMAND ----------

# DBTITLE 0,Identifying Environment
# MAGIC %sh
# MAGIC pwd
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC rm *.csv*

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC wget https://sparkprojectevand.s3.us-east-2.amazonaws.com/DL-us-mobility-daterow.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC wget https://sparkprojectevand.s3.us-east-2.amazonaws.com/cases_and_deaths.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC wget https://sparkprojectevand.s3.us-east-2.amazonaws.com/community_mobility_change_us.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC wget https://sparkprojectevand.s3.us-east-2.amazonaws.com/social_distancing_by_state.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC wget https://sparkprojectevand.s3.us-east-2.amazonaws.com/key_social_distancing.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ###Directory Setup###
# MAGIC 
# MAGIC I first remove the directory from any previous run. Then remake the directory and move the csv files into the project directory named "project3_data".
# MAGIC 
# MAGIC Only run the first command (command 10) on initial runthrough, as running it again will remove the data we are using for the rest of the project

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r project3_data

# COMMAND ----------

# MAGIC %sh
# MAGIC ls
# MAGIC mkdir project3_data
# MAGIC mv *.csv project3_data

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC ls project3_data

# COMMAND ----------

# DBTITLE 1,Displaying the current Databricks filesystem
display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Next I will make a project directory in the dbfs. I will copy all of the raw csv's that were downloaded above into the dbfs.

# COMMAND ----------

dbutils.fs.rm("dbfs:/project3_spark/", True)

# COMMAND ----------

# DBTITLE 0,Making a project directory in the dbfs
# MAGIC %fs 
# MAGIC mkdirs project3_spark

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# DBTITLE 1,Copying files from base directory into the dbfs directory project3_spark
dbutils.fs.cp("file:/databricks/driver/project3_data", "dbfs:/project3_spark/", True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls project3_spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Design##
# MAGIC 
# MAGIC In the next part, I am constructing schemas manually for the five following datasets:
# MAGIC 
# MAGIC * cases_and_deaths - simply table with covid 19 cases and fatalities by country/state level
# MAGIC * community_mobility_change_us - person mobility data by country/state level and mobility type
# MAGIC * DL-us-mobility-daterow - person mobility data by state/county level with mobility index scores
# MAGIC * social_distancing_by_state - method and date of social distancing measures on a state basis
# MAGIC * key_social_distancing - the key table for social_distancing_by_state

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType
from pyspark.sql import Row

# COMMAND ----------

# cases_and_deaths
cases_and_deaths_schema = StructType([
StructField("id", IntegerType(), True), 
StructField("province_state", StringType(), True), 
StructField("country_region", StringType(), True), 
StructField("date", DateType(), True), 
StructField("confirmed_cases", DoubleType(), True), 
StructField("fatalities", DoubleType(), True)])

cases_and_deaths_df = spark.read.format("csv").option("header","true").\
schema(cases_and_deaths_schema).load("project3_spark/cases_and_deaths.csv")

# initially contains many contain countries some of which don't have states
cases_and_deaths_df.show(3)

# COMMAND ----------

# community_mobility_change_us
community_mobility_change_us_schema = StructType([
StructField("location", StringType(), True), 
StructField("loc_type", StringType(), True), 
StructField("parent_loc", StringType(), True), 
StructField("mobility_type", StringType(), True), 
StructField("date", DateType(), True), 
StructField("mobility_change", DoubleType(), True)])

community_mobility_change_us_df = spark.read.format("csv").option("header","true").\
schema(community_mobility_change_us_schema).load("project3_spark/community_mobility_change_us.csv")

community_mobility_change_us_df.show(3)

# COMMAND ----------

# DL-us-mobility-daterow
dl_us_mobility_daterow_schema = StructType([
StructField("date", DateType(), True),
StructField("country_code", StringType(), True),
StructField("admin_level", IntegerType(), True),
StructField("state", StringType(), True),
StructField("county", StringType(), True), 
StructField("fips", StringType(), True),
StructField("samples", IntegerType(), True), 
StructField("m50", DoubleType(), True),
StructField("m50_index", IntegerType(), True)])

dl_us_mobility_daterow_df = spark.read.format("csv").option("header","true").\
schema(dl_us_mobility_daterow_schema).\
load("project3_spark/DL-us-mobility-daterow.csv")

# some rows dont contain county data which means it is data on the state as a whole
dl_us_mobility_daterow_df.show(3)

# COMMAND ----------

#social_distancing_by_state
social_distancing_by_state_schema = StructType([
StructField("state", StringType(), True),
StructField("religious_restrictions", IntegerType(), True),
StructField("stay_at_home_end_date_as_of_april_28", DateType(), True),
StructField("current_restriction", IntegerType(), True),
StructField("current_population", IntegerType(), True)])

social_distancing_by_state_df = spark.read.format("csv").option("header","true").\
schema(social_distancing_by_state_schema).\
load("project3_spark/social_distancing_by_state.csv")

social_distancing_by_state_df.show(3)

# COMMAND ----------

#key_social_distancing
key_social_distancing_schema = StructType([
StructField("key", IntegerType(), True),
StructField("religious_restrictions", StringType(), True),
StructField("current_restrictions", StringType(), True)])

key_social_distancing_df = spark.read.format("csv").option("header","true").\
schema(key_social_distancing_schema).\
load("project3_spark/key_social_distancing.csv")

key_social_distancing_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Cleansing & Quality Checks##
# MAGIC 
# MAGIC Now that the data has been loaded from the raw csvs, I want to cleanse the data to look at states within the United States and look for potential outliers or errors in the data.
# MAGIC 
# MAGIC First, I will put all the dataframes into parquet tables which can then be used with spark.sql

# COMMAND ----------

dbutils.fs.rm("project3_spark/social_distancing_by_state.parquet", True)
dbutils.fs.rm("project3_spark/key_social_distancing.parquet", True)
dbutils.fs.rm("project3_spark/dl_us_mobility_daterow.parquet", True)
dbutils.fs.rm("project3_spark/cases_and_deaths.parquet", True)
dbutils.fs.rm("project3_spark/community_mobility_change_us.parquet", True)

# COMMAND ----------

# MAGIC %fs ls project3_spark

# COMMAND ----------

social_distancing_by_state_df.select("*").write.save("project3_spark/social_distancing_by_state.parquet", format="parquet")
key_social_distancing_df.select("*").write.save("project3_spark/key_social_distancing.parquet", format="parquet")
dl_us_mobility_daterow_df.select("*").write.save("project3_spark/dl_us_mobility_daterow.parquet", format="parquet")
cases_and_deaths_df.select("*").write.save("project3_spark/cases_and_deaths.parquet", format="parquet")
community_mobility_change_us_df.select("*").write.save("project3_spark/community_mobility_change_us.parquet", format="parquet")

# COMMAND ----------

# MAGIC %fs ls project3_spark

# COMMAND ----------

cases_and_deaths_cleanse_df = spark.sql("""SELECT DISTINCT * FROM parquet.`project3_spark/cases_and_deaths.parquet`""").where("country_region LIKE 'US'")

cases_and_deaths_cleanse_df.show(3)

# COMMAND ----------

community_mobility_cleanse_df = spark.sql("""SELECT * FROM parquet.`project3_spark/community_mobility_change_us.parquet`""").where("location IS NOT NULL").where("parent_loc IS NOT NULL")

community_mobility_cleanse_df.show(3)

# COMMAND ----------

# MAGIC %fs ls project3_spark

# COMMAND ----------

dl_mobility_cleanse_df = spark.sql("""SELECT * FROM parquet.`project3_spark/dl_us_mobility_daterow.parquet`""").where("country_code LIKE 'US'").where("state IS NOT NULL")
dl_mobility_cleanse_df.show(3)

# COMMAND ----------

# DBTITLE 1,Deleting any previous cleansed parquet tables if they existed
dbutils.fs.rm("project3_spark/community_mobility_cleanse.parquet", True)
dbutils.fs.rm("project3_spark/dl_mobility_cleanse.parquet", True)
dbutils.fs.rm("project3_spark/cases_and_deaths_cleanse.parquet", True)
dbutils.fs.rm("project3_spark/social_distancing_by_state_cleanse.parquet", True)
dbutils.fs.rm("project3_spark/key_social_distancing_cleanse.parquet", True)

# COMMAND ----------

social_distancing_by_state_cleanse_df = social_distancing_by_state_df
key_social_distancing_cleanse_df = key_social_distancing_df

# COMMAND ----------

community_mobility_cleanse_df.select("*").write.save("project3_spark/community_mobility_cleanse.parquet", format="parquet")
dl_mobility_cleanse_df.select("*").write.save("project3_spark/dl_mobility_cleanse.parquet", format="parquet")
cases_and_deaths_cleanse_df.select("*").write.save("project3_spark/cases_and_deaths_cleanse.parquet", format="parquet")
social_distancing_by_state_cleanse_df.select("*").write.save("project3_spark/social_distancing_by_state_cleanse.parquet", format="parquet")
key_social_distancing_cleanse_df.select("*").write.save("project3_spark/key_social_distancing_cleanse.parquet", format="parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Joins##
# MAGIC 
# MAGIC Now that the data has been loaded into dataframes, I will be performed joins on the datasets in several different combinations to answer the questions on social distancing. I first will put the datasets into temperatory views.

# COMMAND ----------

community_mobility_cleanse_df.createOrReplaceTempView("community_mobility")
dl_mobility_cleanse_df.createOrReplaceTempView("dl_mobility")
cases_and_deaths_cleanse_df.createOrReplaceTempView("cases_and_deaths")
social_distancing_by_state_cleanse_df.createOrReplaceTempView("social_distancing")
key_social_distancing_cleanse_df.createOrReplaceTempView("key_social_distancing")

# COMMAND ----------

# MAGIC %md
# MAGIC The first join I am making is on the key of social distancing and the social distancing by state table. This is adding the description into the social distancing by state table

# COMMAND ----------

temp = spark.sql("""CREATE OR REPLACE TEMP VIEW temp_key_social_distance AS SELECT state, social_distancing.religious_restrictions, key_social_distancing.religious_restrictions AS religious_rest, stay_at_home_end_date_as_of_april_28, current_population, current_restriction FROM key_social_distancing INNER JOIN social_distancing ON (key_social_distancing.key = social_distancing.religious_restrictions)""")


# COMMAND ----------

social_distance_final_df = spark.sql("""SELECT state, temp_key_social_distance.religious_rest, stay_at_home_end_date_as_of_april_28, current_population, key_social_distancing.current_restrictions FROM temp_key_social_distance INNER JOIN key_social_distancing ON (key_social_distancing.key = temp_key_social_distance.current_restriction)""")
social_distance_final_df.createOrReplaceTempView("social_distance_final")

# COMMAND ----------

social_distance_final_df.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC Next I am going to make a join with the two mobility datasets on the date and state.
# MAGIC I will also do a join on the county level.

# COMMAND ----------

# location|loc_type|   parent_loc|      mobility_type|      date|     mobility_change|
spark.sql("""CREATE OR REPLACE TEMP VIEW temp_state_mobility AS SELECT * FROM community_mobility WHERE parent_loc like 'United States'""")

# COMMAND ----------

state_level_mobility_join = spark.sql("""SELECT dl_mobility.state, dl_mobility.date, mobility_type, mobility_change, m50, m50_index FROM temp_state_mobility RIGHT OUTER JOIN dl_mobility ON (temp_state_mobility.date = dl_mobility.date AND temp_state_mobility.location = dl_mobility.state) WHERE dl_mobility.county IS NULL""")
state_level_mobility_join.createOrReplaceTempView("state_level_mobility")
state_level_mobility_join.show(3)

# COMMAND ----------

county_level_mobility_join = spark.sql("""SELECT dl_mobility.state, dl_mobility.county, dl_mobility.date, mobility_type, mobility_change, m50, m50_index FROM community_mobility INNER JOIN dl_mobility ON (community_mobility.date = dl_mobility.date AND community_mobility.location = dl_mobility.county AND community_mobility.parent_loc = dl_mobility.state)""")
county_level_mobility_join.createOrReplaceTempView("county_level_mobility")
county_level_mobility_join.show(3)

# COMMAND ----------

# MAGIC %md 
# MAGIC Next I am going to join in the cases and deaths on the previous joins based on the state and the date.
# MAGIC 
# MAGIC First, I will join on the social distancing dataframe and then on the mobility dataframes.

# COMMAND ----------

social_distance_w_cases_deaths = spark.sql("""SELECT province_state, date, confirmed_cases, fatalities, religious_rest, stay_at_home_end_date_as_of_april_28, current_population, current_restrictions FROM social_distance_final RIGHT OUTER JOIN cases_and_deaths ON (state = province_state) WHERE current_population IS NOT NULL""")
social_distance_w_cases_deaths.createOrReplaceTempView("social_distance_cases_deaths")
social_distance_w_cases_deaths.orderBy("date").show(3)

# COMMAND ----------

state_mobility_w_cases_deaths = spark.sql("""SELECT state, state_level_mobility.date, confirmed_cases, fatalities, mobility_type, mobility_change, m50, m50_index FROM cases_and_deaths RIGHT OUTER JOIN state_level_mobility ON (state = province_state AND cases_and_deaths.date = state_level_mobility.date)""")
state_mobility_w_cases_deaths.createOrReplaceTempView("state_mobility_cases_deaths")
state_mobility_w_cases_deaths.show(3)

# COMMAND ----------

county_mobility_w_cases_deaths = spark.sql("""SELECT state, county_level_mobility.county, county_level_mobility.date, confirmed_cases AS statewide_confirmed_cases, fatalities as statewide_fatalities, mobility_type, mobility_change, m50, m50_index FROM cases_and_deaths RIGHT OUTER JOIN county_level_mobility ON (state = province_state AND cases_and_deaths.date = county_level_mobility.date)""")
county_mobility_w_cases_deaths.createOrReplaceTempView("county_mobility_cases_deaths")
county_mobility_w_cases_deaths.show(3)

# COMMAND ----------

county_mobility_social_distance_cases_deaths = spark.sql("""SELECT county_mobility_cases_deaths.state, county_mobility_cases_deaths.county, county_mobility_cases_deaths.date, county_mobility_cases_deaths.statewide_confirmed_cases, statewide_fatalities, stay_at_home_end_date_as_of_april_28 AS restriction_end_date_of_april28,  current_population, religious_rest as religious_restrictions, current_restrictions, mobility_type, mobility_change, m50, m50_index FROM county_mobility_cases_deaths INNER JOIN social_distance_cases_deaths ON (county_mobility_cases_deaths.state = social_distance_cases_deaths.province_state AND county_mobility_cases_deaths.date = social_distance_cases_deaths.date AND social_distance_cases_deaths.fatalities = county_mobility_cases_deaths.statewide_fatalities)""")

# COMMAND ----------

combined_county_df = county_mobility_social_distance_cases_deaths.orderBy("state", "date", "county")
combined_county_df.createOrReplaceTempView("combined_county")
combined_county_df.show(3)

# COMMAND ----------

state_mobility_social_distance_cases_deaths = spark.sql("""SELECT state_mobility_cases_deaths.state, state_mobility_cases_deaths.date, state_mobility_cases_deaths.confirmed_cases, state_mobility_cases_deaths.fatalities, stay_at_home_end_date_as_of_april_28 AS restriction_end_date_of_april28,  current_population, religious_rest as religious_restrictions, current_restrictions, mobility_type, mobility_change, m50, m50_index FROM state_mobility_cases_deaths INNER JOIN social_distance_cases_deaths ON (state_mobility_cases_deaths.state = social_distance_cases_deaths.province_state AND state_mobility_cases_deaths.date = social_distance_cases_deaths.date AND social_distance_cases_deaths.fatalities = state_mobility_cases_deaths.fatalities)""")

# COMMAND ----------

combined_df = state_mobility_social_distance_cases_deaths.orderBy("state", "date")
combined_df.createOrReplaceTempView("combined")
combined_df.show(30000)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Exploratory Analysis##
# MAGIC 
# MAGIC Now that I have joined the datasets together into several different dataframes, I can begin analysis on social distancing and its effectiveness on stoping the spread of covid-19. The joined dataframes now include:
# MAGIC 
# MAGIC 
# MAGIC * combined_df - |  state|county|date|statewide_confirmed_cases|statewide_fatalities|restriction_end_date_of_april28|current_population|religious_restrictions|current_restrictions|      mobility_type|mobility_change|m50|m50_index|
# MAGIC * county_mobility_w_cases_deaths - state|county|date|statewide_confirmed_cases|statewide_fatalities|mobility_type|mobility_change|m50|m50_index|
# MAGIC * state_mobility_w_cases_deaths - state|date|confirmed_cases|fatalities|mobility_type|mobility_change|m50|m50_index|
# MAGIC * social_distance_w_cases_deaths - province_state|date|confirmed_cases|fatalities|religious_rest|stay_at_home_end_date_as_of_april_28|current_population|current_restrictions|
# MAGIC * original dataframes with schemas made in the schema design section and cleansed 

# COMMAND ----------

# MAGIC %md
# MAGIC Calculations to make:
# MAGIC 
# MAGIC * get the density of cases for each state based on population, sum the number of cases and deaths and divide by population - social_distance_w_cases_deaths
# MAGIC * group together different social distancing measures and get the average density of cases for each method of social distancing - social_distance_w_cases_deaths
# MAGIC * average the mobility of each state and compare that with cases and deaths - state_mobility_w_cases_deaths
# MAGIC 
# MAGIC Definition of the mobility measures:
# MAGIC 
# MAGIC m50: The median of the max-distance mobility for all samples in the specified region. in km
# MAGIC 
# MAGIC m50_index: The percent of normal m50 in the region, with normal m50 defined during 2020-02-17 to 2020-03-07.
# MAGIC 
# MAGIC mobility_change: Indicate the percentage of the change compare to the normal days before the spread of COVID19
# MAGIC 
# MAGIC mobility_type: The mobility type, e.g., 'Retail & recreation', 'Grocery & pharmacy', 'Parks', 'Transit stations', 'Workplace' or 'Residential'

# COMMAND ----------

#social_distance_cases_deaths
# density_social_dist_df = spark.sql("""SELECT confirmed_cases, date FROM social_distance_cases_deaths""")
density_social_dist_df = spark.sql("""SELECT *, (confirmed_cases / current_population) AS cases_density, (fatalities / current_population) AS fatality_density FROM social_distance_cases_deaths""")


# COMMAND ----------

ordered_density_social_dist = density_social_dist_df.orderBy("current_restrictions")

ordered_density_social_dist.createOrReplaceTempView("ordered_density_social_dist")
ordered_density_social_dist.show(3)

# COMMAND ----------

social_distance_method_average_case_by_density = spark.sql("""SELECT current_restrictions, avg(cases_density) AS average_cases, avg(fatality_density) AS average_fatalities, avg(confirmed_cases) as avg_num_cases, avg(fatalities) as avg_num_fatalities FROM ordered_density_social_dist GROUP BY current_restrictions""")

# COMMAND ----------

social_distance_method_avg_case_ordered = social_distance_method_average_case_by_density.orderBy("average_cases")
social_distance_method_avg_case_ordered.show(7, False)

# COMMAND ----------

display(social_distance_method_avg_case_ordered)

# COMMAND ----------

display(social_distance_method_avg_case_ordered)

# COMMAND ----------

social_distance_method_average_case_by_density.orderBy("average_fatalities").show(7, False)

# COMMAND ----------

# MAGIC %md
# MAGIC The model just ran was on the total average cases and fatality density per state - that is the average number of cases based on the population of the state over the course of dates in the dataset. 
# MAGIC 
# MAGIC The following model will be run with just April 28th data for the number of cases and fatalities to get the most current data on social distancing measures compared to the total number of cases or deaths in a state. April 28th has the most current data in the dataset.

# COMMAND ----------

social_distance_method_average_case_by_density_april_28 = spark.sql("""SELECT current_restrictions, avg(cases_density) AS cases_density, avg(fatality_density) AS fatalities_density, avg(confirmed_cases) as num_cases, avg(fatalities) as num_fatalities  FROM ordered_density_social_dist WHERE date >'2020-04-27' GROUP BY current_restrictions""")

# COMMAND ----------

social_distance_avg_case_april_28_ordered = social_distance_method_average_case_by_density_april_28.orderBy("num_cases")
social_distance_avg_case_april_28_ordered.show(7,False)

# COMMAND ----------

display(social_distance_avg_case_april_28_ordered)
# in order th display shows
# 20 or fewer, closed nonessential businesses, opening of some small businesses, safer at home, social distancing of 6 feet but no restrictions, stay at home

# COMMAND ----------

display(social_distance_avg_case_april_28_ordered)
# in order th display shows
# 20 or fewer, closed nonessential businesses, opening of some small businesses, safer at home, social distancing of 6 feet but no restrictions, stay at home

# COMMAND ----------

social_distance_method_average_case_by_density_april_28.orderBy("num_fatalities").show(7,False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The above queries show the number of cases based on population, or the density of cases, for each of the 7 types of social distancing. There are only six output as no states yet have the 7th level which would be no restrictions. The results were computed as an average over the type of social distancing restriction. The first two outputs showed the averages over the entire course of the dataset which starts on March 1st. The second two outputs showed the averages as of April 28th which was the latest date. The number of cases and fatilities on April 28th in the dataset was the cumulative total of cases for that state. Thus, the last two outputs show the average cases and average fatalities up to April 28th. The results show that the 'Stay at Home' group of states actually has the highest average cases and average fatalities as of April 28th. This makes sense as the states that have been hit the hardest are ones that have the Stay at Home Order. 
# MAGIC 
# MAGIC Below I will output the cases density for each state to give a reference how each state was fairing as of April 28th. I will also show the current restrictions column so you can see the distribution of types of social distancing and the states current case and fatality density.

# COMMAND ----------

states_cases_density = spark.sql("""SELECT province_state, cases_density, fatality_density, current_restrictions, confirmed_cases, fatalities FROM ordered_density_social_dist WHERE date >'2020-04-27' ORDER BY cases_density""")
states_cases_density.show(51, False)

# COMMAND ----------

display(states_cases_density)

# COMMAND ----------

display(states_cases_density)

# COMMAND ----------

# this dataframe is used as a temporary variable in combining all of the datasets together. There will be a county and state version

temp_county_pop_df = spark.sql("""SELECT state, county, date, restriction_end_date_of_april28, religious_restrictions, current_restrictions, mobility_type, mobility_change, m50, m50_index, (statewide_confirmed_cases / current_population) AS cases_density, (statewide_fatalities / current_population) AS fatality_density FROM combined_county ORDER BY state, county, date""")
temp_county_pop_df.createOrReplaceTempView("temp_county_pop_df")

temp_pop_df = spark.sql("""SELECT state, date, restriction_end_date_of_april28, religious_restrictions, current_restrictions, mobility_type, mobility_change, m50, m50_index, (confirmed_cases / current_population) AS cases_density, (fatalities / current_population) AS fatality_density FROM combined ORDER BY state, date""")
temp_pop_df.createOrReplaceTempView("temp_pop_df")
temp_pop_df.show(3)

# COMMAND ----------

temp_county_pop_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC The "temp_pop_df" and "temp_county_pop_df" will be used for several problems. The first of which we will explore is on mobility_type and mobility_change. We will then do an analysis on the m50 and m50_index. After that we will look at the entire combined dataset and see what trends we can find with the social distancing restrictions, mobility, and the cases. 

# COMMAND ----------

mobility_type_change_df = spark.sql("""SELECT state, county, avg(mobility_change) AS mobility_change, mobility_type, avg(cases_density) AS average_cases, avg(fatality_density) AS average_fatalities, current_restrictions  FROM temp_county_pop_df GROUP BY state, county, mobility_type, current_restrictions ORDER BY state, county, mobility_type""")
mobility_type_change_df.createOrReplaceTempView("mobility_type_change_df")
mobility_type_change_df.show(8)

# COMMAND ----------

statewide_mobility_type_df = spark.sql("""SELECT state, avg(mobility_change) as state_avg_mobility_change, avg(average_cases) as state_avg_cases, avg(average_fatalities) as state_avg_fatalities, current_restrictions FROM mobility_type_change_df GROUP BY state, current_restrictions""")

# COMMAND ----------

statewide_mobility_ordered_df = statewide_mobility_type_df.orderBy("state_avg_mobility_change")
statewide_mobility_ordered_df.show(3, False) # change show value from 3 to 51 to see every state

# COMMAND ----------

display(statewide_mobility_ordered_df)

# COMMAND ----------

statewide_mobility_type_df.createOrReplaceTempView("statewide_mobility_type_df")
statewide_mobility_type_df.orderBy("state_avg_cases").show(3, False) # change show value from 3 to 51 to see every state

# COMMAND ----------

# MAGIC %md
# MAGIC The output above shows the average mobility change since March 1st for each state based on county by county basis. Because of the differing level of detail, the average fatalities and cases changed as some counties are probably not correctly recording cases and fatalities. The average fatalities and cases are all lower than when we just used the state data rather than drilling down to the county level. Regardless, we can see from the output that a lot of the states that are stay at home have seen the biggest decrease in movement. That said, there still are states with stay at home orders that haven't seen as big of difference in mobility.
# MAGIC 
# MAGIC Next, we will look at the m50 and m50 index from descarteslabs. 
# MAGIC 
# MAGIC m50: The median of the max-distance mobility for all samples in the specified region in km.
# MAGIC 
# MAGIC m50_index: The percent of normal m50 in the region, with normal m50 defined during 2020-02-17 to 2020-03-07.

# COMMAND ----------

mobility_m50_df = spark.sql("""SELECT state, county, avg(m50) AS avg_m50, avg(m50_index) AS avg_m50_index, avg(cases_density) as average_cases, avg(fatality_density) AS average_fatalities, current_restrictions  FROM temp_county_pop_df WHERE county NOT LIKE 'Pocahontas County' GROUP BY state, county, current_restrictions ORDER BY state, county""")
mobility_m50_df.createOrReplaceTempView("mobility_m50_df")

# COMMAND ----------

# found outlier in dataset Pocahontas county that had a m50_index of 11070, will remove so data is not thrown off. Removed in command above.
from pyspark.sql.functions import col
mobility_m50_df.where(col("state") == "West Virginia").show(3) # change show value from 3 to 100 to see every state

# COMMAND ----------

statewide_m50_df = spark.sql("""SELECT state, avg(avg_m50) as state_avg_m50, avg(avg_m50_index) as state_avg_m50_index, avg(average_cases) as state_avg_cases, avg(average_fatalities) as state_avg_fatalities, current_restrictions FROM mobility_m50_df GROUP BY state, current_restrictions""")
statewide_m50_ordered_df = statewide_m50_df.orderBy("state_avg_m50_index")
statewide_m50_ordered_df.show(3, False) # change show value from 3 to 51 to see every state

# COMMAND ----------

display(statewide_m50_ordered_df)

# COMMAND ----------

display(statewide_m50_ordered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC I find the output above very interesting. This does an even better job of showing the different restriction levels at play. Even more so than the previous mobility type and change metrics, the m50_index shows that the stay at home states are indeed moving less. New Yorker's are moving only 3.85% of their average. Meanwhile people in Wyoming are moving about 70% of their normal over the course of the data. This makes sense as many people in Wyoming are on farms/ranches and there also is not strict restrictions on movements of individuals. The m50 and m50_index seem to be better indicators of social distancing and have a strong correlation.

# COMMAND ----------

statewide_m50_df.orderBy("state_avg_cases").show(51, False) # change show value from 3 to 51 to see every state

# COMMAND ----------

# MAGIC %md
# MAGIC ###Advanced ML Algorithms###

# COMMAND ----------

# will be creating a new ML dataframe from the combined dataframe above
from pyspark.ml.feature import StringIndexer
interested_cols_ML = spark.sql("""SELECT DISTINCT state, date, restriction_end_date_of_april28, religious_restrictions, current_restrictions, m50, m50_index, confirmed_cases AS cases, fatalities AS fatalities, (confirmed_cases / current_population) AS cases_density, (fatalities / current_population) AS fatality_density FROM combined ORDER BY state, date""")
interested_cols_ML.createOrReplaceTempView("interested_cols_ML")
interested_cols_ML.show(3)

# COMMAND ----------

# turning string categorical variables back into integers
lblIndxr = StringIndexer().setInputCol("religious_restrictions").setOutputCol("label_religious_rest") 
idxRes = lblIndxr.fit(interested_cols_ML).transform(interested_cols_ML)
lblIndxr2 = StringIndexer().setInputCol("current_restrictions").setOutputCol("label_curr_rest") 
idxRes2 = lblIndxr2.fit(idxRes).transform(idxRes)
# tried using for loop, ended up taking just as long
# indexers = [StringIndexer(inputCol=column, outputCol=column+"_label").fit(interested_cols_ML).transform(interested_cols_ML) for column in interested_cols_ML.columns if "_restrictions" in column ]


# COMMAND ----------

cols_drop = ['fatality_density','religious_restrictions','current_restrictions']
final_cols_df = idxRes2.drop(*cols_drop)
final_cols_df.show(3)

# COMMAND ----------

dbutils.fs.rm("project3_spark/final_ml_df.parquet", True)

# COMMAND ----------

final_cols_df.select("*").write.save("project3_spark/final_ml_df.parquet", format="parquet")

# COMMAND ----------

final_ml_df = spark.sql("""SELECT * FROM parquet.`project3_spark/final_ml_df.parquet`""")
final_ml_df.createOrReplaceTempView("final_ml_df")
final_ml_df.where("date >= '2020-04-22'").show(3)
# did this to speed up results rather than relying on all the temp tables we just made

# COMMAND ----------

train_final_ml_df = final_ml_df.where("date < '2020-04-22'")
test_final_ml_df = final_ml_df.where("date >= '2020-04-22'")

# COMMAND ----------

# checks to see that the dataframe split worked successfully
test_final_ml_df.count()

# COMMAND ----------

final_ml_df.count()

# COMMAND ----------

train_final_ml_df.count()

# COMMAND ----------

train_final_ml_df.show(3)

# COMMAND ----------

# linear regression model, starting witha vector assembler
from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler(inputCols = ['m50', 'm50_index', 'label_religious_rest', 'label_curr_rest'], outputCol = 'features')
vtrain_final = vectorAssembler.transform(train_final_ml_df)
vtrain_final = vtrain_final.select(['features', 'cases'])
vtrain_final.show(3)

vtest_final = vectorAssembler.transform(test_final_ml_df)
vtest_final = vtest_final.select(['features', 'cases'])
vtest_final.show(3)

# COMMAND ----------

from pyspark.ml.regression import LinearRegression
# performing the linear regression training
lr = LinearRegression(featuresCol = 'features', labelCol='cases', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(vtrain_final)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

# COMMAND ----------

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# COMMAND ----------

vtrain_final.describe().show()

# COMMAND ----------

lr_predictions = lr_model.transform(vtest_final)
lr_predictions.select("prediction","cases","features").show(50)
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="cases",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

# COMMAND ----------

test_result = lr_model.evaluate(vtest_final)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

# COMMAND ----------

display(lr_predictions)

# COMMAND ----------

# MAGIC %md 
# MAGIC The output shows the model has a very hard time predicting the number of cases based on m50, m50 index, current restrictions, and religious restrictions. If anything, the model seemed to be severly underestimating the growth of the number of cases. Next, I will try the model again but including cases in the feature set to see how the model can predict future numbers based on the current trend. We will see if the model overpredicts the values which would indicate that the social distancing is having some affect.

# COMMAND ----------

vectorAssembler2 = VectorAssembler(inputCols = ['m50', 'm50_index', 'label_religious_rest', 'label_curr_rest','cases','fatalities'], outputCol = 'features')
vtrain_final2 = vectorAssembler2.transform(train_final_ml_df)
vtrain_final2 = vtrain_final2.select(['features', 'cases'])
vtrain_final2.show(3)

vtest_final2 = vectorAssembler2.transform(test_final_ml_df)
vtest_final2 = vtest_final2.select(['features', 'cases'])
vtest_final2.show(3)

# COMMAND ----------

lr2 = LinearRegression(featuresCol = 'features', labelCol='cases', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model2 = lr.fit(vtrain_final2)
print("Coefficients: " + str(lr_model2.coefficients))
print("Intercept: " + str(lr_model2.intercept))

# COMMAND ----------

trainingSummary2 = lr_model2.summary
print("RMSE: %f" % trainingSummary2.rootMeanSquaredError)
print("r2: %f" % trainingSummary2.r2)

# COMMAND ----------

vtrain_final2.describe().show()

# COMMAND ----------

lr_predictions2 = lr_model2.transform(vtest_final2)
lr_predictions2.select("prediction","cases","features").show(10)
from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator2 = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="cases",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator2.evaluate(lr_predictions2))

# COMMAND ----------

predictions2 = lr_model2.transform(vtest_final2)
predictions2.select("prediction","cases","features").show(50)

# COMMAND ----------

display(predictions2)

# COMMAND ----------

# MAGIC %md
# MAGIC The prediction with the second linear regression model was a very clear linear line and had a R-squared value of nearly one. Included the previous cases amounts allowed the model to correctly predicted how many cases there would be in the future, and thus this model does a great job of predicting the number of cases heading forward. It would be interesting to see if the model still performs well 2-3 weeks from now after social distancing restrictions have been eased the past 1-2 weeks.
# MAGIC 
# MAGIC 
# MAGIC Below are the fitted vs residual plots for the predictions. The first prediction has fitted values that are much lower as it was underestimating the number of cases. This can also  be seen by the fact that the residuals are all positive which indicates the actual values was higher than the predicted value. The second display is for the second model that included the number of cases on previous days. The output was a lot closer as can be seen that the residual range was -150 to 100 rather than -20k to 250k (huge differene). 

# COMMAND ----------

display(lr_model, vtest_final, "fittedVsResiduals")

# COMMAND ----------

display(lr_model2, vtest_final2, "fittedVsResiduals")
