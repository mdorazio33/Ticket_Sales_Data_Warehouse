# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Data Warehousing Final Project

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For this project, I analyzed a dataset that originally had a total of 172,000 rows of ticket sales transactions from various events during Winter Fest OC, a festival held in Orange County, California, meant to celebrate the magic of winter.
# MAGIC
# MAGIC I organized my data through a medallion lakehouse architecture of bronze, silver, and gold layers, and concluded with an analysis alongside visuals. All dimension and fact tables created throughout this project were stored in AWS S3.
# MAGIC
# MAGIC Once fully processed into the gold layer, I created charts and analysis for the following relationships: ticket sales proportions in relation to affiliates, total revenue/ticket sales as they relate to order location, and ticket sales observed over time.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### AWS Mount to S3 Bucket

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/mount

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/mount/fall_2023_finals/TicketSales

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Bronze Layer

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC dbutils.fs.rm("/user/hive/warehouse/fall_2023_finals.db/",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop database if exists fall_2023_finals cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC create database if not exists fall_2023_finals;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use fall_2023_finals

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC drop table if exists bronze_TicketSales_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC create table bronze_TicketSales_csv using csv options(
# MAGIC   path = '/mnt/mount/fall_2023_finals/TicketSales/bronze/raw_data',
# MAGIC   header = 'true',
# MAGIC   inferSchema = 'true',
# MAGIC   delimiter = ','
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC select * from bronze_TicketSales_csv limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC show create table bronze_TicketSales_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fall_2023_finals.bronze_TcktSales_csv (
# MAGIC   event_id BIGINT,
# MAGIC   order_id INT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   email STRING,
# MAGIC   quantity INT,
# MAGIC   ticket_type STRING,
# MAGIC   attendee_id INT,
# MAGIC   buyer_email STRING,
# MAGIC   event_date TIMESTAMP,
# MAGIC   order_city STRING,
# MAGIC   order_state STRING,
# MAGIC   order_country STRING,
# MAGIC   discount STRING,
# MAGIC   affiliate STRING,
# MAGIC   total_paid DOUBLE,
# MAGIC   home_address_1 STRING,
# MAGIC   home_address_2 STRING,
# MAGIC   home_city STRING,
# MAGIC   home_state STRING,
# MAGIC   home_zip INT,
# MAGIC   home_country STRING,
# MAGIC   what_are_you_most_excited_for_at_winter_fest_oc STRING,
# MAGIC   how_did_you_hear_about_winter_fest_oc STRING,
# MAGIC   have_you_attended_winter_fest_oc_before STRING,
# MAGIC   did_you_attend_winter_fest_oc_last_year STRING,
# MAGIC   billing_address_1 STRING,
# MAGIC   billing_address_2 STRING,
# MAGIC   billing_city STRING,
# MAGIC   billing_state STRING,
# MAGIC   billing_country STRING,
# MAGIC   billing_zip INT,
# MAGIC   zip INT)
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'delimiter' = ',',
# MAGIC   'header' = 'true',
# MAGIC   'inferSchema' = 'true')
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/bronze/raw_data'

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC select * from bronze_TcktSales_csv limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists fall_2023_finals.bronze_Tcktsales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fall_2023_finals.bronze_TcktSales_delta (
# MAGIC   event_id BIGINT,
# MAGIC   order_id INT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   email STRING,
# MAGIC   quantity INT,
# MAGIC   ticket_type STRING,
# MAGIC   attendee_id INT,
# MAGIC   buyer_email STRING,
# MAGIC   event_date TIMESTAMP,
# MAGIC   order_city STRING,
# MAGIC   order_state STRING,
# MAGIC   order_country STRING,
# MAGIC   discount STRING,
# MAGIC   affiliate STRING,
# MAGIC   total_paid DOUBLE,
# MAGIC   home_address_1 STRING,
# MAGIC   home_address_2 STRING,
# MAGIC   home_city STRING,
# MAGIC   home_state STRING,
# MAGIC   home_zip INT,
# MAGIC   home_country STRING,
# MAGIC   what_are_you_most_excited_for_at_winter_fest_oc STRING,
# MAGIC   how_did_you_hear_about_winter_fest_oc STRING,
# MAGIC   have_you_attended_winter_fest_oc_before STRING,
# MAGIC   did_you_attend_winter_fest_oc_last_year STRING,
# MAGIC   billing_address_1 STRING,
# MAGIC   billing_address_2 STRING,
# MAGIC   billing_city STRING,
# MAGIC   billing_state STRING,
# MAGIC   billing_country STRING,
# MAGIC   billing_zip INT,
# MAGIC   zip INT)
# MAGIC USING delta
# MAGIC OPTIONS (
# MAGIC   'delimiter' = ',',
# MAGIC   'header' = 'true',
# MAGIC   'inferSchema' = 'true')
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/bronze/delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into fall_2023_finals.bronze_TcktSales_delta 
# MAGIC select * from  bronze_TcktSales_csv

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC At this point, I noticed that an order_location column would prove beneficial for an order_location dimension. I decided to create one using the CONCAT function in SQL to concatenate the values in the order_city, order_state, and order_country columns into a single column.

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table bronze_TcktSales_delta
# MAGIC add column order_location STRING;
# MAGIC
# MAGIC update bronze_TcktSales_delta
# MAGIC set order_location = concat(order_city, ', ', order_state, ', ', order_country);

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC select count(*) from bronze_TcktSales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC select * from bronze_TcktSales_delta limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As seen in the final column, an order_location column now exists thanks to the use of the CONCAT function based on the three columns specified.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Testing for Zeros/Nulls/Corner Cases

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC At this stage, I will test for zeros/nulls/corner cases for columns my future keys would be derived from later on for fact and dimension tables. As a result, the columns I will perform the aforementioned tests for in the bronze layer consist of order_date, event_id, order_id, attendee_id, and order_location. If the columns ending in "id" turn out not to be suitable as primary keys when developing the dimensional model, surrogate keys will utilized in their stead. For now, however, I will still clean the previously mentinoed columns with the zeros/nulls/corner cases tests.
# MAGIC
# MAGIC For date columns a zero test is not necessary due to them being TIMESTAMPs. For column's ending in "id", the corner cases included a test for leading or trailing spaces along with a length test to see if they were out of the expected range.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tests for order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_date is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_date not between '1000-01-01' AND '9999-12-31'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tests for event_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where event_date is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where event_date not between '1000-01-01' AND '9999-12-31'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tests for event_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where event_id = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where event_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where event_id like ' %' OR event_id like '% '

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where length(event_id) > 11

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tests for order_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_id = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_id like ' %' OR order_id like '% '

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where length(order_id) > 9

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tests for order_location

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_location = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_location is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from bronze_TcktSales_delta where order_location is null

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC select count(*) from bronze_TcktSales_delta where order_location is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where order_location like ' %' OR order_id like '% '

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tests for attendee_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where attendee_id = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where attendee_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where attendee_id like ' %' OR attendee_id like '% '

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_TcktSales_delta where length(attendee_id) > 10

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Identifying Redundant/Unnecessary Columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Some of the columns that I can instantly identify are not useful for my analysis are the "what_are_you_most_excited_for_at_winter_fest_oc", "how_did_you_hear_about_winter_fest_oc", "have_you_attended_winter_fest_oc_before", and "did_you_attend_winter_fest_oc_last_year". Also columns relating to billing addresses are not needed for my analysis, so they will be excluded from the silver layer as well. Another set of columns that are questionable due to their similarity to other columns include those relating to emails and zip codes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### email and buyer_email columns

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from bronze_TcktSales_delta where buyer_email is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from bronze_TcktSales_delta where email is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from bronze_TcktSales_delta where email = buyer_email

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As shown from the updated count of the bronze_TcktSales_delta, the total number of rows that are not null for both email and buyer_email is 159,462. Out of that, 157,086 of those rows consist of matching values between email and buyer_email. To figure out the exact percentage of this in order to assess whether or not keeping the two is redundant, I wrote the following Python script.

# COMMAND ----------

x = 157086
y = 159462
match_percentage = (x/y)*100
print("The percentage of matching values between the email and buyer email column is", match_percentage, '%')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This indicates that roughly, 98.5% of emails between these two columns are matching, indicating that much of the data may prove redundant in later tables as the intention of both columns appeared to be to store the same information apart from a small amount of differences (1.5%). As a result, I have decided that it is not necessary to include both in the silver level tables and will only keep one with that being the "email" column.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### anonymized email rows

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Also although it has nothing to do with cleaning data for use alongside potential key columns, I decided to remove anonymized data in the email/buyer_email columns. The reason for this is out of personal choice, as I wanted to only retain email addresses that were fully viewable for use in the silver layer and beyond.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from bronze_Tcktsales_delta where email like "%*%"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This removed four rows with email addresses containing the "*" symbol.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### home_zip and zip columns

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from bronze_TcktSales_delta where zip is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from bronze_TcktSales_delta where home_zip is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from bronze_TcktSales_delta where zip = home_zip

# COMMAND ----------

x = 34087
y = 35446
match_percentage = (x/y)*100
print("The percentage of matching values between the zip and buyer zip column is", match_percentage, '%')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Given that 96% of these values match, it can be concluded that for the most part, they represent the same information. Meaning that both do not need to be included in the silver level tables.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Based on this, what I will do is pass data from the zip column to the silver layer because it contains more populated data and use it later on when creating the attendee_dimension.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Final count of rows in bronze delta table following data cleaning

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from bronze_TcktSales_delta

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC drop table if exists fall_2023_finals.silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fall_2023_finals.silver_TcktSales_delta_part1 (
# MAGIC   attendee_id INT,
# MAGIC   event_id BIGINT,
# MAGIC   order_id INT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   email STRING,
# MAGIC   quantity INT,
# MAGIC   ticket_type STRING,
# MAGIC   event_date TIMESTAMP,
# MAGIC   order_location STRING,
# MAGIC   order_city STRING,
# MAGIC   order_state STRING,
# MAGIC   order_country STRING,
# MAGIC   discount STRING,
# MAGIC   affiliate STRING,
# MAGIC   total_paid DOUBLE,
# MAGIC   home_address_1 STRING,
# MAGIC   home_address_2 STRING,
# MAGIC   home_city STRING,
# MAGIC   home_state STRING,
# MAGIC   home_country STRING,
# MAGIC   zip INT)
# MAGIC USING delta
# MAGIC OPTIONS (
# MAGIC   'delimiter' = ',',
# MAGIC   'header' = 'true',
# MAGIC   'inferSchema' = 'true')
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/part1'

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC drop table if exists fall_2023_finals.silver_TcktSales_delta_part2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fall_2023_finals.silver_TcktSales_delta_part2 (
# MAGIC   attendee_id INT,
# MAGIC   event_id BIGINT,
# MAGIC   order_id INT,
# MAGIC   order_date TIMESTAMP,
# MAGIC   email STRING,
# MAGIC   quantity INT,
# MAGIC   ticket_type STRING,
# MAGIC   event_date TIMESTAMP,
# MAGIC   order_location STRING,
# MAGIC   order_city STRING,
# MAGIC   order_state STRING,
# MAGIC   order_country STRING,
# MAGIC   discount STRING,
# MAGIC   affiliate STRING,
# MAGIC   total_paid DOUBLE,
# MAGIC   home_address_1 STRING,
# MAGIC   home_address_2 STRING,
# MAGIC   home_city STRING,
# MAGIC   home_state STRING,
# MAGIC   home_country STRING,
# MAGIC   zip INT)
# MAGIC USING delta
# MAGIC OPTIONS (
# MAGIC   'delimiter' = ',',
# MAGIC   'header' = 'true',
# MAGIC   'inferSchema' = 'true')
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/part2'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into silver_TcktSales_delta_part1 (
# MAGIC     attendee_id, event_id, order_id, order_date, email, quantity,
# MAGIC     ticket_type, event_date, order_location,
# MAGIC     order_city, order_state, order_country, discount,
# MAGIC     affiliate, total_paid, home_address_1, home_address_2,
# MAGIC     home_city, home_state, home_country, zip
# MAGIC )
# MAGIC select
# MAGIC     attendee_id, event_id, order_id, order_date, email, quantity,
# MAGIC     ticket_type, event_date, order_location,
# MAGIC     order_city, order_state, order_country, discount,
# MAGIC     affiliate, total_paid, home_address_1, home_address_2,
# MAGIC     home_city, home_state, home_country, zip
# MAGIC from bronze_TcktSales_delta
# MAGIC limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into silver_TcktSales_delta_part2 (
# MAGIC     attendee_id, event_id, order_id, order_date, email, quantity,
# MAGIC     ticket_type, event_date, order_location,
# MAGIC     order_city, order_state, order_country, discount,
# MAGIC     affiliate, total_paid, home_address_1, home_address_2,
# MAGIC     home_city, home_state, home_country, zip
# MAGIC )
# MAGIC select
# MAGIC     attendee_id, event_id, order_id, order_date, email, quantity,
# MAGIC     ticket_type, event_date, order_location,
# MAGIC     order_city, order_state, order_country, discount,
# MAGIC     affiliate, total_paid, home_address_1, home_address_2,
# MAGIC     home_city, home_state, home_country, zip
# MAGIC from bronze_TcktSales_delta
# MAGIC offset 100;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Viewing first 5 rows of both Silver tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver_TcktSales_delta_part1 limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from silver_TcktSales_delta_part2 limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating Dimension and Fact Tables from Silver Table - Part 1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC All fact and dimension tables created at this stage are stored in AWS S3.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Affiliate

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dim_affiliate

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_affiliate (
# MAGIC affiliate_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC affiliate STRING,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimaffiliate'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In order to handle null values and treat them as instances where no affiliate was involved, I used the COALESCE function to handle them appropritely and rename those instances to "No Affiliate"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into dim_affiliate(affiliate,last_updated_on) select distinct coalesce(affiliate, 'No Affiliate'), CURRENT_TIMESTAMP() from silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_affiliate limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Ticket Type

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dim_ticket_type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_ticket_type (
# MAGIC ticket_type_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC ticket_type STRING,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimtickettype'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into dim_ticket_type(ticket_type,last_updated_on) select distinct ticket_type, CURRENT_TIMESTAMP() from silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_ticket_type limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Event

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists dim_event

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_event (
# MAGIC event_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC event_number BIGINT,
# MAGIC event_date TIMESTAMP,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimevent'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into dim_event(event_number, event_date, last_updated_on) select distinct event_id, event_date, CURRENT_TIMESTAMP() from silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_event limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Order Location

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dim_order_location

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_order_location (
# MAGIC order_location_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC order_location STRING,
# MAGIC order_city STRING,
# MAGIC order_state STRING,
# MAGIC order_country STRING,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimorderlocation'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into dim_order_location(order_location, order_city, order_state, order_country, last_updated_on) select distinct order_location, order_city, order_state, order_country, CURRENT_TIMESTAMP() from silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_order_location limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Attendee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists dim_attendee

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_attendee (
# MAGIC attendee_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC attendee_number INT,
# MAGIC email STRING,
# MAGIC home_address_1 STRING,
# MAGIC home_address_2 STRING,
# MAGIC home_city STRING,
# MAGIC home_state STRING,
# MAGIC home_country STRING,
# MAGIC zip INT,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimattendee'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into dim_attendee(attendee_number, email, home_address_1, home_address_2, home_city, home_state, home_country, zip, last_updated_on) select distinct attendee_id, email, home_address_1, home_address_2, home_city, home_state, home_country, zip, CURRENT_TIMESTAMP() from silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_attendee limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists dim_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_date (
# MAGIC date_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC order_date TIMESTAMP,
# MAGIC shortdate STRING,
# MAGIC day INT,
# MAGIC day_name STRING,
# MAGIC month INT,
# MAGIC month_name STRING,
# MAGIC week_of_month INT,
# MAGIC year INT,
# MAGIC week_of_year INT,
# MAGIC quarter_number INT,
# MAGIC is_weekend BOOLEAN,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimdate'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dim_date (order_date, shortdate, day, day_name, month, month_name, week_of_month, year, week_of_year, quarter_number, is_weekend, last_updated_on)
# MAGIC select distinct
# MAGIC   order_date as order_date,
# MAGIC   date_format(order_date, 'MM-dd-yyyy') as shortdate,
# MAGIC   day(order_date) as day,
# MAGIC   date_format(order_date, 'EEEE') as day_name,
# MAGIC   month(order_date) as month,
# MAGIC   date_format(order_date, 'MMMM') as month_name,
# MAGIC   floor((day(order_date) - 1) / 7) + 1 as week_of_month,
# MAGIC   year(order_date) as year,
# MAGIC   weekofyear(order_date) as week_of_year,
# MAGIC   quarter(order_date) as quarter_number,
# MAGIC   dayofweek(order_date) in (1, 7) as is_weekend,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC FROM
# MAGIC   silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_date limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fact - Order Transaction

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC At this stage, I was testing the data for which I would use to build the fact table by first creating a regular table called fact_test prior to creating my external fact table in AWS S3. In order to prevent nulls in my fact table, I used the COALESCE function to handle them appropriately.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists fact_test

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table fact_test as
# MAGIC select distinct 
# MAGIC   e.event_id,
# MAGIC   a.attendee_id,
# MAGIC   coalesce(af.affiliate_id, 3) as affiliate_id,
# MAGIC   ord.order_location_id,
# MAGIC   tk.ticket_type_id,
# MAGIC   d.date_id,
# MAGIC   s.order_id,
# MAGIC   coalesce(s.discount, 'No Discount') as discount,
# MAGIC   s.total_paid,
# MAGIC   s.quantity,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC from
# MAGIC   silver_TcktSales_delta_part1 s
# MAGIC   left join dim_attendee a on s.attendee_id = a.attendee_number
# MAGIC   left join dim_event e on s.event_id = e.event_number
# MAGIC   left join dim_affiliate af on s.affiliate = af.affiliate
# MAGIC   left join dim_order_location ord on s.order_location = ord.order_location
# MAGIC   left join dim_ticket_type tk on s.ticket_type = tk.ticket_type
# MAGIC   left join dim_date d on s.order_date = d.order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_test limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC describe fact_test

# COMMAND ----------

# MAGIC %md
# MAGIC After this information was acquired, I created my external fact table in a similar way.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists fact_order_transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table fact_order_transaction (
# MAGIC order_transaction_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC event_id BIGINT,
# MAGIC attendee_id INT,
# MAGIC affiliate_id BIGINT,
# MAGIC order_location_id BIGINT,
# MAGIC ticket_type_id BIGINT,
# MAGIC date_id STRING,
# MAGIC order_id INT,
# MAGIC discount STRING,
# MAGIC total_paid DOUBLE,
# MAGIC quantity INT,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/factorder'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into fact_order_transaction
# MAGIC (event_id, attendee_id, affiliate_id, order_location_id, ticket_type_id, date_id, order_id, discount, total_paid, quantity, last_updated_on)
# MAGIC select distinct 
# MAGIC   e.event_id,
# MAGIC   a.attendee_id,
# MAGIC   COALESCE(af.affiliate_id, 3) as affiliate_id,
# MAGIC   ord.order_location_id,
# MAGIC   tk.ticket_type_id,
# MAGIC   d.date_id,
# MAGIC   s.order_id,
# MAGIC   COALESCE(s.discount, 'No Discount') as discount,
# MAGIC   s.total_paid,
# MAGIC   s.quantity,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC from
# MAGIC   silver_TcktSales_delta_part1 s
# MAGIC   left join dim_attendee a on s.attendee_id = a.attendee_number
# MAGIC   left join dim_event e on s.event_id = e.event_number
# MAGIC   left join dim_affiliate af on s.affiliate = af.affiliate
# MAGIC   left join dim_order_location ord on s.order_location = ord.order_location
# MAGIC   left join dim_ticket_type tk on s.ticket_type = tk.ticket_type
# MAGIC   left join dim_date d on s.order_date = d.order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from fact_order_transaction limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Month

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dim_month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_month (
# MAGIC month_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC order_date TIMESTAMP,
# MAGIC month INT,
# MAGIC month_name STRING,
# MAGIC week_of_month INT,
# MAGIC year INT,
# MAGIC quarter_number INT,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimmonth'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dim_month (order_date, month, month_name, week_of_month, year, quarter_number, last_updated_on)
# MAGIC select distinct
# MAGIC   order_date as order_date,
# MAGIC   month(order_date) as month,
# MAGIC   date_format(order_date, 'MMMM') as month_name,
# MAGIC   floor((day(order_date) - 1) / 7) + 1 as week_of_month,
# MAGIC   year(order_date) as year,
# MAGIC   quarter(order_date) as quarter_number,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC FROM
# MAGIC   silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_month limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fact - Monthly Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists fact_monthly_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table fact_monthly_sales (
# MAGIC monthly_sales_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC month_id BIGINT,
# MAGIC discount STRING,
# MAGIC total_paid DOUBLE,
# MAGIC quantity INT,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/factmonth'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into fact_monthly_sales
# MAGIC (month_id, discount, total_paid, quantity, last_updated_on)
# MAGIC select distinct
# MAGIC   m.month_id,
# MAGIC   COALESCE(s.discount, 'No Discount') as discount,
# MAGIC   s.total_paid,
# MAGIC   s.quantity,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC from
# MAGIC   silver_TcktSales_delta_part1 s
# MAGIC   left join dim_month m on s.order_date = m.order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_monthly_sales limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Week

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dim_week

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table dim_week (
# MAGIC week_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC order_date TIMESTAMP,
# MAGIC week_of_month INT,
# MAGIC year INT,
# MAGIC quarter_number INT,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/dimweek'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dim_week (order_date, week_of_month, year, quarter_number, last_updated_on)
# MAGIC select distinct
# MAGIC   order_date as order_date,
# MAGIC   floor((day(order_date) - 1) / 7) + 1 as week_of_month,
# MAGIC   year(order_date) as year,
# MAGIC   quarter(order_date) as quarter_number,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC FROM
# MAGIC   silver_TcktSales_delta_part1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dim_week limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fact - Weekly Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists fact_weekly_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table fact_weekly_sales (
# MAGIC weekly_sales_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC week_id BIGINT,
# MAGIC discount STRING,
# MAGIC total_paid DOUBLE,
# MAGIC quantity INT,
# MAGIC last_updated_on TIMESTAMP
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/silver/factweek'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into fact_weekly_sales
# MAGIC (week_id, discount, total_paid, quantity, last_updated_on)
# MAGIC select distinct
# MAGIC   w.week_id,
# MAGIC   COALESCE(s.discount, 'No Discount') as discount,
# MAGIC   s.total_paid,
# MAGIC   s.quantity,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC from
# MAGIC   silver_TcktSales_delta_part1 s
# MAGIC   left join dim_week w on s.order_date = w.order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_weekly_sales limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Merging Dimension & Fact Tables with Silver Table Data - Part 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension - Affiliate

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_newaffiliate_temp as 
# MAGIC   select affiliate,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(affiliate) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select affiliate from dim_affiliate 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_newaffiliate_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_affiliate a using vw_newaffiliate_temp ta
# MAGIC on a.affiliate = ta.affiliate
# MAGIC when not matched then insert (affiliate,last_updated_on) values(ta.affiliate,ta.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_affiliate limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension - Ticket Type

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_newticket_type_temp as 
# MAGIC   select ticket_type,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(ticket_type) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select ticket_type from dim_ticket_type 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_newticket_type_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_ticket_type t using vw_newticket_type_temp tt
# MAGIC on t.ticket_type = tt.ticket_type
# MAGIC when not matched then insert (ticket_type,last_updated_on) values(tt.ticket_type,tt.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_ticket_type limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension - Event

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_newevent_temp as 
# MAGIC   select event_id,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(event_id) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select event_number from dim_event 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_newevent_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_event e using vw_newevent_temp et
# MAGIC on e.event_number = et.event_id
# MAGIC when not matched then insert (event_number,last_updated_on) values(et.event_id,et.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_event limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Order Location

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_neworder_temp as 
# MAGIC   select order_location,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(order_location) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select order_location from dim_order_location 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_neworder_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_order_location ord using vw_neworder_temp tor
# MAGIC on ord.order_location = tor.order_location
# MAGIC when not matched then insert (order_location,last_updated_on) values(tor.order_location,tor.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_order_location limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Attendee

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_newattendee_temp as 
# MAGIC   select attendee_id,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(attendee_id) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select attendee_number from dim_attendee 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_newattendee_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_attendee att using vw_newattendee_temp tatt
# MAGIC on att.attendee_number = tatt.attendee_id
# MAGIC when not matched then insert (attendee_number,last_updated_on) values(tatt.attendee_id,tatt.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_attendee limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension - Date

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_newdate_temp as 
# MAGIC   select order_date,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(order_date) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select order_date from dim_date 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_newdate_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_date d using vw_newdate_temp nd
# MAGIC on d.order_date = nd.order_date
# MAGIC when not matched then insert (order_date,last_updated_on) values(nd.order_date,nd.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_date limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fact - Order Transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_order_transaction as
# MAGIC select distinct
# MAGIC   e.event_id,
# MAGIC   a.attendee_id,
# MAGIC   COALESCE(af.affiliate_id, 3) as affiliate_id,
# MAGIC   ord.order_location_id,
# MAGIC   tk.ticket_type_id,
# MAGIC   d.date_id,
# MAGIC   s.order_id,
# MAGIC   COALESCE(s.discount, 'No Discount') as discount,
# MAGIC   s.total_paid,
# MAGIC   s.quantity,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC from
# MAGIC   silver_TcktSales_delta_part2 s
# MAGIC   left join dim_attendee a on s.attendee_id = a.attendee_number
# MAGIC   left join dim_event e on s.event_id = e.event_number
# MAGIC   left join dim_affiliate af on s.affiliate = af.affiliate
# MAGIC   left join dim_order_location ord on s.order_location = ord.order_location
# MAGIC   left join dim_ticket_type tk on s.ticket_type = tk.ticket_type
# MAGIC   left join dim_date d on s.order_date = d.order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_order_transaction limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into fact_order_transaction f using vw_order_transaction t
# MAGIC on f.event_id = t.event_id AND f.attendee_id = t.attendee_id and f.affiliate_id = t.affiliate_id and f.order_location_id = t.order_location_id 
# MAGIC and f.ticket_type_id = t.ticket_type_id and f.date_id = t.date_id
# MAGIC when matched then 
# MAGIC update set
# MAGIC   f.event_id = t.event_id
# MAGIC   ,f.attendee_id = t.attendee_id
# MAGIC   ,f.affiliate_id = t.affiliate_id
# MAGIC   ,f.order_location_id = t.order_location_id
# MAGIC   ,f.ticket_type_id = t.ticket_type_id
# MAGIC   ,f.date_id = t.date_id
# MAGIC   ,f.order_id = t.order_id
# MAGIC   ,f.discount = t.discount
# MAGIC   ,f.total_paid = t.total_paid
# MAGIC   ,f.quantity = t.quantity
# MAGIC   ,f.last_updated_on = t.last_updated_on
# MAGIC
# MAGIC when not matched then insert
# MAGIC   (
# MAGIC     event_id
# MAGIC     ,attendee_id
# MAGIC     ,affiliate_id
# MAGIC     ,order_location_id
# MAGIC     ,ticket_type_id
# MAGIC     ,date_id
# MAGIC     ,order_id
# MAGIC     ,discount
# MAGIC     ,total_paid
# MAGIC     ,quantity
# MAGIC     ,last_updated_on
# MAGIC   ) values
# MAGIC   (
# MAGIC     t.event_id
# MAGIC     ,t.attendee_id
# MAGIC     ,t.affiliate_id
# MAGIC     ,t.order_location_id
# MAGIC     ,t.ticket_type_id
# MAGIC     ,t.date_id
# MAGIC     ,t.order_id
# MAGIC     ,t.discount
# MAGIC     ,t.total_paid
# MAGIC     ,t.quantity
# MAGIC     ,t.last_updated_on
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_order_transaction limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Month

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_newmonth_temp as 
# MAGIC   select order_date,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(order_date) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select order_date from dim_month 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_newmonth_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_month m using vw_newmonth_temp nm
# MAGIC on m.order_date = nm.order_date
# MAGIC when not matched then insert (order_date,last_updated_on) values(nm.order_date,nm.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_month limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fact - Monthly Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_monthly_sales as
# MAGIC select distinct
# MAGIC   m.month_id,
# MAGIC   COALESCE(s.discount, 'No Discount') as discount,
# MAGIC   s.total_paid,
# MAGIC   s.quantity,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC from
# MAGIC   silver_TcktSales_delta_part2 s
# MAGIC   left join dim_month m on s.order_date = m.order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_monthly_sales limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into fact_monthly_sales f using vw_monthly_sales t
# MAGIC on f.month_id = t.month_id and f.discount = t.discount and f.total_paid = t.total_paid and f.quantity = t.quantity and f.last_updated_on = t.last_updated_on
# MAGIC when matched then 
# MAGIC   update set
# MAGIC     f.month_id = t.month_id,
# MAGIC     f.discount = t.discount,
# MAGIC     f.total_paid = t.total_paid,
# MAGIC     f.quantity = t.quantity,
# MAGIC     f.last_updated_on = t.last_updated_on
# MAGIC when not matched then 
# MAGIC   insert (
# MAGIC     month_id,
# MAGIC     discount,
# MAGIC     total_paid,
# MAGIC     quantity,
# MAGIC     last_updated_on
# MAGIC   ) values (
# MAGIC     t.month_id,
# MAGIC     t.discount,
# MAGIC     t.total_paid,
# MAGIC     t.quantity,
# MAGIC     t.last_updated_on
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_monthly_sales limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Dimension - Week

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_newweek_temp as 
# MAGIC   select order_date,current_timestamp() as last_updated_on from (
# MAGIC     select distinct(order_date) from fall_2023_finals.silver_TcktSales_delta_part2
# MAGIC     minus
# MAGIC     select order_date from dim_week 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_newweek_temp limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into dim_week w using vw_newweek_temp nw
# MAGIC on w.order_date = nw.order_date
# MAGIC when not matched then insert (order_date,last_updated_on) values(nw.order_date,nw.last_updated_on)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_week limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fact - Weekly Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view vw_weekly_sales as
# MAGIC select distinct
# MAGIC   w.week_id,
# MAGIC   COALESCE(s.discount, 'No Discount') as discount,
# MAGIC   s.total_paid,
# MAGIC   s.quantity,
# MAGIC   CURRENT_TIMESTAMP() as last_updated_on
# MAGIC from
# MAGIC   silver_TcktSales_delta_part2 s
# MAGIC   left join dim_week w on s.order_date = w.order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_weekly_sales limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into fact_weekly_sales f using vw_weekly_sales t
# MAGIC on f.week_id = t.week_id and f.discount = t.discount and f.total_paid = t.total_paid and f.quantity = t.quantity and f.last_updated_on = t.last_updated_on
# MAGIC when matched then 
# MAGIC   update set
# MAGIC     f.week_id = t.week_id,
# MAGIC     f.discount = t.discount,
# MAGIC     f.total_paid = t.total_paid,
# MAGIC     f.quantity = t.quantity,
# MAGIC     f.last_updated_on = t.last_updated_on
# MAGIC when not matched then 
# MAGIC   insert (
# MAGIC     week_id,
# MAGIC     discount,
# MAGIC     total_paid,
# MAGIC     quantity,
# MAGIC     last_updated_on
# MAGIC   ) values (
# MAGIC     t.week_id,
# MAGIC     t.discount,
# MAGIC     t.total_paid,
# MAGIC     t.quantity,
# MAGIC     t.last_updated_on
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact_weekly_sales limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC At this stage, the data merging/updating aspect for the silver layer was complete.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Gold Layer & Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC At this stage, the gold layer aggregation tables are created along with three forms of meaningful analysis & charts.

# COMMAND ----------

# MAGIC %md
# MAGIC In developing three aggregated gold layer analysis tables that are derived from the fact and dimension tables, insights at the business-level can be successfully derived from the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis #1 - Ticket Sales by Affiliate
# MAGIC
# MAGIC For my first gold level analysis, I wanted to create a table that analyzes the relation between affiliate and total sales. From a business standpoint, this analysis could prove beneficial in understanding which promoters are the best at contributing towards overall ticket sales.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists analysis1_salesbyaffiliate

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table analysis1_salesbyaffiliate (
# MAGIC   sales_by_affiliate_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   affiliate_id BIGINT,
# MAGIC   total_sales DOUBLE
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/gold/analysis1'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into analysis1_salesbyaffiliate(affiliate_id,total_sales)
# MAGIC select
# MAGIC   COALESCE(af.affiliate_id, 3) as affiliate_id,
# MAGIC   ROUND(SUM(total_paid), 2) as total_sales
# MAGIC from
# MAGIC   fact_order_transaction f
# MAGIC   left join dim_affiliate af on f.affiliate_id = af.affiliate_id
# MAGIC group by
# MAGIC   COALESCE(af.affiliate_id, 3);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analysis1_salesbyaffiliate limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC At this point, I wanted to develop a Pie Chart that showed the total sales of the top 10 affiliates in proportion to one another.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct affiliate_id, total_sales from analysis1_salesbyaffiliate order by total_sales desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC In order to find out specifically which affiliates were among the top three highest contributors in the Pie Chart, I wrote the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct f.affiliate_id, a.affiliate as affiliate_name from
# MAGIC   fact_order_transaction f
# MAGIC   left join dim_affiliate a on f.affiliate_id = a.affiliate_id
# MAGIC where
# MAGIC   f.affiliate_id in (4, 3, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As a result of this query, the conclusion is that the best affiliates when it comes to making salse are echckt and BoxOffice. However, when it comes to direct rankings, the second highest sales were derived from tickets purchased through no affiliate at all. Thus making the cumulative ranking for total ticket sales as follows: 1. echckt 2. No Affiliate 3. BoxOffice

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I also wanted to create a Bar Chart for this table to capture important data (affiliate_id 4, 3, and 2), so I wrote the following query to isolate the x-axis where affiliate_id is between 1 and 5.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct affiliate_id, total_sales from analysis1_salesbyaffiliate where affiliate_id between 1 and 5;

# COMMAND ----------

# MAGIC %md
# MAGIC With the Bar Chart, one can more easiliy see the top three affiliates that contributed most towards total sales, being affiliate_id 4, 3, and 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis #2 - Number of Orders/Sales by Order Location 
# MAGIC
# MAGIC For my second gold level analysis, I wanted to create a table to analyze the numbers of total revenue and sales in relation to the order location. I figured this would be beneficial as it would allow a business to understand the specific locations where most of their sales are coming from and plan advertisements acccordingly.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists analysis2_salesbylocation

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table analysis2_salesbylocation (
# MAGIC   sales_by_location_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   order_location_id BIGINT,
# MAGIC   total_paid DOUBLE,
# MAGIC   total_sales INT
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/gold/analysis2';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into analysis2_salesbylocation(order_location_id, total_paid, total_sales)
# MAGIC select
# MAGIC   order_location_id,
# MAGIC   ROUND(SUM(total_paid), 2) as total_paid,
# MAGIC   SUM(quantity) as total_sales
# MAGIC from
# MAGIC   fact_order_transaction
# MAGIC group by
# MAGIC   order_location_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analysis2_salesbylocation limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct order_location_id, total_paid, total_sales from analysis2_salesbylocation where order_location_id between 1 and 20;

# COMMAND ----------

# MAGIC %md
# MAGIC Among the order_location_ids reflected on the Bar Chart, those with order_location_ids of 1, 4, and 6 for this sample greatly stood out to me as they were the top three earners both in terms of total paid and total sales. In order to find out exactly what those locations were, I wrote the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC select f.order_location_id, o.order_location, f.total_paid, f.total_sales from analysis2_salesbylocation f
# MAGIC join
# MAGIC   dim_order_location o on f.order_location_id = o.order_location_id
# MAGIC where
# MAGIC   f.order_location_id in (1, 4, 6);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC From this, we can conclude that among the snapshot of locations between 1 and 20, Costa Mesa was the order location that had the highest amount of total ticket sales along with revenue, followed by Hacienda Heights and Los Angeles, respectively.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis #3 - Sales Over Time Throughout a Given Time Period
# MAGIC
# MAGIC For my third gold level analysis, I wanted to analyze how sales developed overtime leading up to the Christmas holiday. Since the dataset I analyzed was for Winter Fest OC, I assumed ticket sales leading up to the Winter/Christmas season holiday would be interesting to create a visual for in order to observe how sales developed.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists analysis3_salesovertime

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table analysis3_salesovertime (
# MAGIC   sales_over_time_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   date_id BIGINT,
# MAGIC   shortdate STRING,
# MAGIC   tickets_sold INT
# MAGIC )
# MAGIC using delta
# MAGIC LOCATION 'dbfs:/mnt/mount/fall_2023_finals/TicketSales/gold/analysis3';

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into analysis3_salesovertime(date_id, shortdate, tickets_sold)
# MAGIC select distinct
# MAGIC   d.date_id,
# MAGIC   d.shortdate,
# MAGIC   SUM(f.quantity) over (order by d.date_id) as tickets_sold
# MAGIC from
# MAGIC   dim_date d
# MAGIC left join
# MAGIC   fact_order_transaction f on d.date_id = f.date_id
# MAGIC where
# MAGIC   d.shortdate is not NULL
# MAGIC order by
# MAGIC   d.date_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analysis3_salesovertime limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analysis3_salesovertime

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC From the available shortdates where sales occurred, a line chart was developed for a time period that spanned over roughly one month from November 12th to December 24th. As we can see, the number of tickets sold from November 12th to December 12th is roughly the same. The major jump in ticket sales occurs on the dates leading up to Christmas, with the most sales occurring on December 23rd and December 24th, respectively. This seems to indicate that many individuals may choose to purchase tickets closer to Christmas or at the entrance of an event rather than ordering them ahead of time.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### UNIT Testing

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Lastly, at this stage, some UNIT testing was performed on the silver and gold layer tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNIT Testing - Silver Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Checking row counts.

# COMMAND ----------


assert spark.table("silver_TcktSales_delta_part1").count() <= 100, "number of rows expected doesnt match"

# COMMAND ----------


assert spark.table("silver_TcktSales_delta_part2").count() <= 159358, "number of rows expected doesnt match"

# COMMAND ----------

# MAGIC %md
# MAGIC Just to be safe, in addition to my actual silver tables, I wanted to check my fact tables to ensure none of them exceeded the maximum possible number of rows from the combined part 1 and 2 silver tables.

# COMMAND ----------


assert spark.table("fact_order_transaction").count() <= 159458, "number of rows expected doesnt match"

# COMMAND ----------


assert spark.table("fact_weekly_sales").count() <= 159458, "number of rows expected doesnt match"

# COMMAND ----------


assert spark.table("fact_monthly_sales").count() <= 159458, "number of rows expected doesnt match"

# COMMAND ----------

# MAGIC %md
# MAGIC They all passed. Back to the testing the actual part 1 and part 2 silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if table exists.

# COMMAND ----------


assert spark.table("silver_TcktSales_delta_part1"), "Table silver_TcktSales_delta_part1 does not exists"

# COMMAND ----------


assert spark.table("silver_TcktSales_delta_part2"), "Table silver_TcktSales_delta_part2 does not exists"

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for expected columns.

# COMMAND ----------


assert spark.table("silver_TcktSales_delta_part1").columns == ['attendee_id', 'event_id', 'order_id', 'order_date', 'email', 'quantity', 'ticket_type', 'event_date', 'order_location',
 'order_city', 'order_state', 'order_country', 'discount', 'affiliate', 'total_paid', 'home_address_1', 'home_address_2', 'home_city', 'home_state', 'home_country', 'zip'], "columns missing or not in right order"

# COMMAND ----------


assert spark.table("silver_TcktSales_delta_part2").columns == ['attendee_id', 'event_id', 'order_id', 'order_date', 'email', 'quantity', 'ticket_type', 'event_date', 'order_location',
 'order_city', 'order_state', 'order_country', 'discount', 'affiliate', 'total_paid', 'home_address_1', 'home_address_2', 'home_city', 'home_state', 'home_country', 'zip'], "columns missing or not in right order"

# COMMAND ----------

# MAGIC %md
# MAGIC The silver tables passed each of the unit tests performed on them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNIT Testing - Gold Layer Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Checking row counts.

# COMMAND ----------


assert spark.table("analysis1_salesbyaffiliate").count() <= 159458, "number of rows expected doesnt match"

# COMMAND ----------


assert spark.table("analysis2_salesbylocation").count() <= 159458, "number of rows expected doesnt match"

# COMMAND ----------


assert spark.table("analysis3_salesovertime").count() <= 159458, "number of rows expected doesnt match"

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if tables exist.

# COMMAND ----------


assert spark.table("analysis1_salesbyaffiliate"), "Table analysis1_salesbyaffiliate does not exists"

# COMMAND ----------


assert spark.table("analysis2_salesbylocation"), "Table analysis2_salesbylocation does not exists"

# COMMAND ----------


assert spark.table("analysis3_salesovertime"), "Table analysis3_salesovertime does not exists"

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for nulls.

# COMMAND ----------


from pyspark.sql.functions import col

df = spark.table("analysis1_salesbyaffiliate")

for column in ["affiliate_id", "total_sales"]:
    null_count = df.filter(col(column).isNull()).count()
    assert null_count == 0, f"Column {column} contains {null_count} null values"

# COMMAND ----------


from pyspark.sql.functions import col

df = spark.table("analysis2_salesbylocation")

for column in ["order_location_id", "total_sales", "total_quantity"]:
    null_count = df.filter(col(column).isNull()).count()
    assert null_count == 0, f"Column {column} contains {null_count} null values"

# COMMAND ----------


from pyspark.sql.functions import col

df = spark.table("analysis3_salesovertime")

for column in ["date_id", "shortdate", "tickets_sold"]:
    null_count = df.filter(col(column).isNull()).count()
    assert null_count == 0, f"Column {column} contains {null_count} null values"

# COMMAND ----------

# MAGIC %md
# MAGIC The gold tables passed each of the unit tests performed on them.
