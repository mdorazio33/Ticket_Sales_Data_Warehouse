# Ticket_Sales_Data_Warehouse

This repository contains the Python and SQL code for a Data Warehousing Final Project that I did during the Fall of 2023. I used a combination o Vertabelo, AWS S3, and Databricks to visualize, store, process and transform the data from the original dataset.
The dataset I used for this project was a real-world 172,000 row dataset reflecting ticket sales for an event called Winter Fest OC in Orange County, California and was originally obtained from California Open Data.
Following a Data Lakehouse medallion architecture of bronze, silver, and gold layers, the data was processed and organized from its raw state at the bronze layer, cleaned, filtered, and augmented at the silver layer, and aggregated into business-level tables at
the gold layer.

In other words, the bronze layer contains the messy original data as it existed originally, the silver layer contains the cleaned and filtered data along with the creation of Kimball-style dimensional tables made by splitting into a smaller portion at first
and then updating it with the rest of the dataset later on, and the gold layer contains aggregate tables and figures that help answer business-level questions and analysis such as the total sales by certain affiliates, the total amount of money paid by order location,
the total sales by order location, and sales over time. In order to view these charts easier, I have attached a PDF containing screenshots of them as they appear in the Analysis #1, Analysis #2, and Analysis #3 sections of this project.

Since this project ties in the use of multiple technologies and techniques including the use of delta tables, Apache Spark (PySpark SQL), and Kimball-style dimensional modeling, this repository contains many different attachments. The following is a brief Table of Contents
to better understand what is contained in this repo.

Table of Contents:
1) Database_Model_for_Ticket_Sales_DW.pdf - PDF of the Dimensional Model made to reflect the Dimension and Fact tables modeled in Databricks. This visualization was created using Vertabelo.
2) Ticket_Sales_DW_Visualizaitons.pdf - PDF containing the visualizations created in the Analysis #1, Analysis #2, and Analysis #3 sections of the project using the built-in visualization tools that Databricks provides.
3) Data_Warehousing_Final_Project.ipynb - IPython Notebook containing the Python code and SQL queries used throughout this project.
4) Data_Warehousing_Final_Project.dbc - DBC Archive of the Databricks Notebook.
5) https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5460236223438621/4210893823703454/6201367242108505/latest.html - Published Databricks Notebook link that allows for the notebook to be viewed and interacted with.
6) Data_Warehousing_Final_Project.py - Exported Python Source File for the Databricks Notebook.
7) Data_Warehousing_Final_Project.HTML - Exported HTML File of the Databricks Notebook.
8) sample_data.csv - This CSV file represents the first 100 rows of the raw dataset that was used in this project. The original was 172,000 rows and too large to store on GitHub.

Worth mentioning, the files for this project are fairly large, so if you are viewing the IPython Notebook in this repository directly through GitHub, please give it a few moments to render.
If it every says something like, "Unable to render code block", simply refresh the page and it should render properly.
As for the Data_Warehousing_Final_Project.HTML file, it is recommended to download this one and view it in your browser in order to see the code as it appears in Databricks.
If viewed through GitHub directly or as a raw file, it will not appear as it does in Databricks.
Otherwise, following the link in item 5 will bring you to the same page on your browser as well.
