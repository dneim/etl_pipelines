The skeleton of this project is based on the code-along project completed previously [Duetsche Bank Trading Report]

In this project I apply the coding practices and syntactical approach outlined by the teacher of the previous project [Jan Schwarzlose]

The goal here has been to recreate the general steps he took in creating an ETL pipeline that reads in CSV files, applies transformations,
and uploads the files to a given AWS destination.

As of 02/27/2023 the operational approach is complete. The main digression is in uploading the finalized report as CSV instead of .parquet.
This coice was made for memory purposes, as i did not wish to create a new virtual environment.
