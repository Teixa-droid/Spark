# Spark

## Part 1: Calculate Average Sentiment by App

This part calculates the average sentiment polarity for each app by cleaning and casting the "Sentiment_Polarity" column in the user reviews data.
## Part 2: Find Best Rated Apps and Save to CSV

It filters the apps with a rating of 4.0 or higher from the Google Play Store data and orders them by rating in descending order. The resulting DataFrame contains the best-rated apps.
## Part 3: Clean and Transform Data

This section performs data cleaning and transformation on the Google Play Store data. It does the following:
- Splits the "Genres" column into an array of genres.
- Casts the "Rating" column to float.
- Casts the "Reviews" column to long.
- Processes the "Price" column to remove "$" and apply a 10% discount.
- Converts the "Size" column to kilobytes if it contains 'k' or 'K' and removes any non-numeric characters.
- Converts the "Last Updated" column to a date format.
- Renames certain columns.
- Aggregates the data by grouping it by the "App" column.
## Part 4: Combine Exercise 1 and Exercise 3 DataFrames
This part combines the DataFrames generated in Part 1 and Part 3 using a left join based on the "App" column.
## Part 5: Save Combined DataFrame as Parquet
It saves the combined DataFrame as a Parquet file with gzip compression.
## Part 6: Calculate Metrics by Genre and Save as Parquet

 This part explodes the "Genres" array column to create one row per genre. Then it groups by genre and calculates metrics like the number of applications, average rating, and average sentiment polarity for each genre. Finally, it saves the resulting DataFrame as a Parquet file with gzip compression.
