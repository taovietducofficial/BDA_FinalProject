# ANALYZING AND VISUALIZING DATA WITH POWER BI

<p align="center">
  <img src="https://github.com/taovietducofficial/BDA_FinalProject/blob/master/1.jpg" width="1000" />
</p>

The project on analyzing traffic accidents in the U.S. and visualizing data with Power BI is designed to examine various factors related to accidents, such as location, time, weather conditions, and severity levels occurring daily. By evaluating and processing real-world data, the project aims to provide deep insights into the causes and conditions that contribute to higher accident rates, thereby proposing solutions to minimize risks.

The current context highlights that climate change, extreme weather, and human-related factors such as distraction and non-compliance with traffic safety regulations are the main causes of the increasing number of accidents. Therefore, the project focuses on applying data analysis and visualization methods to identify high-risk areas, seasonal accident trends, and environmental conditions that impact road safety.

The project’s outcomes will serve as a crucial database to support authorities in:
- Enhancing traffic management efficiency and improving road safety.
- Planning road maintenance and infrastructure upgrades based on weather factors and regional characteristics.
- Promoting awareness and educating the public on traffic safety to reduce human-related causes of accidents.

## Target

The objective of the project is to standardize, analyze, and visualize data. We will use Jupyter Lab and Power BI as our main tools. First, we will preprocess and analyze the data based on factors such as time, weather, location, and infrastructure. Then, Power BI will be used to visualize the data through various charts and graphs.

## Tasks
- Data Management and Preprocessing: Collect, clean, and preprocess data.
- Accident Analysis: Analyze factors influencing traffic accidents.
- Data Visualization: Create reports and web interfaces to present analysis results.
- System Deployment: Ensure the data processing and analysis system operates stably.

## TECHNOLOGY USED
1. What is Apache Spark?
- Apache Spark is an open-source distributed data processing system optimized for high-performance computing using in-memory caching and optimized query execution. It supports multiple programming languages (Java, Scala, Python, R) and can handle batch processing, real-time streaming, machine learning, and graph processing.

2. Development History
- Started in 2009 at AMPLab, UC Berkeley.

- First research paper published in 2010, open-sourced under a BSD license.

- June 2013: Became an Apache Incubator project.

- February 2014: Upgraded to an Apache Top-Level Project.

3. How Apache Spark Works
- Faster than MapReduce: Processes data in memory instead of reading/writing from disk.

- Fewer processing steps: Improves performance and efficiency.

- Data reuse: Stores data in memory using RDDs and DataFrames for faster machine learning and iterative analysis.

4. Comparison Between Spark and Hadoop
- Hadoop: A data storage and processing ecosystem with HDFS, YARN, and MapReduce.

- Spark: A high-speed data processing system without built-in storage but can run on HDFS, S3, Cassandra, etc.

- When running on Hadoop, Spark utilizes YARN to manage resources more efficiently.

## Powerful application of Apache Spark
Apache Spark is widely applied across various industries, especially in processing and analyzing large-scale data. Some common applications include:
- Big Data Analytics – Used extensively in finance, healthcare, and e-commerce for processing and analyzing large, complex datasets.

- Machine Learning – Spark’s MLlib library enables scalable machine learning models for classification, regression, clustering, and dimensionality reduction.

- Real-time Data Analytics – Spark Streaming processes real-time data streams for applications like log analysis, network monitoring, and live event processing.

- Graph Processing – The GraphX library supports social network analysis, friend recommendations, and traffic network analysis.

- Multi-source Data Processing – Spark integrates with HDFS, Cassandra, Amazon S3, HBase, and other storage systems for seamless data processing.

- Log Management and Analysis – Used by companies like Yelp, Netflix, and Uber to analyze large log files for system monitoring and network insights.

- Interactive Reporting – Spark integrates with Tableau and Power BI to generate dynamic and interactive data reports for real-time decision-making.


## THEME IMPLEMENTATION
This is a dataset on car accidents across the United States, covering 49 states. The accident data was collected from February 2016 to March 2023 through various APIs that provide real-time traffic incident data. These APIs stream traffic data gathered by multiple organizations, including federal and state Departments of Transportation, law enforcement agencies, traffic cameras, and road network traffic sensors.

Currently, the dataset contains approximately 7.7 million accident records.

- Data format: US_Accidents.csv

- Source: Kaggle - US Accidents Dataset


### Data preprocessing
### Import Libraries
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
```

### Create SparkSession
```python
spark = SparkSession.builder \
    .appName("USTrafficAccidents") \
    .getOrCreate()
```
- **Import necessary libraries** from PySpark.
- **Initialize SparkSession** using `SparkSession.builder`.
- **Set application name** as "USTrafficAccidents".
- **Create a SparkSession instance** using `.getOrCreate()`.

### Load Data
```python
file_path = "dataset/US_Accidents.csv"  
df = spark.read.csv(file_path, header=True, inferSchema=True)
```
- **Set file path** pointing to `US_Accidents.csv` inside the `dataset` directory.
- **Read CSV file** using `spark.read.csv()`:
  - `header=True`: The first row is treated as column names.
  - `inferSchema=True`: Automatically detects data types.
- **Store data** into a DataFrame `df`.

### Check for Null Values
```python
null_counts = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])

# Display results
null_counts.show()
```
- **Identify null values**:
  - For each column, `when(col(c).isNull(), 1).otherwise(0)` returns `1` if null, otherwise `0`.
  - `sum()` calculates the total count of null values per column.
- **Create `null_counts` DataFrame**:
  - Uses `select()` to generate a new column with null counts for each original column.
  - `alias(c)` renames the result column to match the original column name.
- **Display results** using `show()`.

### Handle Missing Values
```python
def fill_date_for_missing(df):
    # Extract date from Weather_Timestamp
    df = df.withColumn("Start_Date", date_format("Weather_Timestamp", "M/d/yyyy"))
    df = df.withColumn("End_Date", date_format("Weather_Timestamp", "M/d/yyyy"))

    # Fill missing dates in Start_Time and End_Time
    df = df.withColumn(
        "Start_Time",
        when(
            col("Start_Time").rlike("^[0-9]{1,2}:[0-9]{2}(\.[0-9]+)?$"),
            concat(col("Start_Date"), lit(" 00:"), regexp_replace(col("Start_Time"), r":[0-9]{2}(\.[0-9]+)?$", ""))
        ).otherwise(col("Start_Time"))
    )

    df = df.withColumn(
        "End_Time",
        when(
            col("End_Time").rlike("^[0-9]{1,2}:[0-9]{2}(\.[0-9]+)?$"),
            concat(col("End_Date"), lit(" 00:"), regexp_replace(col("End_Time"), r":[0-9]{2}(\.[0-9]+)?$", ""))
        ).otherwise(col("End_Time"))
    )

    # Convert Start_Time and End_Time to timestamp format
    df = df.withColumn("Start_Time", to_timestamp("Start_Time", "M/d/yyyy H:mm"))
    df = df.withColumn("End_Time", to_timestamp("End_Time", "M/d/yyyy H:mm"))
    
    return df

# Apply function
df = fill_date_for_missing(df)
# Drop temporary columns
df = df.drop("Start_Date", "End_Date")
```
- **Extract dates** from `Weather_Timestamp`.
- **Fill missing time values** in `Start_Time` and `End_Time`:
  - If only time is present (HH:mm format), add corresponding date.
  - Uses `rlike`, `concat`, and `regexp_replace` to process time values.
- **Convert to timestamp format**.
- **Remove temporary columns** `Start_Date` and `End_Date`.

### Replace Null Values
```python
df = df.fillna({
    "Description": "No Description",
    "Street": "Unknown",
    "City": "Unknown",
    "Weather_Condition": "Unknown",
    "Wind_Direction": "Unknown"
})
```
- **Replace null values** in string columns with default values.

### Replace Nulls in Boolean Columns
```python
boolean_columns = [
    "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
    "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
    "Traffic_Signal", "Turning_Loop"
]
for col_name in boolean_columns:
    df = df.withColumn(col_name, when(col(col_name).isNull(), lit(False)).otherwise(col(col_name)))
```
- **Replace null values** in boolean columns with `False`.

### Replace Nulls in Numeric Columns
```python
numeric_columns = [
    "Severity", "Distance(mi)", "Temperature(F)", "Wind_Chill(F)",
    "Humidity(%)", "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)",
    "Precipitation(in)"
]
for col_name in numeric_columns:
    mean_value = df.select(mean(col(col_name))).collect()[0][0]
    df = df.withColumn(col_name, when(col(col_name).isNull(), lit(mean_value)).otherwise(col(col_name)))
```
- **Replace null values** in numeric columns with their mean values.

### Add Time-Based Features
```python
df = df.withColumn("Sunrise_Sunset", when(hour("Start_Time").between(6, 18), "Day").otherwise("Night"))
df = df.withColumn("Civil_Twilight", when(hour("Start_Time").between(5, 19), "Day").otherwise("Night"))
df = df.withColumn("Nautical_Twilight", when(hour("Start_Time").between(4, 20), "Day").otherwise("Night"))
df = df.withColumn("Astronomical_Twilight", when(hour("Start_Time").between(3, 21), "Day").otherwise("Night"))
```
- **Create new columns** `Sunrise_Sunset`, `Civil_Twilight`, `Nautical_Twilight`, and `Astronomical_Twilight`.
- **Determine if it is "Day" or "Night"** based on `Start_Time` hour.

### Format Timestamp Columns
```python
df = df.withColumn("Start_Time", date_format("Start_Time", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("End_Time", date_format("End_Time", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("Weather_Timestamp", date_format("Weather_Timestamp", "yyyy-MM-dd HH:mm:ss"))
```
- **Convert timestamps** to `yyyy-MM-dd HH:mm:ss` format.

### Save Processed Data
```python
output_path = "dataset/CleanData"
df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
spark.stop()
```
- **Save the DataFrame** as a single CSV file inside `dataset/CleanData`.
- **Stop the SparkSession** after processing is complete.

## Data Analysis and Visualization Preparation

### Accident Severity Analysis Based on Weather Conditions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName('Weather_Analysis').getOrCreate()

# Read data into DataFrame
df = spark.read.csv("dataset/CleanData/cleandata.csv", header=True, inferSchema=True)

# Convert Start_Time to timestamp format
df = df.withColumn("Start_Time", to_timestamp("Start_Time", "M/d/yyyy H:mm"))

# Map Severity values to descriptive labels
df = df.withColumn(
    "Severity",
    when(col("Severity") == 1, "Low")
    .when(col("Severity") == 2, "Moderate")
    .when(col("Severity") == 3, "High")
    .when(col("Severity") == 4, "Severe")
)

# Aggregate average weather conditions by severity
weather_analysis = df.groupBy("Severity").agg(
    avg("Temperature(F)").alias("Avg_Temperature"),
    avg("Visibility(mi)").alias("Avg_Visibility"),
    avg("Wind_Speed(mph)").alias("Avg_WindSpeed")
)

# Show results
weather_analysis.show()
```

### Accident Severity Analysis Based on Infrastructure

```python
boolean_columns = ["Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit", "Railway", 
                   "Roundabout", "Station", "Stop", "Traffic_Calming", "Traffic_Signal", "Turning_Loop"]

# Convert boolean columns to numerical values
for col_name in boolean_columns:
    df = df.withColumn(col_name, when(col(col_name) == True, 1).otherwise(0))

# Select relevant columns for analysis
accidents_by_infras = df.select("ID", "Severity", *boolean_columns)

# Show results
accidents_by_infras.show()
```

### Accident Severity Analysis Based on Geolocation

```python
# Group data by state, city, and severity
accidents_by_geolocation = df.groupBy("State", "City", "Severity").count()

# Show results
accidents_by_geolocation.show()
```

### Accident Severity Analysis Based on Time

```python
# Extract time-based features
df = df.withColumn("Hour", hour("Start_Time"))
df = df.withColumn("Day", dayofweek("Start_Time"))
df = df.withColumn("Month", month("Start_Time"))
df = df.withColumn("Year", year("Start_Time"))

# Group data by time features and severity
accidents_by_time = df.groupBy("Year", "Month", "Day", "Hour", "Severity").count()

# Show results
accidents_by_time.show()
```

This analysis helps visualize accident trends based on weather conditions, infrastructure, geolocation, and time. The processed data can be used for further insights and visualization.


## General visualization
<p align="center">
  <img src="https://github.com/taovietducofficial/BDA_FinalProject/blob/master/1.jpg" width="1000" />
</p>

- Weather Conditions by Severity Level – Classify weather conditions by severity level and analyze their relationship with accidents.

- Monthly Distribution of Accidents by Severity – Number of accidents occurring each month, categorized by severity level.

- Yearly Trends of Accidents by Severity – Trends in the number of accidents per year, analyzing severity levels.

- Accident Distribution by City and Severity – Distribution of accidents by city, combined with severity levels.

- Accident Distribution by State and Severity – Distribution of accidents by state/region, combined with severity levels.

- Hourly Distribution of Accidents by Severity – Number of accidents occurring at different hours of the day, categorized by severity level.

- Impact of Amenity, Bump, Crossing, and Give Way – Analysis of the impact of amenities, speed bumps, pedestrian crossings, and yield signs on accidents.

- Impact of Junction, Railway, and Roundabout – Effects of intersections, railways, and roundabouts on the number and severity of accidents.

- Impact of Traffic Signals, Stops, and Calming Features – Study the impact of traffic lights, stop signs, and speed calming measures on accidents.

## Visualization by influence
<p align="center">
  <img src="https://github.com/taovietducofficial/BDA_FinalProject/blob/master/%C3%A2.png" width="1000" />
</p>
Summary:

- Avg Temperature: Less severe accidents (Low) occur in warmer weather (72.2°F), while severe accidents (Severe) happen in colder conditions (58.2°F).

- Avg Visibility: Visibility remains similar across severity levels (~9.1–9.5), with Low having the best visibility (9.52).

- Avg Wind Speed: Stronger winds (8.26 mph) are linked to more severe accidents (High), while lower wind speeds (7.08 mph) are associated with less severe accidents (Low).

Conclusion:

- Severe accidents tend to occur in colder temperatures and higher wind speeds.

- Less severe accidents are more common in warmer temperatures and good visibility.

- Weather factors like temperature and wind speed play a crucial role in accident severity.

## Visualization by infrastructure influence
<p align="center">
  <img src="https://github.com/taovietducofficial/BDA_FinalProject/blob/master/3.png" width="1000" />
</p>

Summary:

- Accidents in areas with well-developed infrastructure tend to be less severe. This suggests that proper infrastructure helps mitigate accident severity and reduces its impact on traffic.

Conclusion:

- Investing in essential infrastructure can help lower accident severity and improve road safety.

## Visualization by time influence
<p align="center">
  <img src="https://github.com/taovietducofficial/BDA_FinalProject/blob/master/4.png" width="1000" />
</p>

Summary:

- Hourly Accident Severity: Peak hours (0 AM, 7 AM, 8 AM, 4 PM, 6 PM) have a high accident frequency, but severity remains moderate. This is likely due to rush hour traffic.

- Weekly Accident Trends: Accidents occur most frequently from Tuesday to Saturday, coinciding with workdays and increased traffic flow.

- Monthly Accident Trends: The highest accident rates occur in January, February, November, and December, possibly due to holiday travel and year-end activities. The lowest rates are in June and July, when students are on summer break.

Conclusion:

- To reduce accident risks, avoid peak-hour travel when possible, drive cautiously, and consider using public transportation.

## Visualization by geographical location influence
<p align="center">
  <img src="https://github.com/taovietducofficial/BDA_FinalProject/blob/master/5.png" width="1000" />
</p>

# CONCLUDE
## Achievements
- Built a PySpark pipeline for fast, automated, and efficient processing of large datasets.

- Conducted a detailed analysis of factors affecting traffic accidents, including time, location, weather conditions, and other parameters.

- Designed an interactive dashboard in Power BI, providing a comprehensive and visual representation of traffic accident data.

## Advantages
- PySpark enables fast data processing, meeting the demands of handling large volumes of data.

- Power BI provides an intuitive interface with dynamic visualizations, allowing users to quickly grasp insights.

- The project focuses on practical applications in supporting traffic decision-making and proposing solutions.

## Limitations
- PySpark requires complex infrastructure setup and advanced technical expertise.

- Data quality is inconsistent, with missing or inaccurate fields.

- Power BI has limitations in handling extremely large datasets and complex analytical requirements.

## Future Development
- Integrate more data sources, including IoT systems and real-time data streams.

- Enhance analytics with AI/ML for automated trend detection and risk warnings.

- Develop new visualization elements to improve interactivity and clarity.

# THANK YOU FOR WATCHING!
<p align="justify">
Only 85% of the complete code has been pushed due to certain ownership rights reasons from me. Please review and respect this. Thank you very much!
</p>
