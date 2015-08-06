This project is to develop a widget to determine seasonality at our hospitals. The process should look something like this:

1) Use Sqoop to obtain raw data sources if they don't exist.
2) Use Spark to enrich the raw data. Store it in HBase?
3) Use SparkR to do some calculations about monthly seasonality and weekly seasonality.
4) Integrate Holidays into the seasonality calculation. Will need to digest holidays from source ICS file.
5) Use MLib to perform a cluster analysis. Perhaps use a similarity calculation to determine which days are most like another.
6) Use the model that we've developed to forecast patient volumes.

Extensions
1) Segment the analysis by geographic area, facility, and department. Segment the analysis by hour.
2) Integrate weather. What are the effects of rain, snow, extreme temperature, and other weather related events on the volume.
	Will need to source weather data, store and process it.
3) Integrate local and national sports events. Will need to find or create source file.