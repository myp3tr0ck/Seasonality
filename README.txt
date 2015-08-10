This project is to develop a widget to determine seasonality at our hospitals. The process should look something like this:

1) Use Sqoop to obtain raw data sources if they don't exist.
2) Use Hive to summarize and enrich the raw data. *This is left as an exercise for the reader* 
3) Use R to do some calculations about monthly seasonality and weekly seasonality.
4) Integrate Holidays into the seasonality calculation. A later enhancement to digest holidays from source ICS file.
5) Use Cascading exercise the model that we've developed to forecast patient volumes.

Extensions
1) Segment the analysis by geographic area, facility, patient acuity, and department. Segment the analysis by hour.
2) Integrate weather. What are the effects of rain, snow, extreme temperature, and other weather related events on the volume.
	Will need to source weather data, store and process it.
3) Integrate local and national sports events. Will need to find or create source file.
4) Integrate with phases of the moon
5) Develop alternate models using different numerical methods. Do other models predict more accurately?