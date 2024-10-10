# Azure-Spotify-ETL-Wrapped

## Introduction

In today’s digital era, music streaming services like Spotify have transformed the way we interact with music, offering personalized experiences that cater to individual tastes and preferences. Among these features, Spotify Wrapped has become a highly anticipated annual event, summarizing our music listening habits with colorful infographics and insightful statistics. While Spotify Wrapped provides a valuable snapshot of our year in music, enthusiasts often find themselves wanting more immediate and detailed insights into their complete listening history.

Imagine having the ability to explore not just your annual highlights, but your entire music journey — your all-time favorite tracks, genres that have shaped your musical tastes, and the evolution of artists who have defined your playlists. This desire to delve deeper into my music listening habits, beyond the limitations of a once-a-year summary, sparked the inspiration for a project aimed at leveraging Azure’s robust data processing tools.

Unlike Spotify Wrapped, which focuses on a single snapshot in time, this project seeks to offer continuous and comprehensive insights into music preferences. By harnessing Azure’s capabilities in data extraction, transformation, and loading (ETL), coupled with the analytical power of platforms like Power BI, I aim to create a more personalized and dynamic analysis platform. This platform will empower users to explore and understand their music listening habits in real-time, gaining deeper insights and fostering a greater appreciation for their musical journey throughout the year.

## Prerequisites

### Spotify account : 
Necessary to access detailed listening history and utilize the Spotify API for data extraction.

### Azure Subscription :
Needed to leverage Azure Data Factory, Azure Databricks, Azure DataFlow, and Azure SQL Database. The free trial offers sufficient resources to complete this project.

## Tools Used:

### Azure Data Factory: 
Orchestrates and automates data movement and transformation pipelines, facilitating seamless integration and processing of Spotify data.
### Azure Databricks:
Provides a collaborative and scalable environment for data engineering tasks, including data cleaning, transformation, and advanced analytics using languages like Python and SQL.
### Azure Data Flow: 
Enables visual data integration and transformation at scale, offering a simplified approach to building and operationalizing ETL (Extract, Transform, Load) workflows.
### Azure SQL Database: 
Offers a secure and scalable database service, storing structured data from Spotify for efficient querying and analysis.
### Power BI:
Empowers visualization and exploration of data insights through interactive dashboards and reports, showcasing trends and patterns in my music preferences over time.

By integrating these Azure tools with Spotify’s API, I set out to extract raw data, transform it into actionable insights, and visualize these insights in real-time. This approach not only enhances my understanding of my Spotify listening history but also provides deeper insights into trends and patterns in my music preferences.


## Architecture Diagram :
![Spotify_Wrapped_Schema-Page-2 drawio (1)](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/e8d64e06-eb4f-4241-a158-b27bc0e6fa1c)

This architecture diagram illustrate the data orchestration process for managing and processing data within a system. It begins with data sources, including user streaming history and Spotify API, collected through Azure Data Lake. The data then undergoes transformation using Azure Databricks and Azure Data Flow, followed by storage in Azure SQL Database. Finally, the data is visualized using Power BI, with the entire process monitored and governed by tools such as Azure Monitor and Azure Active Directory. This ensures that raw data is transformed into actionable and secure information.

## Data Model Diagram :
![Spotify_Wrapped_Schema-Database_model drawio (1)](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/269b4612-9cd4-4078-9a90-ca9a72904bfb)

I built this data model based on the data I collected from Spotify.
1. For each track, I can have multiple streaming IDs associated with it.
2. For a specified track, multiple artists can be associated with it (featuring, etc.).
3. An artist can be associated with different genres.


## Collect your personal data from Spotify :
The first step is to navigate to Spotify’s Privacy Setting pages and request your personal data. This process allows you to access your listening history, top tracks, and other relevant information needed for this project. This request can take several days to be done.
The collected data from Spotify will follow a structured JSON format.

![spotify_json_history_track](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/45d3dde4-6ed1-4eb6-8c99-77eb3c05c907)

We have a list of streaming items like this. For our project, we will extract only the data highlighted in green from each streaming item.
I set up this pipeline to be run each time a file is uploaded in the storage container to copy the data to Azure Data Lake.

![Df_user_data_process](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/238dc10c-6510-4364-a3e5-3badbbd5daeb)

Then I transform the user data with Azure Databricks by running this following code. 
#### Databricks_files/Transform User Data.ipynb
Next, I load the transformed data to an Azure SQL Table with a Copy Data activity.
The whole pipeline for User data will look like this :

![Df_user_pipeline](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/a37af7d8-df24-4966-80f7-c6c6d4cf4862)

## Collect your Tracks data from Spotify API :
Subsequently, I build a pipeline to ingest the track data from the Spotify API.
For this steps you need a Spotify Developer account and follow the “Getting Started” steps from the Spotify Documentation to get a ClientId and ClientSecret.

![ppl_spotify_token](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/7fde85f7-e633-470f-84b5-9e506d1a8e77)

![Df_tracks_data_ingest](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/78d70d9d-1bff-4426-80a6-dec4387b0f40)

I select all distinct track IDs from the user_streaming table to create a list of TracksIDs for API calls. I then run this code inside Databricks 
#### Databricks_files/Ingest Tracks.ipynb
To process track data, I’ve developed this Data Flow in Azure Data Factory :

![Df_tracks_dataflows](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/d8340dc9-54b1-41e2-9dcc-9f548b596944)

Using “GetTracksJSON,” I initially retrieve the generated track data from the ingestion pipeline. Subsequently, employing the select activity, I extract only the necessary columns from the Tracks data JSON.

![Df_tracks_data_dataflowselect](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/658984c7-8c61-4051-b444-5a82e7ac0105)

Following this, I employ a sink activity to create a CSV file containing the required columns.
“GetTracksJSON2” is necessary to construct the Artist_Track associative table. I incorporate it here since we already possess the artist_id and track_id. I simply add a surrogateKey activity to assign a unique ID to each row in the table.
Afterward, I load the transformed data into an Azure SQL Table using a Copy Data activity for Tracks and Streaming_Tracks.

![Df_tracks_sqlize](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/32f45092-f8ca-4421-b95c-a6c8d22de98e)

## Collect your Album data from Spotify API :
For the Album data, the ingestion process will be very similar to the Tracks process. The main differences will be in the copy data activity and the Databricks notebook. Here, I will select all distinct AlbumID from the Tracks table to create a list of AlbumID for the API calls. I execute the following code within Databricks.
#### Databricks_files/Ingest Album.ipynb
To process Album data, I’ve constructed this Data Flow in Azure Data Factory.

![Df_albums_dataflows](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/2769d3cc-d658-4478-b049-503106fa92c1)
And then, I load the transformed data to an Azure SQL Table using a Copy Data activity.

## Collect your Artist data from Spotify API :
For the Artist data, the ingestion process will be very similar to the Album and Tracks processes. The main differences will be in the copy data activity and the Databricks notebook. Here, I will select all distinct TrackID from the Tracks table to create a list of TrackID for the API calls.

![Df_artist_data_ingest](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/92d37abe-eeb4-47f6-934d-257ab2788b1c)

And in Azure Databricks I use this following code :
#### Databricks_files/Ingest Artist.ipynb
To process the Album data I’ve build this Data Flow in Azure DF.

![Df_artist_dataflows](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/69fa1e2f-0eec-42b3-8347-e7b6bc84b135)

Here, I execute two different sinks because I need to export this data for the genre table and artist table. Then, for artist data, I load the transformed data into an Azure SQL Table using a Copy Data activity for Artist and Track_Artist.

## Collect your genre data from Spotify API :
For genre data, I verify the correct functioning of sink2 by checking if the necessary file has been generated inside the container. Then, I execute the following code in Azure Databricks:
#### Databricks_files/Transform Artist Data for Genre.ipynb
And then, I load the transformed data into an Azure SQL Table using a Copy Data activity for genre and genre_artist.

## Visualization with PowerBI :
To create a dashboard in PowerBI Desktop, connect with your Azure SQL Database. Then use the collected data to construct a dashboard similar to this one :

![powerbi_dashboard_v2](https://github.com/NaguguSel/Azure-Spotify-ETL-Wrapped/assets/168862132/667be319-7fcf-455d-8d6d-bb0144533977)

## Conclusion :
This project demonstrates the powerful capabilities of Azure’s data processing and analytics tools in transforming raw Spotify data into meaningful insights. By leveraging Azure Data Factory, Databricks, and SQL Database, along with Power BI for visualization, we’ve built a robust ETL pipeline that provides real-time analysis of music listening habits.

The comprehensive architecture not only automates the data extraction, transformation, and loading processes but also ensures scalability and efficiency. This allows for continuous updates and real-time insights, going beyond the annual summaries provided by Spotify Wrapped.

Through this project, we have unlocked deeper insights into our all-time music preferences, top tracks, and listening patterns. It showcases how integrating personal interests with cutting-edge technology can lead to powerful, personalized analytics.

## Next steps for improvement :
1. Develop a visualization similar to Spotify Wrapped by building a full-stack app.
2. Build an application to automate the entire process for visualization based on specified periods (e.g., from January 2024 to March 2024).


## Ressources :
https://github.com/Shawwnscott/music-data-engineering-azure

https://developer.spotify.com/documentation/web-api

https://learn.microsoft.com/en-us/azure/databricks/

https://learn.microsoft.com/en-us/azure/data-factory/

https://learn.microsoft.com/en-us/power-bi/

https://learn.microsoft.com/en-us/azure/data-factory/concepts-data-flow-overview
