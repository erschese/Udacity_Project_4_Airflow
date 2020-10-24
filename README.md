# Project: Data Pipelines with Airflow

## project overview
This project will introduce you to the core concepts of Apache Airflow. To complete the project, you will need to create your own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We have provided you with a project template that takes care of all the imports and provides four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

## airflow pipeline

The project template package contains three major components for the project:

    * The dag template has all the imports and task templates in place, but the task dependencies have not been set
    * The operators folder with operator templates
    * A helper class for the SQL transformations

## tasks

The dag consists of the tasks

	* start_operator -> dummy operator to start the dag
    * create_tables -> creates the redshift tables
    * stage_events_to_redshift -> loads s3 event data into the stageing area
    * stage_songs_to_redshift -> loads s3 song data into the stageing area
    * load_songplays_table -> creats the songplays fact table from stageing data
    * load_user_dimension_table -> creats the user dimension table from stageing data
    * load_song_dimension_table -> creats the song dimension table from stageing data
    * load_artist_dimension_table -> creats the artists dimension table from stageing data
    * load_time_dimension_table -> creats the time dimension table from stageing data
    * run_quality_checks -> execute some quality checks
    
###run_quality_checks-Task
The quality checks are executed through the 'data_quality'-operator.
The operator takes a parameter value to define the quality checks which should be performed.
The parameter needed has the following structure:
	params={
        'tables_count': {'public.users', 'public.artists', 'public.songplays', 'public.songs', 'public.time'},
        'tables_fields_null': {'public.users;userid', 'public.songplays;playid', 'public.staging_events;sessionid',
                              'public.staging_events;ts'},
    }

* The 'tables_count' value provides table names. For all tables given the 'data_quality'-operator will check, if there is at least some data in the table

* The 'tables_fields_null' has the structure '[table name to check];[field name to check]'. For all given tables and fields the 'data_quality'-operator will check, if the field is null in the given table.
    

## airflow connection settings
For this project, we'll use Airflow's UI to configure the AWS credentials and connection to Redshift.
Follow the steps -> 

To configure connections in airflow follow the next steps
* click on the admin tab and select connections
* under connections, select create

### aws credentials

To configure the aws connection enter the following fields
* Conn Id: -> aws_credentials
* Conn Type: -> Amazon Web Services
* Login: ->  --your Access key ID-- (AKIA4CPOQQ6BWD3VTS57)
* Password: -> --your Secret access key--

### redshift

To configure the redshift connection enter the following fields
* Conn Id: -> redshift
* Conn Type: -> Postgres
* Host: -> --your aws cluster endoint-- (dwhcluster.clxoohxr0jdn.us-west-2.redshift.amazonaws.com)
* Schema: -> dwh
* Login: --> dwhuser
* Password: --> Passw0rd
* Port: --> 5439


## table structure
###Fact table:

    songplays - records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

###Dimension tables:

    users - users in the app
    columns: user_id, first_name, last_name, gender, level
    
    songs - songs in music database
    columns: song_id, title, artist_id, year, duration
    
    artists - artists in music database
    columns: artist_id, name, location, lattitude, longitude
    
    time - timestamps of records in songplays broken down into specific units
    columns: start_time, hour, day, week, month, year, weekday

###Database schema:

![alt text](data/tables.JPG "Logo Title Text 1")
