# Programming Assignment

We want to get a better understanding of the bookings that happen on our website. Specifically, how many bookings are cancellable, number of bookings per day, what are the popular destinations and what is the peak travel season.
The data is available at a central location (assume any file system or database) in csv format and is updated periodically. You need to do the following tasks:

1. Design and build a data pipeline that will save and process this data to be able to answer the above questions. 
2. Use the data to build a report that shows number of bookings per day. 
	The output of the report should have two columns:
		a. date
		b. num_bookings
3. Create a report to show which bookings are free cancellable and which bookings are cancellable with a fee (cheap_cancellable) and until when. You are free to decide the structure of the report as you feel necessary

## Data

1. bookings.csv : Data about bookings. eg. booking_id, booking_date, destination, source, travel_date, arrival_date
2. cancellation.csv : Information about which bookings are cancellable and till when. Columns : booking_id, cancellation_id (52 for free and 53 for cheap), enddate (till what date is the booking cancellable)

You are free to decide the storage location and format of the final report. Make assumptions wherever necessary. Please use **Scala** language and **Spark** framework (in local mode).

## What do we look for?

1. The code that is scalable, easy to maintain and extensible. We want to keep as minimum manual work as possible
2. Tests wherever necessary
3. Readme to explain your assumptions, solution and how to run it
4. Bonus : Architecture diagram	

# The Solution
The solution provides a single entrypoint to both reporting and processing jobs by using a CLI. 
The application is conceived in such a way that each source of data (bookings and cancellations) 
can go through a first cleansing phase independently 
and later can be joined together in a single flat table to be able to be accessed for analytics purpose.

## Cleansing Jobs
We provide two pre-processing jobs, namely, the job to cleanse the bookings and the cancellation data.
The aim of the pre-processing jobs is to improve the data quality of the raw data as much as possible,
in order to ensure reliable source data for any downstream jobs.
Moreover, we don't only cleanse the source data by filtering out malformed records,
we also provide, for each dataset, an error repository to track
which record was filtered out from the original dataset.
Having an error repository ensures that the quality of both the source data and the etl code is always high.
However, the reader might think there is necessary manual work to be carried out in order to adjust the source data,
on the long term, simple fixes can be automated either by incorporating such adjustments in the etl jobs or by creating ad-hoc scripts.
Moreover, having such an error repository available via analytics tools by members of different departments,
lays the foundation for a better communication between teams across the organization,
but especially enables catching bugs earlier on in the software development cycle.

Therefore, each cleansing job produces a `data` and `errors` folder in each root directory, 
both partitioned by the date in which the batch was executed 
(assuming the batch was executed daily), hence `extraction_date=yyyy-MM-dd`.

The resulting error table will have the following schema (which is defined in the case class `middleware.DataError`):
- The `rowKey` (autogenerated value), which identifies the row within the source dataset.
- The `stage` of the pipeline where the record was found malformed.
- The `fieldName` or column name
- The `fieldValue`, the actual column value for the filtered out row  
- The `error`, a human-readable message explaining the reason why the record was filtered out.  

| rowKey | stage                      | fieldName    | fieldValue  | error                           | 
|--------|----------------------------|--------------|-------------|---------------------------------|
| 100001 | ValidateNotNullColumnStage | destination  | 1           | Invalid Airport Code: 1         |
| 21450  | ParseDateTimeStringStage   | booking_date | -2147483648 | Unable to parse DateTime string |

This schema can be also extended to add the **severity** of the errors 
or _additional information_ to ease any potential debugging efforts.

The `rowKey` could be either a monotonically increasing id, as in the example, 
a unique identifier in the dataset, such as the `booking_id`, 
or a hash of the entire row. We preferred the simplest among the strategies, 
mostly to demonstrate the potential of error tracking.

## The Software Architecture
Even though, the processing steps are very simple and easy 
we want to aim for a readable and extensible software architecture. 
Thus, we built the pipeline as a sequence of composable transformation steps, 
where each is step is highly cohese ahd loosely coupled with the others. 
Each step carries out a simple computation step and is responsible 
for producing both a dataset of valid records and errors.
The steps are then combined in order to form a full-blown pipeline, 
and can also be reused across different pipelines. 
This approach also makes it easier for the developer
to test the code in isolation reducing also the overall execution of the tests, 
compared to having to test several e2e scenarios.

Modularity of the code is achieved by building several middleware components using the functional library `cats`. 
Each transformation step extends a `DataStage`, which has an `apply` method; it takes a DataFrame or Dataset as a parameter
and returns a `Writer` which left argument is a Dataset of DataError. 
The writer class are then yield into the next DataStage and 
the Dataset of DataError are implicitly combined into a single dataset (see `middleware.DataFrameOps`).

Each ETL or Reporting Job that encapsulate such a pipeline follows a specific template (see `jobs.JobTemplate`)
which contains the _extract_, _transform_, _load_ methods.
By structuring the jobs around this three simple methods, 
we reduce the amount of e2e tests to be written and executed. 
This is possible because most of the business logic is encapsulated in the _transform_ method,
which allows us to do integration testing by inject a DataFrame and checking the result of the method execution.

### File Format
The cleansed data and the error repository are all stored as parquet files,
while the result of the reports are stored in csv.

# Building the artifact
Building the artifact is simple as:
```bash
sbt assembly
```

# CLI Description
Here you can find an easy description of the cli.  
To run successfully the entire steps within the project, 
you just need to copy and paste the commands written below the text _Full command_ in your terminal and run.


```bash
spark-submit [...] --help

usage: entry_point [options] <command name>

[options]
 -h, --help  display help messages

[commands]
 job            List of all Jobs
 report         List of all Reports

```

## Job Entrypoint
```bash
spark-submit [...] job --help

usage: [global options] job [options] <command name>
  jobs entrypoint

[global options]
 -h, --help  display help messages

[commands]
 bookings       Cleaning Bookings Data
 cancellation   Cleansing Cancellation Data
 joinBookings   Joining Data
```

### Launching Bookings Job
```bash
spark-submit [...] job bookings --help

usage: bookings 
  Cleaning Bookings Data

[global options]
 -h, --help  display help messages
[options]
 -i, --input:[INPUTPATH]                 Input Path
 -o, --output:[OUTPUTPATH]               Output Path
 -m, --mode:[MODE]                       Mode
 -e, --extraction_date:[EXTRACTIONDATE]  Date of Extraction yyyy-MM-dd
```
Full command:
```bash
spark-submit \
  --class "de.holidaycheck.Main" \
  --master local[4] \
  target/scala-2.12/code_challenge-assembly-1.0.jar \
  job bookings \
    -i bookings.csv \
    -o bookings \
    -m overwrite \
    -e 2022-03-03
```


### Launching Cancellation Job

```bash
spark-submit [...] job cancellation --help

usage: cancellation
Cleansing Cancellation Data

[global options]
-h, --help  display help messages

[options]
-i, --input:[INPUTPATH]                 Input Path
-o, --output:[OUTPUTPATH]               Output Path
-m, --mode:[MODE]                       Mode
-e, --extraction_date:[EXTRACTIONPATH]  Date of Extraction yyyy-MM-dd
```
Full Command:
```bash
spark-submit \
  --class "de.holidaycheck.Main" \
  --master local[4] \
  target/scala-2.12/code_challenge-assembly-1.0.jar \
  job cancellation \
    -i cancellation.csv \
    -o cancellation \
    -m overwrite \
    -e 2022-03-03
```


### Launching Join Job
````bash
spark-submit [...] job joinBookings --help

usage: joinBookings 
  Joining Data

[global options]
 -h, --help  display help messages
 
[options]
 -b, --bookings:[BOOKINGSINPUTPATH]          Bookings Input Path
 -c, --cancellation:[CANCELLATIONINPUTPATH]  Cancellation Input Path
 -o, --output:[OUTPUTPATH]                   Output Path
 -m, --mode:[MODE]                           Mode
 -e, --extraction_date:[EXTRACTIONDATE]      Date of Extraction yyyy-MM-dd
````
Full Command:
```bash
spark-submit \
  --class "de.holidaycheck.Main" \
  --master local[4] \
  target/scala-2.12/code_challenge-assembly-1.0.jar \
  job joinBookings \
    -b bookings/data \
    -c cancellation/data \
    -o flatTable \
    -m overwrite \
    -e 2022-03-03
```
## Reports Entrypoint
```bash
spark-submit [...] report --help

usage: [global options] report <command name>
List of all Reports

[global options]
-h, --help                 display help messages

[commands]
numBookingsPerDay              Number of Bookings per Day Report
cheapAndFreeCancellations      Cheap And Free Cancellations Report
```

### Launching Number of Bookings per Day Report
```bash
spark-submit [...] report numBookingsPerDay --help

usage: numBookingsPerDay 
  Number of Bookings per Day Report

[global options]
 -h, --help                 display help messages
[options]
 -i, --input:[INPUTPATH]    Input Path
 -o, --output:[OUTPUTPATH]  Output Path
 -m, --mode:[MODE]          Mode
```
Full Command:
```bash
spark-submit \
  --class "de.holidaycheck.Main" \
  --master local[4] \
  target/scala-2.12/code_challenge-assembly-1.0.jar \
  report numBookingsPerDay \
    -i flatTable/data/extraction_date=2022-03-03 \
    -m overwrite \
    -o reports/numBookingsPerDay
```

### Launching Cheap and Free Cancellations Report
```bash
spark-submit [...] report cheapAndFreeCancellations --help

usage: cheapAndFreeCancellations 
  Cheap And Free Cancellations Report

[global options]
 -h, --help                 display help messages
 
[options]
 -i, --input:[INPUTPATH]    Input Path
 -o, --output:[OUTPUTPATH]  Output Path
 -m, --mode:[MODE]          Mode
```
Full Command:
```bash
spark-submit \
  --class "de.holidaycheck.Main" \
  --master local[4] \
  target/scala-2.12/code_challenge-assembly-1.0.jar \
 report cheapAndFreeCancellations \
  -i flatTable/data/extraction_date=2022-03-03 \
  -o reports/cheapAndFree \
  -m overwrite
```

# Future Work
- Introduce a Global Logging Level options for Spark Jobs and Reports
- E2E tests
- Refactoring potential in cli classes (duplicated code due to parameter validation)
- Use of reflection to instantiate jobs or reports by name
- 

