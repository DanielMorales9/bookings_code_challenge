# CLI Description
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
[options]
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

### Launching Number of Bookings per Day Report
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
