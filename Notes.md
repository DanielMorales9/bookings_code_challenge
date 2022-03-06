# CLI Description
```bash
spark-submit --class "de.holidaycheck.Main" [...] --help

usage: entry_point [options] <command name>

[options]
 -h, --help  display help messages

[commands]
 job            List of all Jobs
 report         List of all Reports

```

## Job Entrypoint
```bash
spark-submit --class "de.holidaycheck.Main" [...] job --help

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
spark-submit --class "de.holidaycheck.Main" [...] job bookings --help

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

### Launching Cancellation Job

```bash
spark-submit --class "de.holidaycheck.Main" [...] job cancellation --help

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

### Launching Join Job
````bash
spark-submit --class "de.holidaycheck.Main" [...] job joinBookings --help

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