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
