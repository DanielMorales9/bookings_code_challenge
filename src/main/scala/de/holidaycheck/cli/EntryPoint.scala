package de.holidaycheck.cli

import de.holidaycheck.jobs.{Bookings, Cancellation}
import wvlet.airframe.launcher.{command, option}

// Define a global option
case class GlobalOption(
    @option(
      prefix = "-h,--help",
      description = "display help messages",
      isHelp = true
    )
    help: Boolean = false
)

class EntryPoint(g: GlobalOption) {

  @command(isDefault = true)
  def default(): Unit = {
    println("Type --help to display the list of commands")
  }

  @command(description = "Cleaning Bookings Data")
  def bookings(
      @option(prefix = "-i,--input", description = "Input Path")
      input_path: String
//      @option(
//        prefix = "-e, --extraction_date",
//        description = "Date of Extraction"
//      )
//      extraction_date: String,
//      @option(prefix = "-o, --output", description = "Output Path")
//      output_path: String = "."
  ): Unit = {
    new Bookings(input_path).run()
  }

  @command(description = "Cleansing Cancellation Data")
  def cancellation(
      @option(prefix = "-i,--input", description = "Input Path")
      input_path: String
      //      @option(
      //        prefix = "-e, --extraction_date",
      //        description = "Date of Extraction"
      //      )
      //      extraction_date: String,
      //      @option(prefix = "-o, --output", description = "Output Path")
      //      output_path: String = "."
  ): Unit = {
    new Cancellation(input_path).run()
  }
}
