package de.holidaycheck.cli

import de.holidaycheck.jobs.{Bookings, Cancellation}
import wvlet.airframe.launcher.{command, option}

class EntryPoint() {

  @command(isDefault = true)
  def default(): Unit = {
    println("Type --help to display the list of commands")
  }

  @command(description = "Cleaning Bookings Data")
  def bookings(
      @option(prefix = "-i,--input", description = "Input Path")
      input_path: String,
      @option(prefix = "-o,--output", description = "Output Path")
      output_path: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: Option[String] = None
      //      @option(
      //        prefix = "-e, --extraction_date",
      //        description = "Date of Extraction"
      //      )
      //      extraction_date: String,
  ): Unit = {
    // TODO missing arguments validation
    new Bookings(input_path, output_path, mode.getOrElse("error")).run()
  }

  @command(description = "Cleansing Cancellation Data")
  def cancellation(
      @option(prefix = "-i,--input", description = "Input Path")
      input_path: String,
      @option(prefix = "-o,--output", description = "Output Path")
      output_path: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: Option[String] = None
      //      @option(
      //        prefix = "-e, --extraction_date",
      //        description = "Date of Extraction"
      //      )
      //      extraction_date: String,
  ): Unit = {
    // TODO missing arguments validation
    new Cancellation(input_path, output_path, mode.getOrElse("error")).run()
  }
}
