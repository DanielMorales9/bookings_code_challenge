package de.holidaycheck.cli

import de.holidaycheck.cli.Validator.{
  validateDateString,
  validatePath,
  validateSaveMode
}
import de.holidaycheck.jobs.{Bookings, Cancellation}
import wvlet.airframe.launcher.{command, option}

import scala.util.{Failure, Success, Try}

class EntryPoint() {

  def validateSimpleJobArguments(
      input_path: String,
      output_path: String,
      mode: String,
      extraction_date: String
  ): Try[(String, String, String, String)] = {
    for {
      inputPath <- validatePath(input_path)
      extractionDate <- validateDateString(extraction_date)
      mode <- validateSaveMode(mode)
    } yield (inputPath, output_path, mode, extractionDate)
  }

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
      mode: String = "error",
      @option(
        prefix = "-e,--extraction_date",
        description = "Date of Extraction yyyy-MM-dd"
      )
      extraction_date: String
  ): Unit = {
    validateSimpleJobArguments(
      input_path,
      output_path,
      mode,
      extraction_date
    ) match {
      case Success(args) =>
        new Bookings(
          args._1,
          args._2,
          args._3,
          args._4
        ).run()
      case Failure(e) => throw e
    }
  }

  @command(description = "Cleansing Cancellation Data")
  def cancellation(
      @option(prefix = "-i,--input", description = "Input Path")
      input_path: String,
      @option(prefix = "-o,--output", description = "Output Path")
      output_path: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: String = "error",
      @option(
        prefix = "-e,--extraction_date",
        description = "Date of Extraction yyyy-MM-dd"
      )
      extraction_date: String
  ): Unit = {
    validateSimpleJobArguments(
      input_path,
      output_path,
      mode,
      extraction_date
    ) match {
      case Success(args) =>
        new Cancellation(
          args._1,
          args._2,
          args._3,
          args._4
        ).run()
      case Failure(e) => throw e
    }
  }
}
