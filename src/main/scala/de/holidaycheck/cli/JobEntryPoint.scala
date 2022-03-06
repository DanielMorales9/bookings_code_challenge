package de.holidaycheck.cli

import de.holidaycheck.utils.ArgumentValidator.{
  validateDateString,
  validateNotNull,
  validatePathExistance,
  validateSaveMode
}
import de.holidaycheck.jobs.{BookingsJob, CancellationJob, JoinBookingsJob}
import wvlet.airframe.launcher.{command, option}

import scala.util.{Failure, Success, Try}

class JobEntryPoint {

  def validateSimpleJobArguments(
      inputPath: String,
      outputPath: String,
      mode: String,
      extractionDate: String
  ): Try[(String, String, String, String)] = {
    for {
      inputPath <- validatePathExistance(inputPath)
      outputPath <- validateNotNull(outputPath)
      extractionDate <- validateDateString(extractionDate)
      mode <- validateSaveMode(mode)
    } yield (inputPath, outputPath, mode, extractionDate)
  }

  @command(description = "Cleaning Bookings Data")
  def bookings(
      @option(prefix = "-i,--input", description = "Input Path")
      inputPath: String,
      @option(prefix = "-o,--output", description = "Output Path")
      outputPath: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: String = "error",
      @option(
        prefix = "-e,--extraction_date",
        description = "Date of Extraction yyyy-MM-dd"
      )
      extractionDate: String
  ): Unit = {
    validateSimpleJobArguments(
      inputPath,
      outputPath,
      mode,
      extractionDate
    ) match {
      case Success(args) =>
        new BookingsJob(
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
      inputPath: String,
      @option(prefix = "-o,--output", description = "Output Path")
      outputPath: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: String = "error",
      @option(
        prefix = "-e,--extraction_date",
        description = "Date of Extraction yyyy-MM-dd"
      )
      extractionPath: String
  ): Unit = {
    validateSimpleJobArguments(
      inputPath,
      outputPath,
      mode,
      extractionPath
    ) match {
      case Success(args) =>
        new CancellationJob(
          args._1,
          args._2,
          args._3,
          args._4
        ).run()
      case Failure(e) => throw e
    }
  }

  def validateJoinArguments(
      inputPath1: String,
      inputPath2: String,
      outputPath: String,
      mode: String,
      extractionDate: String
  ): Try[(String, String, String, String, String)] = {
    for {
      extractionDate <- validateDateString(extractionDate)
      inputPath1 <- validatePathExistance(inputPath1)
      inputPath2 <- validatePathExistance(inputPath2)
      outputPath <- validateNotNull(outputPath)
      mode <- validateSaveMode(mode)
    } yield (inputPath1, inputPath2, outputPath, mode, extractionDate)
  }

  @command(description = "Joining Data")
  def joinBookings(
      @option(prefix = "-b,--bookings", description = "Bookings Input Path")
      bookingsInputPath: String,
      @option(
        prefix = "-c,--cancellation",
        description = "Cancellation Input Path"
      )
      cancellationInputPath: String,
      @option(prefix = "-o,--output", description = "Output Path")
      outputPath: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: String = "error",
      @option(
        prefix = "-e,--extraction_date",
        description = "Date of Extraction yyyy-MM-dd"
      )
      extractionDate: String
  ): Unit = {
    validateJoinArguments(
      f"$bookingsInputPath/extraction_date=$extractionDate",
      f"$cancellationInputPath/extraction_date=$extractionDate",
      outputPath,
      mode,
      extractionDate
    ) match {
      case Success(args) =>
        new JoinBookingsJob(
          args._1,
          args._2,
          args._3,
          args._4,
          args._5
        ).run()
      case Failure(e) => throw e
    }
  }
}
