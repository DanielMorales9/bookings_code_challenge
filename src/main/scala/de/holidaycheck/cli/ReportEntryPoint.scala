package de.holidaycheck.cli

import de.holidaycheck.reports.{
  CheapAndFreeBookingsReport,
  NumberOfBookingsPerDayReport
}
import de.holidaycheck.utils.ArgumentValidator.{
  validateNotNull,
  validatePathExistance,
  validateSaveMode
}
import wvlet.airframe.launcher.{command, option}

import scala.util.{Failure, Success, Try}

class ReportEntryPoint {

  def validateReportArguments(
      inputPath: String,
      outputPath: String,
      mode: String
  ): Try[(String, String, String)] = for {
    inputPath <- validatePathExistance(inputPath)
    outputPath <- validateNotNull(outputPath)
    mode <- validateSaveMode(mode)
  } yield (inputPath, outputPath, mode)

  @command(description = "Number of Bookings per Day Report")
  def numBookingsPerDay(
      @option(prefix = "-i,--input", description = "Input Path")
      inputPath: String,
      @option(prefix = "-o,--output", description = "Output Path")
      outputPath: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: String = "error"
  ): Unit = {
    validateReportArguments(inputPath, outputPath, mode) match {
      case Success(args) =>
        new NumberOfBookingsPerDayReport(
          args._1,
          args._2,
          args._3
        ).run()
      case Failure(e) => throw e
    }
  }

  @command(description = "Cheap And Free Cancellations Report")
  def cheapAndFreeCancellations(
      @option(prefix = "-i,--input", description = "Input Path")
      inputPath: String,
      @option(prefix = "-o,--output", description = "Output Path")
      outputPath: String,
      @option(prefix = "-m,--mode", description = "Mode")
      mode: String = "error"
  ): Unit = {
    validateReportArguments(inputPath, outputPath, mode) match {
      case Success(args) =>
        new CheapAndFreeBookingsReport(
          args._1,
          args._2,
          args._3
        ).run()
      case Failure(e) => throw e
    }
  }
}
