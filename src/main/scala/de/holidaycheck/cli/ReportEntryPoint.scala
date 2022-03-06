package de.holidaycheck.cli

import de.holidaycheck.jobs.JoinBookings
import de.holidaycheck.reports.NumberOfBookingsPerDay
import de.holidaycheck.utils.ArgumentValidator.{
  validateDateString,
  validateNotNull,
  validatePathExistance,
  validateSaveMode
}
import wvlet.airframe.launcher.{command, option}

import scala.util.{Failure, Success, Try}

class ReportEntryPoint(
    @option(
      prefix = "-h,--help",
      description = "display help messages",
      isHelp = true
    )
    help: Boolean = false
) {

  def validateReportArguments(
      inputPath: String,
      outputPath: String,
      mode: String
  ): Try[(String, String, String)] = for {
    inputPath <- validatePathExistance(inputPath)
    outputPath <- validateNotNull(outputPath)
    mode <- validateSaveMode(mode)
  } yield (inputPath, outputPath, mode)

  @command(description = "Number of Bookings per Day")
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
        new NumberOfBookingsPerDay(
          args._1,
          args._2,
          args._3
        ).run()
      case Failure(e) => throw e
    }
  }
}
