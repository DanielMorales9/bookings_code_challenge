package de.holidaycheck.utils

import java.time.LocalDate
import java.time.format.DateTimeParseException
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}

object ArgumentValidator {

  def validateSaveMode(mode: String): Try[String] = {
    val modes = List("overwrite", "append", "ignore", "error", "errorifexists")
    if (modes.contains(mode)) {
      Success(mode)
    } else {
      Failure(new IllegalArgumentException(f"Invalid Save mode $mode"))
    }
  }

  def validateNotNull(value: String): Try[String] = {
    if (value != null) {
      Success(value)
    } else {
      Failure(new IllegalArgumentException(f"Path cannot be Null"))
    }
  }

  def validatePathExistance(filePath: String): Try[String] = {
    if (File(filePath).exists)
      Success(filePath)
    else
      Failure(new IllegalArgumentException(f"Path $filePath does not exists"))
  }

  def validateDateString(extractionDate: String): Try[String] = {
    try {
      LocalDate.parse(extractionDate)
      Success(extractionDate)
    } catch {
      case _: DateTimeParseException =>
        Failure(new IllegalArgumentException(f"Invalid date $extractionDate"))
    }
  }
}
