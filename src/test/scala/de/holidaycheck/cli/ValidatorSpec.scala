package de.holidaycheck.cli

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.util.{Failure, Success}

class ValidatorSpec extends AnyFunSuite {
  test("it should fail validating path") {
    val validationResult = Validator.validatePath("/does/not/exists")
    assert(validationResult.isFailure)
  }

  test("it should fail validating save mode") {
    val validationResult = Validator.validateSaveMode("wrongSaveMode")
    assert(validationResult.isFailure)
  }

  test("it should success validating save mode") {
    val validationResult = Validator.validateSaveMode("overwrite")
    assert(validationResult.isSuccess)
    validationResult shouldBe Success("overwrite")
  }

  test("it should fail validating extraction_date") {
    val validationResult = Validator.validateDateString("2022-13-32")
    assert(validationResult.isFailure)
  }
}
