package de.holidaycheck.utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.util.Success

class ArgumentValidatorSpec extends AnyFunSuite {
  test("it should fail validating path") {
    val validationResult =
      ArgumentValidator.validatePathExistance("/does/not/exists")
    assert(validationResult.isFailure)
  }

  test("it should fail validating save mode") {
    val validationResult = ArgumentValidator.validateSaveMode("wrongSaveMode")
    assert(validationResult.isFailure)
  }

  test("it should success validating save mode") {
    val validationResult = ArgumentValidator.validateSaveMode("overwrite")
    assert(validationResult.isSuccess)
    validationResult shouldBe Success("overwrite")
  }

  test("it should fail validating extraction_date") {
    val validationResult = ArgumentValidator.validateDateString("2022-13-32")
    assert(validationResult.isFailure)
  }

  test("it should fail on null value") {
    val validationResult = ArgumentValidator.validateNotNull(null)
    assert(validationResult.isFailure)
  }

}
