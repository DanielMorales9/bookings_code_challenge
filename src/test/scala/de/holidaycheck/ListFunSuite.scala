package de.holidaycheck
import org.scalatest.funsuite.AnyFunSuite

class ListFunSuite extends AnyFunSuite {

  test("An empty List should have size 0") {
    assert(List.empty.isEmpty)
  }

}