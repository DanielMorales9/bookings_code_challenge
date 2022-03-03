package de.holidaycheck.middleware

case class DataError(
    rowKey: String,
    stage: String,
    fieldName: String,
    fieldValue: String,
    error: String
)
