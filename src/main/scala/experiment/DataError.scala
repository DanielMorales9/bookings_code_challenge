package experiment

case class DataError(rowKey: String, stage: String, fieldName: String, fieldValue: String, error: String, severity: String, addlInfo: String = "")
