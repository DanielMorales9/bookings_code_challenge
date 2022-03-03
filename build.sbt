name := "code_challenge"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided,
  "org.typelevel" %% "cats-core" % "2.7.0",
  "org.typelevel" %% "cats-kernel" % "2.7.0",
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.0" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % Test
)

scalacOptions += "-Ypartial-unification"
