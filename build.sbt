val sv = "2.11.7"

lazy val hedgehog = (project in file("."))
  .settings (
    name := "Hedgehog Map",
    scalaVersion := sv,
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.11" % "3.0.0-M14" % "test")
    )
