val sv = "2.11.7"

scalaSource in IntegrationTest := baseDirectory.value / "src" / "test-integration" / "scala"

lazy val hedgehog = (project in file("."))
  .settings (
    name := "Hedgehog Map",
    scalaVersion := sv,
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.11" % "3.0.0-M14" % "test,it")
    )
  .configs(IntegrationTest)
  .settings(Defaults.itSettings:_*)
  .settings(parallelExecution in IntegrationTest := false)