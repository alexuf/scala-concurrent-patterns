
lazy val Benchmark = config("bench") extend Test

lazy val root = (project in file(".")).
  settings(
    scalaVersion := "2.12.1",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.8.2" % "bench",
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    parallelExecution in Benchmark := false,
    logBuffered := false
  ).
  configs(
    Benchmark
  ).
  settings (
    inConfig(Benchmark)(Defaults.testSettings): _*
  )
