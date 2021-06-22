scalaVersion := "2.12.14"
crossScalaVersions += "2.13.6"

libraryDependencies ++=
  "com.datadoghq" % "dd-trace-api" % "0.42.0" ::
    "com.google.auto.service" % "auto-service-annotations" % "1.0-rc6" ::
    // "com.twitter" %% "finagle-core" % "19.13.0" ::
    "com.twitter" %% "finagle-base-http" % "19.12.0" ::
    "org.msgpack" % "jackson-dataformat-msgpack" % "0.8.20" ::
    Nil
