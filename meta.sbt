name := "DD Finagle"
moduleName := "dd-finagle"

description := "Datadog reporter for Finagle."

organization := "com.datadoghq"
organizationName := "Datadog"

licenses += ("The Apache Software License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

developers += Developer("datadog", "Datadog", "info@datadoghq.com", url("http://github.com/michaelahlers"))

scmInfo :=
  Some(ScmInfo(
    browseUrl = url("https://www.github.com/DataDog/datadog-finagle"),
    connection = "scm:git:git@github.com:DataDog/datadog-finagle.git",
    devConnection = Some("scm:git:git@github.com:DataDog/datadog-finagle.git")
  ))
