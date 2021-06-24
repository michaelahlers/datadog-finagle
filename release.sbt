import ReleaseTransformations._
releaseProcess -= publishArtifacts
releaseTagName := s"finagle-21.6_${(ThisBuild / version).value}"
