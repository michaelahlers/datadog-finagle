//githubOwner := "michaelahlers"
//githubRepository := "datadog-finagle"

publishMavenStyle := false
publishTo := Some(Resolver.url("Ahlers Consulting Artifacts (public)", url("s3://ahlers-consulting-artifacts-public.s3.amazonaws.com/"))(Resolver.ivyStylePatterns))
