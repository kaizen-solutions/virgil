import ReleaseTransformations._

inThisBuild {
  val scala212 = "2.12.16"
  val scala213 = "2.13.8"
  val scala3   = "3.1.3"

  List(
    scalaVersion                        := scala3,
    crossScalaVersions                  := Seq(scala212, scala213, scala3),
    githubWorkflowPublishTargetBranches := Seq.empty,
    githubWorkflowBuild += WorkflowStep.Sbt(
      name = Option("Coverage Coveralls"),
      commands = List("clean", "coverage", "test", "coverageReport", "coverageAggregate", "coveralls"),
      cond = Some("${{ matrix.scala != '3.1.2' }}"), // Disable coverage for Scala 3.1.2
      env = Map(
        "COVERALLS_REPO_TOKEN" -> "${{ secrets.GITHUB_TOKEN }}",
        "COVERALLS_FLAG_NAME"  -> "Scala ${{ matrix.scala }}"
      )
    )
  )
}
addCommandAlias("coverme", "; clean; coverage; test; coverageReport; coverageAggregate")

lazy val root =
  (project in file("."))
    .settings(
      licenses         := List("MPL-2.0" -> url("https://www.mozilla.org/en-US/MPL/2.0/")),
      organization     := "io.kaizensolutions",
      organizationName := "kaizen-solutions",
      name             := "virgil",
      libraryDependencies ++= {
        val datastax  = "com.datastax.oss"
        val datastaxV = "4.14.1"

        val zio                   = "dev.zio"
        val zioV                  = "2.0.0"
        val magnoliaForScala2     = "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.2"
        val scalaReflectForScala2 = "org.scala-lang"                % "scala-reflect" % scalaVersion.value
        val magnoliaForScala3     = "com.softwaremill.magnolia1_3" %% "magnolia"      % "1.1.4"

        val coreDependencies =
          Seq(
            datastax                  % "java-driver-core"        % datastaxV,
            "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.0",
            zio                      %% "zio"                     % zioV,
            zio                      %% "zio-streams"             % zioV,
            zio                      %% "zio-test"                % zioV     % Test,
            zio                      %% "zio-test-sbt"            % zioV     % Test,
            "com.dimafeng"           %% "testcontainers-scala"    % "0.40.8" % Test,
            "com.outr"               %% "scribe-slf4j"            % "3.10.1" % Test
          )

        val magnolia =
          if (scalaVersion.value.startsWith("2")) Seq(magnoliaForScala2, scalaReflectForScala2)
          else Seq(magnoliaForScala3)

        coreDependencies ++ magnolia
      },
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
      Test / fork                 := true,
      releaseIgnoreUntrackedFiles := true,
      releaseTagName              := s"${version.value}",
      releaseProcess := Seq[ReleaseStep](
        checkSnapshotDependencies,
        inquireVersions,
        runClean,
        runTest,
        setReleaseVersion,
        commitReleaseVersion,
        tagRelease,
        setNextVersion,
        commitNextVersion,
        pushChanges
      )
    )
