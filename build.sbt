import ReleaseTransformations._

inThisBuild(
  List(
    scalaVersion                        := "2.13.8",
    crossScalaVersions                  := Seq("2.13.8", "2.12.15"),
    githubWorkflowPublishTargetBranches := Seq.empty,
    githubWorkflowBuild += WorkflowStep.Sbt(
      name = Option("Coverage Coveralls"),
      commands = List("clean", "coverage", "test", "coverageReport", "coverageAggregate", "coveralls"),
      env = Map(
        "COVERALLS_REPO_TOKEN" -> "${{ secrets.GITHUB_TOKEN }}",
        "COVERALLS_FLAG_NAME"  -> "Scala ${{ matrix.scala }}"
      )
    )
  )
)
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
        val datastaxV = "4.14.0"

        val zio  = "dev.zio"
        val zioV = "1.0.13"

        Seq(
          datastax                        % "java-driver-core"        % datastaxV,
          "org.scala-lang.modules"       %% "scala-collection-compat" % "2.7.0",
          "com.softwaremill.magnolia1_2" %% "magnolia"                % "1.1.2",
          "org.scala-lang"                % "scala-reflect"           % scalaVersion.value,
          zio                            %% "zio"                     % zioV,
          zio                            %% "zio-streams"             % zioV,
          zio                            %% "zio-test"                % zioV     % Test,
          zio                            %% "zio-test-sbt"            % zioV     % Test,
          "com.dimafeng"                 %% "testcontainers-scala"    % "0.40.4" % Test,
          "com.outr"                     %% "scribe-slf4j"            % "3.8.2"  % Test
        )
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
