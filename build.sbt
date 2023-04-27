import ReleaseTransformations._

inThisBuild {
  val scala212 = "2.12.17"
  val scala213 = "2.13.10"
  val scala3   = "3.2.2"

  List(
    scalaVersion                        := scala3,
    crossScalaVersions                  := Seq(scala212, scala213, scala3),
    githubWorkflowPublishTargetBranches := Seq.empty,
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(
        name = Option("Build & Test"),
        commands = List("clean", "coverage", "test"),
        cond = None,
        env = Map.empty
      ),
      WorkflowStep.Sbt(
        name = Option("Coverage"),
        commands = List("coverageReport"), // coveralls is temporarily disabled
        cond = None,
        env = Map(
          "COVERALLS_REPO_TOKEN" -> "${{ secrets.GITHUB_TOKEN }}",
          "COVERALLS_FLAG_NAME"  -> "Scala ${{ matrix.scala }}"
        )
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
        val datastaxV = "4.15.0"

        val zio                   = "dev.zio"
        val zioV                  = "2.0.13"
        val magnoliaForScala2     = "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.3"
        val scalaReflectForScala2 = "org.scala-lang"                % "scala-reflect" % scalaVersion.value
        val magnoliaForScala3     = "com.softwaremill.magnolia1_3" %% "magnolia"      % "1.3.0"

        val coreDependencies =
          Seq(
            datastax                  % "java-driver-core"        % datastaxV,
            "org.scala-lang.modules" %% "scala-collection-compat" % "2.10.0",
            zio                      %% "zio"                     % zioV,
            zio                      %% "zio-streams"             % zioV,
            zio                      %% "zio-test"                % zioV      % Test,
            zio                      %% "zio-test-sbt"            % zioV      % Test,
            "com.dimafeng"           %% "testcontainers-scala"    % "0.40.15" % Test,
            "com.outr"               %% "scribe-slf4j"            % "3.11.1"  % Test
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
