import ReleaseTransformations._

inThisBuild {
  val scala212 = "2.12.17"
  val scala213 = "2.13.10"
  val scala3   = "3.3.0"

  List(
    scalaVersion                        := scala213,
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
    ),
    testFrameworks ++= Seq(
      new TestFramework("zio.test.sbt.ZTestFramework"),
      new TestFramework("weaver.framework.CatsEffect")
    ),
    semanticdbEnabled                              := true,
    scalafixDependencies += "com.github.liancheng" %% "organize-imports" % "0.6.0"
  )
}

lazy val root =
  project
    .in(file("."))
    .settings(publishTo := None)
    .aggregate(core, zio, catsEffect)

lazy val core =
  (project in file("core"))
    .settings(organizationSettings *)
    .settings(
      name := "virgil-core",
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
            zio                      %% "zio-test"                % zioV      % Test,
            zio                      %% "zio-test-scalacheck"     % zioV      % Test,
            zio                      %% "zio-test-sbt"            % zioV      % Test,
            "com.dimafeng"           %% "testcontainers-scala"    % "0.40.16" % Test,
            "com.outr"               %% "scribe-slf4j"            % "3.11.5"  % Test
          )

        val isScala2x = scalaVersion.value.startsWith("2")

        val magnolia =
          if (isScala2x) Seq(magnoliaForScala2, scalaReflectForScala2)
          else Seq(magnoliaForScala3)

        val betterMonadicFor =
          if (isScala2x) Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
          else Seq.empty

        coreDependencies ++ magnolia ++ betterMonadicFor
      },
      Test / fork := true
    )
    .settings(releaseSettings *)

lazy val zio =
  project
    .in(file("zio"))
    .settings(organizationSettings *)
    .settings(
      name := "virgil-zio",
      libraryDependencies ++= {
        val zio  = "dev.zio"
        val zioV = "2.0.13"

        Seq(
          zio %% "zio"         % zioV,
          zio %% "zio-streams" % zioV
        )
      }
    )
    .settings(releaseSettings *)
    .dependsOn(core % "compile->compile;test->test")

lazy val catsEffect =
  project
    .in(file("cats-effect"))
    .settings(organizationSettings *)
    .settings(
      name := "virgil-cats-effect",
      libraryDependencies ++= {
        val disney  = "com.disneystreaming"
        val weaverV = "0.8.3"
        Seq(
          "org.typelevel" %% "cats-effect"       % "3.5.0",
          "co.fs2"        %% "fs2-core"          % "3.7.0",
          "co.fs2"        %% "fs2-io"            % "3.7.0",
          disney          %% "weaver-cats"       % weaverV % Test,
          disney          %% "weaver-scalacheck" % weaverV % Test
        )
      },
      Test / fork := true
    )
    .settings(releaseSettings)
    .dependsOn(core % "compile->compile;test->test")

def organizationSettings =
  Seq(
    licenses         := List("MPL-2.0" -> url("https://www.mozilla.org/en-US/MPL/2.0/")),
    organization     := "io.kaizensolutions",
    organizationName := "kaizen-solutions"
  )

def releaseSettings = Seq(
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
