inThisBuild {
  val scala212 = "2.12.17"
  val scala213 = "2.13.10"
  val scala3   = "3.3.0"

  List(
    scalaVersion       := scala213,
    crossScalaVersions := Seq(scala212, scala213, scala3),
    githubWorkflowBuild := Seq(
      WorkflowStep.Sbt(
        name = Option("Build & Test"),
        commands = List("clean", "coverage", "test"),
        cond = None,
        env = Map.empty
      ),
      WorkflowStep.Sbt(
        name = Option("Coverage"),
        commands = List("coverageAggregate"),
        cond = None,
        env = Map.empty
      )
    ),
    githubWorkflowTargetTags ++= Seq("v*"),
    githubWorkflowPublishTargetBranches := Seq(
      RefPredicate.StartsWith(Ref.Tag("v")),
      RefPredicate.Equals(Ref.Branch("main"))
    ),
    githubWorkflowPublish := Seq(
      WorkflowStep.Sbt(
        commands = List("ci-release"),
        name = Some("Publish project"),
        env = Map(
          "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
          "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
          "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
          "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
        )
      )
    ),
    testFrameworks ++= Seq(
      new TestFramework("zio.test.sbt.ZTestFramework"),
      new TestFramework("weaver.framework.CatsEffect")
    ),
    semanticdbEnabled      := true,
    versionScheme          := Some("early-semver"),
    sonatypeCredentialHost := "s01.oss.sonatype.org",
    sonatypeProfileName    := "io.kaizen-solutions",
    sonatypeRepository     := "https://s01.oss.sonatype.org/service/local",
    licenses               := List("MPL-2.0" -> url("https://www.mozilla.org/en-US/MPL/2.0/")),
    organization           := "io.kaizen-solutions",
    organizationName       := "kaizen-solutions",
    homepage               := Some(url("https://www.kaizen-solutions.io/")),
    developers := List(
      Developer("calvinlfer", "Calvin Fernandes", "cal@kaizen-solutions.io", url("https://www.kaizen-solutions.io"))
    ),
    sonatypeCredentialHost := "s01.oss.sonatype.org"
  )
}

lazy val root =
  project
    .in(file("."))
    .settings(publish / skip := true)
    .aggregate(core, zio, catsEffect)

lazy val core =
  (project in file("core"))
    .settings(
      name := "virgil-core",
      libraryDependencies ++= {
        val datastax  = "com.datastax.oss"
        val datastaxV = "4.15.0"

        val zio                   = "dev.zio"
        val zioV                  = "2.0.14"
        val magnoliaForScala2     = "com.softwaremill.magnolia1_2" %% "magnolia"      % "1.1.3"
        val scalaReflectForScala2 = "org.scala-lang"                % "scala-reflect" % scalaVersion.value
        val magnoliaForScala3     = "com.softwaremill.magnolia1_3" %% "magnolia"      % "1.3.0"

        val coreDependencies =
          Seq(
            datastax                  % "java-driver-core"        % datastaxV,
            "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0",
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

lazy val zio =
  project
    .in(file("zio"))
    .settings(
      name := "virgil-zio",
      libraryDependencies ++= {
        val zio  = "dev.zio"
        val zioV = "2.0.14"

        Seq(
          zio %% "zio"         % zioV,
          zio %% "zio-streams" % zioV
        )
      }
    )
    .dependsOn(core % "compile->compile;test->test")

lazy val catsEffect =
  project
    .in(file("cats-effect"))
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
    .dependsOn(core % "compile->compile;test->test")
