import ReleaseTransformations._

ThisBuild / scalaVersion                        := "2.13.8"
ThisBuild / crossScalaVersions                  := Seq("2.13.8", "2.12.15")
ThisBuild / githubWorkflowPublishTargetBranches := Seq.empty

lazy val root =
  (project in file("."))
    .settings(
      licenses         := List("MPL-2.0" -> url("https://www.mozilla.org/en-US/MPL/2.0/")),
      organization     := "io.kaizensolutions",
      organizationName := "kaizen-solutions",
      name             := "virgil",
      addCompilerPlugin("io.tryp" % "splain" % "1.0.0" cross CrossVersion.patch),
      libraryDependencies ++= {
        val datastax  = "com.datastax.oss"
        val datastaxV = "4.14.0"

        val zio  = "dev.zio"
        val zioV = "1.0.13"

        Seq(
          datastax                        % "java-driver-core"        % datastaxV,
          "org.scala-lang.modules"       %% "scala-collection-compat" % "2.6.0",
          "com.softwaremill.magnolia1_2" %% "magnolia"                % "1.1.1",
          "org.scala-lang"                % "scala-reflect"           % scalaVersion.value,
          zio                            %% "zio"                     % zioV,
          zio                            %% "zio-streams"             % zioV,
          zio                            %% "zio-test"                % zioV     % Test,
          zio                            %% "zio-test-sbt"            % zioV     % Test,
          zio                            %% "zio-schema"              % "0.1.8",
          zio                            %% "zio-schema-derivation"   % "0.1.8",
          "com.dimafeng"                 %% "testcontainers-scala"    % "0.40.2" % Test,
          "com.outr"                     %% "scribe-slf4j"            % "3.8.0"  % Test
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
