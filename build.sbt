import scalapb.compiler.Version.scalapbVersion
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.{MergeStrategy, PathList}

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "332project",
    version := "0.1.0",
    organization := "postech.csed332",

    // ScalaPB configuration with gRPC
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),

    libraryDependencies ++= Seq(
      "io.grpc" % "grpc-netty" % "1.63.0",
      "io.grpc" % "grpc-protobuf" % "1.63.0",
      "io.grpc" % "grpc-stub" % "1.63.0",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.11.15",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),

    assembly / mainClass := Some("entry.Main"),

    assembly / assemblyJarName := "dist-sort.jar",

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "io.netty.versions.properties") =>
        MergeStrategy.first

      case PathList("META-INF", "services", xs @ _*) =>
        MergeStrategy.concat

      case PathList("META-INF", "MANIFEST.MF") =>
        MergeStrategy.discard

      case PathList("META-INF", xs @ _*) =>
        MergeStrategy.discard

      case _ =>
        MergeStrategy.first
        }
  )