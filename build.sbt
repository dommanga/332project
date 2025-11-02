import scalapb.compiler.Version.scalapbVersion

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
    )
  )