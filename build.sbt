/*
 * Copyright 2018 Aleksandr Chermenin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

organization := "ru.chermenin"
name := "spark-states"

version := "0.9.9"

crossScalaVersions := Seq("2.11.12", "2.12.7")

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(

  // general dependencies
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.rocksdb" % "rocksdbjni" % "5.17.2",

  // test dependencies
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "com.google.guava" % "guava" % "14.0.1" % "test", // override version because older version check expireAfterAccess as ">" instead of ">=" which makes unit tests fail
  "com.google.guava" % "guava-testlib" % "14.0.1" % "test",
  "com.github.dblock" % "oshi-core" % "3.4.0" % "test"
)
