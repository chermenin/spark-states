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

name := "spark-states"

version := "0.2"

crossScalaVersions := Seq(Versions.Scala_2_11, Versions.Scala_2_12)

libraryDependencies ++= Seq(

  // general dependencies
  "org.apache.spark" %% "spark-sql" % Versions.Spark % "provided",
  "org.apache.spark" %% "spark-streaming" % Versions.Spark % "provided",
  "org.rocksdb" % "rocksdbjni" % Versions.RocksDb,

  // test dependencies
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-sql" % Versions.Spark % "test" classifier "tests",
  "com.google.guava" % "guava-testlib" % "14.0.1" % "test"
)
