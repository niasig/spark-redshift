package com.signifyd.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.scala.ScalaCompile

/**
 * Plugin to run integration tests
 */
class ScalaProject implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.apply(plugin: 'scala')
        // Required to deal with this issue on encrypted linux machines
        // https://stackoverflow.com/questions/28565837/filename-too-long-sbt
        project.tasks.withType(ScalaCompile).configureEach { t ->
            t.getScalaCompileOptions().additionalParameters = ["-Xmax-classfile-name", "78"]
        }
        project.dependencies {
            compile project.ext.libs.'scala-library'
            compile project.ext.libs.'scala-reflect'
            compile project.ext.libs.'scala-java8-compat'
            // This is a hack to get around scala library conflicts between 2.11 and zinc.
            // See https://discuss.gradle.org/t/default-scala-compiler-doesnt-work-in-gradle-2-12-scala-2-11/15328/5
            zinc 'com.typesafe.zinc:zinc:0.3.15'
            zinc 'org.scala-lang:scala-library:2.10.6'
        }
    }
}
