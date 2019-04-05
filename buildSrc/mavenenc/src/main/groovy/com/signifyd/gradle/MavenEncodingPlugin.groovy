package com.signifyd.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.ApplicationPlugin
import org.gradle.play.plugins.PlayPlugin

/**
 * Plugin to check for the deprecated mavenPassword and decode mavenEncPassword.
 */
class MavenEncodingPlugin implements Plugin<Project> {

    @Override
    void apply(Project project) {
        if (project.hasProperty("useSecretTool")) {
            Process process = new ProcessBuilder("secret-tool", "lookup", "gradle", "nexus").start()
            if (process.waitFor() == 0) {
                project.ext.nexusPassword = process.inputStream.text
            }
        } else {

            if (project.hasProperty("nexusPassword")) {
                println "WARNING: nexusPassword property found!"
                println "Run 'encode_nexus_password.sh' in the provysyoner repo to setup nexusEncPassword"
                println "Be sure to remove `nexusPassword` from ~/.gradle/gradle.properties\n\n"
                sleep(5000)
            }

            if (!project.hasProperty("nexusEncPassword")) {
                println "WARNING: Encrypted creds not set!"
                println "Run 'encode_nexus_password.sh' in the provysyoner repo to setup nexusEncPassword"
                println "Be sure to remove `nexusPassword` from ~/.gradle/gradle.properties\n\n"
                sleep(5000)
            } else {
                def decodedPassword = project.ext.nexusEncPassword.decodeBase64()
                def nexusDecPassword = new String(decodedPassword).trim()
                project.ext.nexusPassword = nexusDecPassword
            }
        }
    }
}
