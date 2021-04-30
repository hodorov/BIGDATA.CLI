import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "2.4.4"
    id("io.spring.dependency-management") version "1.0.11.RELEASE"
    kotlin("jvm") version "1.4.31"
    kotlin("plugin.spring") version "1.4.31"
    kotlin("kapt") version "1.4.31"
}

group = "ru.hodorov"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
}

val kotlinLoggingVersion = "2.0.6"
val springShellVersion = "2.0.0.RELEASE"
val hadoopVersion = "3.3.0"
val avroVersion = "1.10.2"
val parquetHadoopVersion = "1.12.0"
val snappyVersion = "1.1.8.4"
val jacksonVersion = "2.11.4" // Inherited from SpringBoot

// From Hadoop libraries
configurations {
    all {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
}

dependencies {
    // Spring
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-aop")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    kapt("org.springframework.boot:spring-boot-configuration-processor")

    // CLI
    implementation("org.springframework.shell:spring-shell-starter:$springShellVersion")
    implementation("javax.el:javax.el-api:3.0.0")

    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation("io.github.microutils:kotlin-logging-jvm:$kotlinLoggingVersion")

    // Hadoop
    implementation("org.apache.hadoop:hadoop-client:$hadoopVersion")
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("org.apache.parquet:parquet-hadoop:${parquetHadoopVersion}")
    implementation("org.xerial.snappy:snappy-java:$snappyVersion")
//    implementation("org.apache.hadoop:hadoop-hdfs:$hadoopVersion")

    // Jackson
    implementation ("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation ("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "1.8"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
