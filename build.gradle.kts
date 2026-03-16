plugins {
    `java-library`
    id("maven-publish")
    id ("jacoco")
}

group = "com.alert"
version = "1.2.5"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

val springBootVersion = "4.0.2"

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:$springBootVersion"))

    implementation("org.springframework.boot:spring-boot-autoconfigure")
    compileOnly("org.springframework.boot:spring-boot-starter-web")
    compileOnly("org.springframework.boot:spring-boot-starter-webflux")
    compileOnly("org.springframework.boot:spring-boot-starter-data-redis")
    compileOnly("org.springframework.boot:spring-boot-starter-data-redis-reactive")
    compileOnly("io.micrometer:micrometer-core")
    compileOnly("org.springframework.kafka:spring-kafka")
    compileOnly("io.projectreactor.kafka:reactor-kafka:1.3.25")
    compileOnly("net.gpedro.integrations.slack:slack-webhook:1.4.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.springframework.boot:spring-boot-starter-data-redis")
    testImplementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
    testImplementation("org.springframework.kafka:spring-kafka")
    testImplementation("io.projectreactor.kafka:reactor-kafka:1.3.25")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("com.fasterxml.jackson.core:jackson-databind")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    testImplementation("io.micrometer:micrometer-core")
    testImplementation("net.gpedro.integrations.slack:slack-webhook:1.4.0")
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}

tasks.jacocoTestReport {
    reports {
        html.required.set(true)
        xml.required.set(true)
        csv.required.set(false)
    }
    val excluded = listOf(
        "**/config/**"
    )

    classDirectories.setFrom(
        files(
            classDirectories.files.map {
                fileTree(it) {
                    exclude(excluded)
                }
            }
        )
    )
}
