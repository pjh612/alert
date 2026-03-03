import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    id("java")
    id("org.springframework.boot") version "4.0.2"
    id("io.spring.dependency-management") version "1.1.4"
    id("maven-publish")
    id ("jacoco")
}

group = "com.alert"
version = "1.2.5"

tasks.named<Jar>("jar") {
    archiveClassifier.set("") // plain 제거, 기본 jar 이름으로 설정
}

tasks.getByName<BootJar>("bootJar") {
    enabled = false
}
tasks.getByName<Jar>("jar") {
    enabled = true
}
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    compileOnly("org.springframework.boot:spring-boot-starter-web")
    compileOnly("org.springframework.boot:spring-boot-starter-webflux")
    compileOnly("org.springframework.boot:spring-boot-starter-data-redis")
    compileOnly("org.springframework.boot:spring-boot-starter-data-redis-reactive")
    compileOnly("io.micrometer:micrometer-core")
    implementation("org.springframework.kafka:spring-kafka")
    implementation ("io.projectreactor.kafka:reactor-kafka:1.3.25")
    implementation("net.gpedro.integrations.slack:slack-webhook:1.4.0")
    implementation("org.springframework.boot:spring-boot-autoconfigure")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation ("io.projectreactor:reactor-test")
    testRuntimeOnly ("org.junit.platform:junit-platform-launcher")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation ("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.springframework.boot:spring-boot-starter-data-redis")
    testImplementation("com.fasterxml.jackson.core:jackson-databind")
    testImplementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
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