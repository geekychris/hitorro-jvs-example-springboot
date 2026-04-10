package com.hitorro.jvs.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * HiTorro JVS Example Application.
 *
 * Demonstrates using the hitorro-jvs library (type system + NLP) with Spring Boot.
 *
 * Endpoints:
 *   POST /api/jvs/documents        - Create and validate a JVS document
 *   POST /api/jvs/merge            - Merge two JVS documents
 *   POST /api/jvs/stem             - Stem text using Snowball stemmer
 *   POST /api/jvs/enrich           - Create a document and enrich with NLP stems
 */
@SpringBootApplication
public class JvsExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(JvsExampleApplication.class, args);
    }
}
