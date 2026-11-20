package com.hitorro.jvs.example;

import com.hitorro.util.json.JsonMappers;
import org.springframework.boot.jackson.autoconfigure.JsonFactoryBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * Keeps the REST/JSON format the app had on Jackson 2.
 *
 * <p>{@code spring.jackson.use-jackson2-defaults=true} (application.yml) gives Boot's Jackson 3
 * {@code JsonMapper} the mapper settings Boot's Jackson 2 mapper had: Jackson 2 defaults
 * ({@code configureForJackson2()}: declaration-order properties, lenient reads) plus Boot's own
 * ISO-8601 dates, ignored unknown properties and no default view inclusion. Neither sets the
 * <em>factory</em>: Jackson 3 halves the nesting limit (500 instead of 1000) and changes the string
 * limit, so this customizer applies hitorro-core's Jackson 2.18 limits and factory defaults, the same
 * ones {@link JsonMappers#builder()} uses. It runs after Boot's own factory customizer, which sets the
 * limits from {@code spring.jackson.factory.constraints.*}.
 */
@Configuration(proxyBeanMethods = false)
public class JsonConfig {

    @Bean
    @Order(Ordered.LOWEST_PRECEDENCE)
    JsonFactoryBuilderCustomizer jackson2StreamLimits() {
        return builder -> builder.configureForJackson2()
                .streamReadConstraints(JsonMappers.JACKSON2_READ_CONSTRAINTS)
                .streamWriteConstraints(JsonMappers.JACKSON2_WRITE_CONSTRAINTS);
    }
}
