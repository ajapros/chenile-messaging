package org.chenile.pubsub.azure.configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertySource;

import java.io.IOException;
import java.util.List;

public class ChenileEventHubPropertiesBindingTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(TestConfiguration.class);

    @Test
    void bindsRoutesAndDefaultRouteFromChenileProperties() {
        contextRunner
                .withInitializer(context -> addYaml(context, "config/valid-routing.yml"))
                .run(context -> {
                    ChenileEventHubProperties properties = context.getBean(ChenileEventHubProperties.class);

                    Assertions.assertEquals("business-events", properties.resolvePhysicalHubName("order-created"));
                    Assertions.assertEquals("billing-events", properties.resolvePhysicalHubName("invoice-paid"));
                    Assertions.assertEquals("shared-events", properties.resolvePhysicalHubName("audit-created"));
                });
    }

    @Test
    void clientPrefixIsDisabledByDefault() {
        contextRunner
                .withInitializer(context -> addYaml(context, "config/valid-routing.yml"))
                .run(context -> {
                    ChenileEventHubProperties properties = context.getBean(ChenileEventHubProperties.class);

                    Assertions.assertFalse(properties.isClientPrefixEnabled());
                });
    }

    @Test
    void blankDefaultRouteFailsWhenUsed() {
        contextRunner
                .withInitializer(context -> addYaml(context, "config/invalid-default-route.yml"))
                .run(context -> {
                    ChenileEventHubProperties properties = context.getBean(ChenileEventHubProperties.class);

                    IllegalStateException exception = Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> properties.resolvePhysicalHubName("audit-created")
                    );

                    Assertions.assertTrue(exception.getMessage().contains("audit-created"));
                });
    }

    @Configuration
    @EnableConfigurationProperties(ChenileEventHubProperties.class)
    static class TestConfiguration {
    }

    private static void addYaml(org.springframework.context.ConfigurableApplicationContext context, String path) {
        try {
            YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
            List<PropertySource<?>> propertySources = loader.load(path, new ClassPathResource(path));
            propertySources.forEach(source ->
                    context.getEnvironment().getPropertySources().addLast(source));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load YAML test resource: " + path, e);
        }
    }
}
