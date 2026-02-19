package org.chenile.pubsub.jvm;

import org.chenile.pubsub.jvm.service.TestService;
import org.chenile.pubsub.jvm.service.TestServiceImpl;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.ActiveProfiles;

@Configuration
@SpringBootApplication(scanBasePackages = {
        "org.chenile.configuration",
        "org.chenile.pubsub.configuration",
        "org.chenile.pubsub.jvm.configuration",
        "org.chenile.pubsub.jvm.service"
})
@PropertySource("classpath:org/chenile/pubsub/jvm/TestJVM.properties")
@ActiveProfiles("unittest")
@EnableScheduling
public class SpringConfig extends SpringBootServletInitializer {

    @Bean("testService")
    public TestService testService() {
        return new TestServiceImpl();
    }

    @Bean
    SharedData sharedData() {
        return new SharedData();
    }
}
