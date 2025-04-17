package org.chenile.pubsub.kafka;

import org.chenile.pubsub.kafka.service.TestService;
import org.chenile.pubsub.kafka.service.TestServiceImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ActiveProfiles;

@Configuration
@SpringBootApplication(scanBasePackages = {  "org.chenile.configuration","org.chenile.pubsub.configuration",
			"org.chenile.pubsub.kafka.service","org.chenile.pubsub.kafka.configuration"})
@PropertySource("classpath:org/chenile/pubsub/kafka/TestKafka.properties")
@ActiveProfiles("unittest")
public class SpringConfig extends SpringBootServletInitializer{
	
	public static void main(String[] args) {
		SpringApplication.run(SpringConfig.class, args);
	}
	
	@Bean("testService") public TestService testService() {
		return new TestServiceImpl();
	}
	@Bean SharedData sharedData() { return new SharedData();}
}

