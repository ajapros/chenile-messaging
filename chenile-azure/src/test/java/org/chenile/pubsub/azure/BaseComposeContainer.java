package org.chenile.pubsub.azure;

import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


@Testcontainers
public abstract class BaseComposeContainer {

    static Map<String, String> map = new HashMap<>();


    static {
        File configFile = new File("src/test/resources/Config.json");


        map.put("CONFIG_PATH",configFile.getAbsolutePath());
        map.put("ACCEPT_EULA","Y");


    }

    static File composeFile = new File(
            ClassLoader.getSystemResource("docker-compose.yml").getFile()
    );

    @Container
    public static DockerComposeContainer<?> environment =
            new DockerComposeContainer<>("junut",composeFile)
                    .withEnv(map)
            ;



}
