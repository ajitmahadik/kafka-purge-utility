package com.utility.kafka.purgeutility;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaAdmin;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.admin.KafkaAdminClient.*;

@SpringBootApplication
public class PurgeUtilityApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(PurgeUtilityApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println("Kafka Purge Utility");
		Map<String, Object> props = new HashMap<>();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
		props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "LOCAL-HOST");

		ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, "output");
		Collection<ConfigEntry> entries = new ArrayList<>();
		entries.add(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(1)));

		Config config = new Config(entries);
		Map<ConfigResource, Config> configs = new HashMap<>();
		configs.put(cr, config);

		System.out.println("---------------------------");

		try (AdminClient client = create(props)) {
			client.alterConfigs(configs);
		}

		TimeUnit.SECONDS.sleep(5);

		System.out.println("Purge Kafka topic");
	}
}
