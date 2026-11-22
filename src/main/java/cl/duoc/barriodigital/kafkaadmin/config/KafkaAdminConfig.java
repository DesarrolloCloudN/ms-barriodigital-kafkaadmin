package cl.duoc.barriodigital.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

// Único bean de configuración: el resto del microservicio administra el clúster solo a través
// de este AdminClient, nunca llamando directo a la librería de Kafka (mismo rol que
// RabbitAdminConfig cumple para RabbitAdmin).
@Configuration
public class KafkaAdminConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean(destroyMethod = "close")
	public AdminClient adminClient() {
		return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
	}

}
