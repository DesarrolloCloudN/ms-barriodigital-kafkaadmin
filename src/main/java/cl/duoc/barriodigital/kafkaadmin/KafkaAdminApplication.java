package cl.duoc.barriodigital.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Microservicio administrador de Kafka: crea, consulta, configura y elimina tópicos, e
// inspecciona consumer groups, vía REST. No conoce el dominio de trámites; solo administra el
// clúster, igual rol que ms-barriodigital-rabbitadmin cumple para RabbitMQ (EP2).
@SpringBootApplication
public class KafkaAdminApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaAdminApplication.class, args);
	}

}
