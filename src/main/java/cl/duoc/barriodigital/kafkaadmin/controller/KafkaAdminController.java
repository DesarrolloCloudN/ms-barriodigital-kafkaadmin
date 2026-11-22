package cl.duoc.barriodigital.kafkaadmin.controller;

import cl.duoc.barriodigital.kafkaadmin.dto.ConsumerGroupInfo;
import cl.duoc.barriodigital.kafkaadmin.dto.TopicInfo;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

// Administración de tópicos y consumer groups de Kafka, según la tabla de endpoints de la
// guía EP3. Todo queda delegado a KafkaAdminService; el controller solo valida la forma HTTP
// de la petición (mismo rol que RabbitMQController cumple para RabbitMQ en EP2).
@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

	private final KafkaAdminService kafkaAdminService;

	public KafkaAdminController(KafkaAdminService kafkaAdminService) {
		this.kafkaAdminService = kafkaAdminService;
	}

	@PostMapping("/topics")
	@ResponseStatus(HttpStatus.CREATED)
	public Map<String, Object> crearTopico(
			@RequestParam String topicName,
			@RequestParam(defaultValue = "3") int partitions,
			@RequestParam(defaultValue = "3") short replicationFactor) {
		kafkaAdminService.crearTopico(topicName, partitions, replicationFactor);
		return Map.of("topicName", topicName, "partitions", partitions, "replicationFactor", replicationFactor);
	}

	@GetMapping("/topics")
	public List<String> listarTopicos() {
		return kafkaAdminService.listarTopicos();
	}

	@GetMapping("/topics/{topicName}")
	public TopicInfo consultarTopico(@PathVariable String topicName) {
		return kafkaAdminService.consultarTopico(topicName);
	}

	@PutMapping("/topics/{topicName}/config")
	public Map<String, Object> actualizarConfiguracion(@PathVariable String topicName, @RequestBody Map<String, String> configuraciones) {
		kafkaAdminService.actualizarConfiguracion(topicName, configuraciones);
		return Map.of("topicName", topicName, "configuracionActualizada", configuraciones);
	}

	@DeleteMapping("/topics/{topicName}")
	@ResponseStatus(HttpStatus.NO_CONTENT)
	public void eliminarTopico(@PathVariable String topicName) {
		kafkaAdminService.eliminarTopico(topicName);
	}

	@GetMapping("/consumer-groups")
	public List<String> listarConsumerGroups() {
		return kafkaAdminService.listarConsumerGroups();
	}

	@GetMapping("/consumer-groups/{groupId}")
	public ConsumerGroupInfo consultarConsumerGroup(@PathVariable String groupId) {
		return kafkaAdminService.consultarConsumerGroup(groupId);
	}

}
