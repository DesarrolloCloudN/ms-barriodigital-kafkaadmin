package cl.duoc.barriodigital.kafkaadmin.service;

import cl.duoc.barriodigital.kafkaadmin.config.KafkaNames;
import cl.duoc.barriodigital.kafkaadmin.dto.ConsumerGroupInfo;
import cl.duoc.barriodigital.kafkaadmin.dto.ConsumerGroupOffsetInfo;
import cl.duoc.barriodigital.kafkaadmin.dto.PartitionInfo;
import cl.duoc.barriodigital.kafkaadmin.dto.TopicInfo;
import cl.duoc.barriodigital.kafkaadmin.exception.ParametroInvalidoException;
import cl.duoc.barriodigital.kafkaadmin.exception.RecursoNoEncontradoException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

// Único punto del microservicio que llama al AdminClient de Kafka: los controllers no conocen
// la librería de Kafka, solo invocan estos métodos y reciben DTOs o excepciones de dominio
// (mismo rol que RabbitAdminService cumple para RabbitMQ en EP2).
@Service
public class KafkaAdminService {

	private final AdminClient adminClient;

	public KafkaAdminService(AdminClient adminClient) {
		this.adminClient = adminClient;
	}

	public String crearTopico(String nombre, int particiones, short factorReplicacion) {
		exigirNoVacio(nombre, "topicName");
		if (particiones <= 0) {
			throw new ParametroInvalidoException("partitions debe ser mayor que 0");
		}
		if (factorReplicacion <= 0) {
			throw new ParametroInvalidoException("replicationFactor debe ser mayor que 0");
		}
		ejecutar(adminClient.createTopics(List.of(new NewTopic(nombre, particiones, factorReplicacion))).all(),
				"No se pudo crear el tópico " + nombre);
		return nombre;
	}

	public List<String> listarTopicos() {
		return new ArrayList<>(ejecutar(adminClient.listTopics().names(), "No se pudo listar los tópicos"));
	}

	public TopicInfo consultarTopico(String nombre) {
		exigirNoVacio(nombre, "topicName");
		TopicDescription descripcion = describirTopico(nombre);

		List<PartitionInfo> detalleParticiones = descripcion.partitions().stream()
				.map(p -> new PartitionInfo(
						p.partition(),
						p.leader() != null ? p.leader().id() : -1,
						p.replicas().stream().map(n -> n.id()).toList(),
						p.isr().stream().map(n -> n.id()).toList()))
				.toList();

		int factorReplicacion = descripcion.partitions().isEmpty() ? 0 : descripcion.partitions().get(0).replicas().size();

		return new TopicInfo(nombre, descripcion.partitions().size(), factorReplicacion,
				detalleParticiones, obtenerConfiguracion(nombre));
	}

	// Solo actualiza (SET) las entradas recibidas, p.ej. {"retention.ms": "604800000",
	// "cleanup.policy": "delete"}; el resto de la configuración del tópico queda intacta.
	public void actualizarConfiguracion(String nombre, Map<String, String> configuraciones) {
		exigirNoVacio(nombre, "topicName");
		if (configuraciones == null || configuraciones.isEmpty()) {
			throw new ParametroInvalidoException("Debe enviar al menos un parámetro de configuración");
		}
		describirTopico(nombre); // valida que el tópico exista antes de intentar configurarlo
		ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
		List<AlterConfigOp> operaciones = configuraciones.entrySet().stream()
				.map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
				.toList();
		ejecutar(adminClient.incrementalAlterConfigs(Map.of(recurso, operaciones)).all(),
				"No se pudo actualizar la configuración de " + nombre);
	}

	// Solo permite borrar tópicos que NO sean los productivos del caso (tramites-eventos,
	// tramites-auditoria y sus .DLT): esta API es para limpiar tópicos temporales de prueba
	// durante la demo, nunca para borrar los tópicos en uso (ver guía EP3).
	public void eliminarTopico(String nombre) {
		exigirNoVacio(nombre, "topicName");
		if (KafkaNames.TOPICOS_PROTEGIDOS.contains(nombre)) {
			throw new ParametroInvalidoException(
					"El tópico " + nombre + " es productivo del caso BarrioDigital y no se puede eliminar desde esta API");
		}
		ejecutar(adminClient.deleteTopics(List.of(nombre)).all(), "No se pudo eliminar el tópico " + nombre);
	}

	public List<String> listarConsumerGroups() {
		Collection<ConsumerGroupListing> listados = ejecutar(
				adminClient.listConsumerGroups().all(), "No se pudo listar los consumer groups");
		return listados.stream().map(ConsumerGroupListing::groupId).toList();
	}

	public ConsumerGroupInfo consultarConsumerGroup(String groupId) {
		exigirNoVacio(groupId, "groupId");

		Map<String, ConsumerGroupDescription> descripciones = ejecutar(
				adminClient.describeConsumerGroups(List.of(groupId)).all(),
				"No se pudo describir el consumer group " + groupId);
		ConsumerGroupDescription descripcion = descripciones.get(groupId);
		if (descripcion == null || descripcion.state() == ConsumerGroupState.DEAD) {
			throw new RecursoNoEncontradoException("No existe el consumer group " + groupId);
		}

		Map<TopicPartition, OffsetAndMetadata> offsetsActuales = ejecutar(
				adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
				"No se pudo obtener los offsets de " + groupId);

		if (offsetsActuales.isEmpty()) {
			return new ConsumerGroupInfo(groupId, descripcion.state().toString(), List.of());
		}

		Map<TopicPartition, OffsetSpec> solicitudFinDeLog = offsetsActuales.keySet().stream()
				.collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
		Map<TopicPartition, ListOffsetsResultInfo> finesDeLog = ejecutar(
				adminClient.listOffsets(solicitudFinDeLog).all(),
				"No se pudo obtener el log end offset de " + groupId);

		List<ConsumerGroupOffsetInfo> offsets = offsetsActuales.entrySet().stream()
				.map(entry -> {
					TopicPartition tp = entry.getKey();
					long actual = entry.getValue().offset();
					long finDeLog = finesDeLog.containsKey(tp) ? finesDeLog.get(tp).offset() : actual;
					return new ConsumerGroupOffsetInfo(tp.topic(), tp.partition(), actual, finDeLog, Math.max(0, finDeLog - actual));
				})
				.sorted((a, b) -> a.topic().equals(b.topic()) ? Integer.compare(a.particion(), b.particion()) : a.topic().compareTo(b.topic()))
				.toList();

		return new ConsumerGroupInfo(groupId, descripcion.state().toString(), offsets);
	}

	private Map<String, String> obtenerConfiguracion(String nombre) {
		ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
		Map<ConfigResource, Config> configs = ejecutar(adminClient.describeConfigs(List.of(recurso)).all(),
				"No se pudo obtener la configuración de " + nombre);
		Config config = configs.get(recurso);
		if (config == null) {
			return Map.of();
		}
		return config.entries().stream()
				.filter(entry -> !entry.isDefault())
				.collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value, (a, b) -> a));
	}

	private TopicDescription describirTopico(String nombre) {
		Map<String, TopicDescription> descripciones = ejecutar(
				adminClient.describeTopics(List.of(nombre)).allTopicNames(),
				"No se pudo describir el tópico " + nombre);
		TopicDescription descripcion = descripciones.get(nombre);
		if (descripcion == null) {
			throw new RecursoNoEncontradoException("No existe el tópico " + nombre);
		}
		return descripcion;
	}

	// Traduce el futuro asíncrono del AdminClient a un resultado síncrono, convirtiendo las
	// excepciones de Kafka más comunes a las excepciones de dominio de este microservicio.
	private <T> T ejecutar(KafkaFuture<T> future, String mensajeError) {
		try {
			return future.get();
		} catch (ExecutionException ex) {
			if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
				throw new RecursoNoEncontradoException(mensajeError + ": no existe");
			}
			throw new RuntimeException(mensajeError + ": " + ex.getCause().getMessage(), ex.getCause());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(mensajeError + ": interrumpido", ex);
		}
	}

	private void exigirNoVacio(String valor, String nombreParametro) {
		if (valor == null || valor.isBlank()) {
			throw new ParametroInvalidoException(nombreParametro + " es obligatorio");
		}
	}

}
