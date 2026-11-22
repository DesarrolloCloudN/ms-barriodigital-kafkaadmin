package cl.duoc.barriodigital.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

// Vista administrativa de un tópico: particiones/réplicas reales y su configuración efectiva
// (retention.ms, cleanup.policy, etc.) para el endpoint de consulta/administración.
public record TopicInfo(
		String nombre,
		int particiones,
		int factorReplicacion,
		List<PartitionInfo> detalleParticiones,
		Map<String, String> configuracion
) {
}
