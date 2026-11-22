package cl.duoc.barriodigital.kafkaadmin.dto;

import java.util.List;

// Estado de un consumer group y el avance (offset/lag) de cada partición que tiene asignada.
public record ConsumerGroupInfo(
		String groupId,
		String estado,
		List<ConsumerGroupOffsetInfo> offsets
) {
}
