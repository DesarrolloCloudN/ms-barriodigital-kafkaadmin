package cl.duoc.barriodigital.kafkaadmin.dto;

// Offset/lag de un consumer group para una partición puntual.
public record ConsumerGroupOffsetInfo(
		String topic,
		int particion,
		long currentOffset,
		long logEndOffset,
		long lag
) {
}
