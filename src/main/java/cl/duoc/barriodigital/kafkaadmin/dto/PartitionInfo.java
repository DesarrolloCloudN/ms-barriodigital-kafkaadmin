package cl.duoc.barriodigital.kafkaadmin.dto;

import java.util.List;

// Detalle de una partición: quién es su líder y cómo están distribuidas sus réplicas/ISR.
public record PartitionInfo(
		int particion,
		int lider,
		List<Integer> replicas,
		List<Integer> isr
) {
}
