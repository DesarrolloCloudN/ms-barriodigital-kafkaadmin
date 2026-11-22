package cl.duoc.barriodigital.kafkaadmin.config;

import java.util.Set;

// Nombres de los tópicos del caso BarrioDigital (EP3). Se usan para bloquear el borrado
// accidental de los tópicos productivos desde el endpoint de administración (ver
// KafkaAdminService.eliminarTopico): esta API solo debe borrar tópicos temporales de prueba.
public final class KafkaNames {

	private KafkaNames() {
	}

	public static final String TOPIC_EVENTOS = "tramites-eventos";
	public static final String TOPIC_AUDITORIA = "tramites-auditoria";

	public static final Set<String> TOPICOS_PROTEGIDOS = Set.of(
			TOPIC_EVENTOS,
			TOPIC_AUDITORIA,
			TOPIC_EVENTOS + ".DLT",
			TOPIC_AUDITORIA + ".DLT"
	);

}
