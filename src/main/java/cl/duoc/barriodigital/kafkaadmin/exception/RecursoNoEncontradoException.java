package cl.duoc.barriodigital.kafkaadmin.exception;

// El tópico o consumer group consultado no existe en el clúster; el GlobalExceptionHandler
// responde HTTP 404.
public class RecursoNoEncontradoException extends RuntimeException {

	public RecursoNoEncontradoException(String message) {
		super(message);
	}

}
