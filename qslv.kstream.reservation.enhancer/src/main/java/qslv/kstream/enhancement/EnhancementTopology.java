package qslv.kstream.enhancement;

import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import qslv.common.kafka.TraceableMessage;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.workflow.CancelReservationWorkflow;
import qslv.kstream.workflow.CommitReservationWorkflow;
import qslv.kstream.workflow.WorkflowMessage;

@Component
public class EnhancementTopology {
	private static final Logger log = LoggerFactory.getLogger(EnhancementTopology.class);

	@Autowired
	ConfigProperties configProperties;
	@Autowired
	Serde<LoggedTransaction> reservationSerde;
	@Autowired
	Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde;

	public void setConfigProperties(ConfigProperties configProperties) {
		this.configProperties = configProperties;
	}

	public void setReservationSerde(Serde<LoggedTransaction> reservationSerde) {
		this.reservationSerde = reservationSerde;
	}

	public void setEnhancedPostingRequestSerde(Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde) {
		this.enhancedPostingRequestSerde = enhancedPostingRequestSerde;
	}

	@Bean
	public KStream<?, ?> kStream(StreamsBuilder builder) {
		log.debug("kStream ENTRY");
		KStream<UUID, TraceableMessage<WorkflowMessage>> rawRequests = builder.stream(configProperties.getMatchReservationTopic(),
				Consumed.with(Serdes.UUID(), enhancedPostingRequestSerde));
		KTable<UUID, LoggedTransaction> reservations = builder.stream(configProperties.getReservationByUuidTopic(),
				Consumed.with(Serdes.UUID(), reservationSerde))
				.groupByKey()
				.aggregate(() -> {return null;}, (k,v,a) -> filterReservations(k,v,a), 
						Materialized.with(Serdes.UUID(), reservationSerde));
		rawRequests.leftJoin( reservations, (s,t) -> joinReservation(s,t) )
				.map((k,v) -> reKey(k,v))
				.to( configProperties.getEnhancedRequestTopic(), Produced.with(Serdes.String(), enhancedPostingRequestSerde));
		log.debug("kStream EXIT");
		return rawRequests;
	}
	
	LoggedTransaction filterReservations( UUID key, LoggedTransaction tran, LoggedTransaction aggregate) {
		if ( tran.getTransactionTypeCode().equals(LoggedTransaction.RESERVATION) ) {
			log.debug("Reservation Message Recieved {}", key.toString());
			return tran;
		}
		if ( aggregate != null && tran.getTransactionTypeCode().equals(LoggedTransaction.RESERVATION_CANCEL)  ) {
			log.debug("Cancel Message Recieved {}", key.toString());
			return null;
		}
		if ( aggregate != null && tran.getTransactionTypeCode().equals(LoggedTransaction.RESERVATION_COMMIT)  ) {
			log.debug("Commit Message Recieved {}", key.toString());
			return null;
		}
		return null;
	}
	
	TraceableMessage<WorkflowMessage> joinReservation( TraceableMessage<WorkflowMessage> message, LoggedTransaction reservation) {
		log.debug("Reservation joined {}", reservation == null ? "null" : reservation.getTransactionUuid().toString());

		if ( message.getPayload().hasCancelReservationWorkflow() )
			message.getPayload().getCancelReservationWorkflow().setReservation(reservation);
		else if ( message.getPayload().hasCommitReservationWorkflow() )
			message.getPayload().getCommitReservationWorkflow().setReservation(reservation);
		return message;
	}
	
	KeyValue<String, TraceableMessage<WorkflowMessage>> reKey(UUID key, TraceableMessage<WorkflowMessage> message) {
		String newKey = null;
		if ( message.getPayload().hasCancelReservationWorkflow() ) {
			if ( message.getPayload().getCancelReservationWorkflow().getReservation() == null ) {
				message.getPayload().getCancelReservationWorkflow().setState(CancelReservationWorkflow.NO_MATCH);
			} else {
				message.getPayload().getCancelReservationWorkflow().setState(CancelReservationWorkflow.CANCEL_RESERVATION);
			}
			newKey = message.getPayload().getCancelReservationWorkflow().getProcessingAccountNumber();
			log.debug("Transaction rekeyed from {} to {}", key.toString(), newKey);
			
		} else if ( message.getPayload().hasCommitReservationWorkflow() ) {
			if ( message.getPayload().getCommitReservationWorkflow().getReservation() == null ) {
				message.getPayload().getCommitReservationWorkflow().setState(CommitReservationWorkflow.NO_MATCH);
			} else {
				message.getPayload().getCommitReservationWorkflow().setState(CommitReservationWorkflow.COMMIT_START);
			}
			newKey = message.getPayload().getCommitReservationWorkflow().getProcessingAccountNumber();
			log.debug("Transaction rekeyed from {} to {}", key.toString(), newKey);
		}
		return new KeyValue<>(newKey, message);
	}

}
