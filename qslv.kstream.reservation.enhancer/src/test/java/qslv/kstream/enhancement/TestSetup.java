package qslv.kstream.enhancement;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import com.fasterxml.jackson.databind.JavaType;

import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.TraceableMessage;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.kstream.LoggedTransaction;

public class TestSetup {

	static public final String SCHEMA_REGISTRY = "http://localhost:8081";

	static public final String MATCH_RESERVATION_TOPIC = "match.reservation";
	static public final String ENHANCED_REQUEST_TOPIC = "enhanced.request";
	static public final String RESERVATION_BY_UUID_TOPIC = "transaction.by.uuid";

	private ConfigProperties configProperties = new ConfigProperties();

	private Serde<LoggedTransaction> reservationSerde;
	private Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde;
	
	private EnhancementTopology enhancementTopology = new EnhancementTopology();

	private TopologyTestDriver testDriver;

	private TestInputTopic<UUID, LoggedTransaction> transactionByUuidTopic;
	private TestInputTopic<UUID, TraceableMessage<WorkflowMessage>> matchReservationTopic;
	private TestOutputTopic<String, TraceableMessage<WorkflowMessage>> enhancedRequestTopic;

	public TestSetup() throws Exception {
		configProperties.setAitid("12345");
		configProperties.setMatchReservationTopic(MATCH_RESERVATION_TOPIC);
		configProperties.setEnhancedRequestTopic(ENHANCED_REQUEST_TOPIC);
		configProperties.setReservationByUuidTopic(RESERVATION_BY_UUID_TOPIC);
		enhancementTopology.setConfigProperties(configProperties);

		Map<String, String> config = new HashMap<>();
		config.put("schema.registry.url", SCHEMA_REGISTRY);
		reservationSerde = reservationSerde(config);
		enhancedPostingRequestSerde = enhancedPostingRequestSerde(config);

		enhancementTopology.setEnhancedPostingRequestSerde(enhancedPostingRequestSerde);
		enhancementTopology.setReservationSerde(reservationSerde);

		StreamsBuilder builder = new StreamsBuilder();
		enhancementTopology.kStream(builder);

		Topology topology = builder.build();

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit.reservation");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		testDriver = new TopologyTestDriver(topology, props);

		transactionByUuidTopic = testDriver.createInputTopic(RESERVATION_BY_UUID_TOPIC, Serdes.UUID().serializer(),
				reservationSerde.serializer());
		matchReservationTopic = testDriver.createInputTopic(MATCH_RESERVATION_TOPIC, Serdes.UUID().serializer(),
				enhancedPostingRequestSerde.serializer());
		enhancedRequestTopic = testDriver.createOutputTopic(ENHANCED_REQUEST_TOPIC, Serdes.String().deserializer(),
				enhancedPostingRequestSerde.deserializer());
	}

	static public Serde<LoggedTransaction> reservationSerde (Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<LoggedTransaction> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<LoggedTransaction> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(config, false);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
	static public Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<TraceableMessage<WorkflowMessage>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<WorkflowMessage>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class, WorkflowMessage.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}

	public TopologyTestDriver getTestDriver() {
		return testDriver;
	}

	public EnhancementTopology getEnhancementTopology() {
		return enhancementTopology;
	}

	public TestInputTopic<UUID, LoggedTransaction> getTransactionByUuidTopic() {
		return transactionByUuidTopic;
	}

	public TestInputTopic<UUID, TraceableMessage<WorkflowMessage>> getMatchReservationTopic() {
		return matchReservationTopic;
	}

	public TestOutputTopic<String, TraceableMessage<WorkflowMessage>> getEnhancedRequestTopic() {
		return enhancedRequestTopic;
	}
}
