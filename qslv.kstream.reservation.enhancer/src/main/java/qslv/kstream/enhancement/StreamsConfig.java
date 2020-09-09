package qslv.kstream.enhancement;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import com.fasterxml.jackson.databind.JavaType;

import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.TraceableMessage;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.kstream.LoggedTransaction;

@Configuration
@EnableKafkaStreams
public class StreamsConfig {
	private static final Logger log = LoggerFactory.getLogger(StreamsConfig.class);
	
	@Autowired
	ConfigProperties configProperties;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean(name=KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public KafkaStreamsConfiguration defaultKafkaStreamsConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaStreamsPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaStreamsPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaStreamsPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		return new KafkaStreamsConfiguration(new HashMap(kafkaconfig));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	public Map<String,String> kafkaConsumerConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaConsumerPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaConsumerPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaConsumerPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		return new HashMap(kafkaconfig);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	public Map<String,String> kafkaProducerConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaProducerPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaProducerPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaProducerPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		return new HashMap(kafkaconfig);
	}	
	
	@Bean
	Serde<LoggedTransaction> reservationSerde () throws Exception {
		JacksonAvroSerializer<LoggedTransaction> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<LoggedTransaction> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(kafkaProducerConfig(), false);
		deserializer.configure(kafkaConsumerConfig(), false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
	@Bean
	Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde() throws Exception {
		JacksonAvroSerializer<TraceableMessage<WorkflowMessage>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<WorkflowMessage>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class, WorkflowMessage.class);
		serializer.configure(kafkaProducerConfig(), false, type);
		deserializer.configure(kafkaConsumerConfig(), false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
}
