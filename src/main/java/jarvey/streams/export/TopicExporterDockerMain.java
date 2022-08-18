package jarvey.streams.export;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vlkan.rfos.policy.RotationPolicy;

import utils.io.FileProxy;
import utils.io.LocalFile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporterDockerMain {
	private static final Logger s_logger = LoggerFactory.getLogger(TopicExporterDockerMain.class.getPackage().getName());
	
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();

		String appId = envs.getOrDefault("KAFKA_APPLICATION_ID_CONFIG", "jarvey.streams.exporter");
		s_logger.info("use Kafka application id: '{}'", appId);
		
		String kafkaServers = envs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		s_logger.info("use the KafkaServers: {}", kafkaServers);
		
		String targets = envs.get("DNA_TARGET_TOPICS");
		if ( targets == null ) {
			System.err.printf("Environment variable not specified: 'DNA_TARGET_TOPICS'");
			System.exit(-1);
		}
		List<String> topics = Arrays.asList(targets.split(","));
		s_logger.info("use the target topics: {}", topics);
		
		String exportDirPath = envs.get("DNA_EXPORT_DIR");
		if ( exportDirPath == null ) {
			System.err.printf("Environment variable not specified: 'DNA_EXPORT_DIR'");
			System.exit(-1);
		}
		FileProxy exportDir = LocalFile.of(exportDirPath);
		s_logger.info("use the export directory: {}", exportDir);
		
		int period = Integer.parseInt(envs.getOrDefault("DNA_ROLLING_PERIOD_HOURS", "2"));
		RotationPolicy policy = new HourBasedRotationPolicy(period);
		s_logger.info("use the rolling period: {} hours", period);
		
		TopicExporter exporter = new TopicExporter(kafkaServers, appId, topics, exportDir, ".json", policy);
		exporter.run();
	}
}
