package jarvey.streams.export;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;

import jarvey.HdfsPath;

import utils.UnitUtils;
import utils.io.FilePath;
import utils.io.LfsPath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporterDockerMain {
	private static final Logger s_logger = Globals.LOGGER;
	
	private static final String DEFAULT_POLL_TIMEOUT = "10s";
	private static final String DEFAULT_FETCH_MIN_BYTE = "128kb";
	private static final String DEFAULT_FETCH_MAX_BYTE = "32mb";
	private static final String DEFAULT_FETCH_MAX_WAIT_MS = "10s";
	private static final String DEFAULT_MAX_POLL_INTERVAL = "30s";
	private static final String DEFAULT_MAX_FILE_BUFFER_SIZE = "64kb";
	
	/**
	 * Environment variables:
	 * <dl>
	 * 	<dt>KAFKA_GROUP_ID_CONFIG</dt>
	 * 	<dl>Consumer group 식별자.</dl>
	 * 
	 * 	<dt>KAFKA_BOOTSTRAP_SERVERS_CONFIG</dt>
	 * 	<dl>Kafka broker 접속 주소 리스트.</dl>
	 * 
	 * 	<dt>KAFKA_FETCH_MIN_BYTES_CONFIG</dt>
	 * 	<dl>Kafka broker 데이터를 읽어올 최소 크기.</dl>
	 * 
	 * 	<dt>KAFKA_FETCH_MAX_BYTES_CONFIG</dt>
	 * 	<dl>Kafka broker 데이터를 읽어올 최대 크기.</dl>
	 * 
	 * 	<dt>KAFKA_FETCH_MAX_WAIT_MS_CONFIG</dt>
	 * 	<dl>Kafka broker가 데이터를 전달하기 위해 대기하는 최대 시간</dl>
	 * 
	 * 	<dt>KAFKA_MAX_POLL_INTERVAL_MS_CONFIG</dt>
	 * 	<dl>Consumer가 broker에게 poll 요청을 하는 최대 기간</dl>
	 * 
	 * 	<dt>JARVEY_TARGET_TOPICS</dt>
	 * 	<dl>Export 대상 topic 이름 리스트. Comma(',')를 통해 구분함.</dl>
	 * 
	 * 	<dt>JARVEY_FS_DEFAULT</dt>
	 * 	<dl>HDFS 접속 설정 정보 파일 경로명. HDFS를 사용하지 않는 경우는 지정되지 않을 수 있음.</dl>
	 * 
	 * 	<dt>JARVEY_EXPORT_ARCHIVE_DIR</dt>
	 * 	<dl>Export되는 topic 데이터의 archive 파일 저장 디렉토리 경로.</dl>
	 * 
	 * 	<dt>JARVEY_EXPORT_TAIL_DIR</dt>
	 * 	<dl>Export되는 topic 데이터의 hot tail 파일 저장 디렉토리 경로.</dl>
	 * 
	 * 	<dt>JARVEY_ROLLING_PERIOD_HOURS</dt>
	 * 	<dl>Export되는 topic 데이터의 rotation 주기 (단위: hour).</dl>
	 * </dl>
	 * 
	 * 	<dt>JARVEY_BUFFER_SIZE</dt>
	 * 	<dl>파일에 쓰기용 버퍼 크기</dl>
	 * </dl>
	 * @param args
	 * @throws Exception
	 */
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();
		
		String targets = envs.get("JARVEY_TARGET_TOPICS");
		if ( targets == null ) {
			System.err.printf("Environment variable not specified: 'JARVEY_TARGET_TOPICS'");
			System.exit(-1);
		}
		List<String> topics = Arrays.asList(targets.split(","));
		s_logger.info("use the target topics: {}", topics);

		FilePath rootFile = null;
		String fsDefault = envs.get("JARVEY_FS_DEFAULT");
		s_logger.info("use a default filesystem: {}", fsDefault);
		if ( fsDefault == null || fsDefault.startsWith("file://") ) {
			rootFile = LfsPath.of("/");
		}
		else if ( fsDefault.startsWith("hdfs://") ) {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", fsDefault);
			FileSystem fs = FileSystem.get(conf);
			rootFile = HdfsPath.of(fs, "/");
		}
		else {
			System.err.printf("invalid 'fs.defaultFS: %s", fsDefault);
			System.exit(-1);
		}
		
		String exportDirPath = envs.get("JARVEY_EXPORT_ARCHIVE_DIR");
		if ( exportDirPath == null ) {
			System.err.printf("Environment variable not specified: 'JARVEY_EXPORT_ARCHIVE_DIR'");
			System.exit(-1);
		}
		FilePath exportArchiveDir = rootFile.path(exportDirPath);
		s_logger.info("use the export archive directory: {}", exportArchiveDir);
		
		String exportTailDirPath = envs.getOrDefault("JARVEY_EXPORT_TAIL_DIR", exportDirPath);
		FilePath exportTailDir = rootFile.path(exportTailDirPath);
		s_logger.info("use the export tail directory: {}", exportTailDir);

		int period = Integer.parseInt(envs.getOrDefault("JARVEY_ROLLING_PERIOD_HOURS", "2"));
		HourBasedRotationPolicy policy = new HourBasedRotationPolicy(period);
//		MinuteBasedRotationPolicy policy = new MinuteBasedRotationPolicy(period);
		policy.setLogger(Globals.LOGGER_ROTATION);
		s_logger.info("use the rolling period: {} hours", period);

		String pollTimeoutStr = envs.getOrDefault("KAFKA_POLL_TIMEOUT", DEFAULT_POLL_TIMEOUT);
		s_logger.info("use KAFKA_POLL_TIMEOUT: {}", pollTimeoutStr);
		Duration pollTimeout = Duration.ofMillis(UnitUtils.parseDuration(pollTimeoutStr));
		
		String fileBufSize = envs.getOrDefault("JARVEY_BUFFER_SIZE", DEFAULT_MAX_FILE_BUFFER_SIZE);
		s_logger.info("use JARVEY_BUFFER_SIZE: {}", fileBufSize);
		int bufSize = (int)UnitUtils.parseByteSize(fileBufSize);
				
		ExportConfig config = new ExportConfig(pollTimeout, exportTailDir, exportArchiveDir, ".json",
													policy, bufSize);

		Properties kafkaProps = buildKafkaProperties(envs);
		final KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<>(kafkaProps);
		TopicExporter exporter = new TopicExporter(consumer, topics, config);
		
		Thread mainThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					consumer.wakeup();
					mainThread.join();
				}
				catch ( InterruptedException e ) {
					s_logger.error("interrupted", e);
				}
			}
		});
		
		exporter.run();
	}
	
	private static Properties buildKafkaProperties(Map<String,String> environs) {
		Properties props = new Properties();
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());

		String appId = environs.getOrDefault("KAFKA_GROUP_ID_CONFIG", "jarvey.streams.exporter");
		s_logger.info("use Kafka group id: '{}'", appId);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
		
		String kafkaServers = environs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		s_logger.info("use the KafkaServers: {}", kafkaServers);
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		
		String fetchMinBytes = environs.getOrDefault("KAFKA_FETCH_MIN_BYTES_CONFIG", DEFAULT_FETCH_MIN_BYTE);
		s_logger.info("use FETCH_MIN_BYTES_CONFIG: {}", fetchMinBytes);
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, (int)UnitUtils.parseByteSize(fetchMinBytes));
		
		String fetchMaxBytes = environs.getOrDefault("KAFKA_FETCH_MAX_BYTES_CONFIG", DEFAULT_FETCH_MAX_BYTE);
		s_logger.info("use FETCH_MAX_BYTES_CONFIG: {}", fetchMaxBytes);
		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int)UnitUtils.parseByteSize(fetchMaxBytes));
		
		String fetchMaxWaitMillis = environs.getOrDefault("KAFKA_FETCH_MAX_WAIT_MS_CONFIG",
															DEFAULT_FETCH_MAX_WAIT_MS);
		s_logger.info("use FETCH_MAX_WAIT_MS_CONFIG: {}", fetchMaxWaitMillis);
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int)UnitUtils.parseDuration(fetchMaxWaitMillis));

		String maxPollIntvl = environs.getOrDefault("KAFKA_MAX_POLL_INTERVAL_MS_CONFIG",
													DEFAULT_MAX_POLL_INTERVAL);
		s_logger.info("use MAX_POLL_INTERVAL_MS_CONFIG: {}", maxPollIntvl);
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int)UnitUtils.parseDuration(maxPollIntvl));
		
		return props;
	}
}
