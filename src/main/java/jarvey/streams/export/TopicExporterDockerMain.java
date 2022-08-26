package jarvey.streams.export;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import rhdfos.HdfsFile;
import utils.io.FileProxy;
import utils.io.LocalFile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporterDockerMain {
	private static final Logger s_logger = Globals.LOGGER;
	
	/**
	 * Environment variables:
	 * <dl>
	 * 	<dt>KAFKA_APPLICATION_ID_CONFIG</dt>
	 * 	<dl>Consumer group 식별자.</dl>
	 * 
	 * 	<dt>KAFKA_BOOTSTRAP_SERVERS_CONFIG</dt>
	 * 	<dl>Kafka broker 접속 주소 리스트.</dl>
	 * 
	 * 	<dt>JARVEY_TARGET_TOPICS</dt>
	 * 	<dl>Export 대상 topic 이름 리스트. Comma(',')를 통해 구분함.</dl>
	 * 
	 * 	<dt>HADOOP_FS_DEFAULT</dt>
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
	 * @param args
	 * @throws Exception
	 */
	public static void main(String... args) throws Exception {
		Map<String,String> envs = System.getenv();

		String appId = envs.getOrDefault("KAFKA_APPLICATION_ID_CONFIG", "jarvey.streams.exporter");
		s_logger.info("use Kafka application id: '{}'", appId);
		
		String kafkaServers = envs.getOrDefault("KAFKA_BOOTSTRAP_SERVERS_CONFIG", "localhost:9092");
		s_logger.info("use the KafkaServers: {}", kafkaServers);
		
		String targets = envs.get("JARVEY_TARGET_TOPICS");
		if ( targets == null ) {
			System.err.printf("Environment variable not specified: 'JARVEY_TARGET_TOPICS'");
			System.exit(-1);
		}
		List<String> topics = Arrays.asList(targets.split(","));
		s_logger.info("use the target topics: {}", topics);

		FileProxy rootFile = null;
		String fsDefault = envs.get("HADOOP_FS_DEFAULT");
		s_logger.info("use a default filesystem: {}", fsDefault);
		if ( fsDefault == null || fsDefault.startsWith("file://") ) {
			rootFile = LocalFile.of("/");
		}
		else if ( fsDefault.startsWith("hdfs://") ) {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", fsDefault);
			FileSystem fs = FileSystem.get(conf);
			rootFile = HdfsFile.of(fs, "/");
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
		FileProxy exportArchiveDir = rootFile.proxy(exportDirPath);
		s_logger.info("use the export archive directory: {}", exportArchiveDir);
		
		String exportTailDirPath = envs.getOrDefault("JARVEY_EXPORT_TAIL_DIR", exportDirPath);
		FileProxy exportTailDir = rootFile.proxy(exportTailDirPath);
		s_logger.info("use the export tail directory: {}", exportTailDir);

		int period = Integer.parseInt(envs.getOrDefault("JARVEY_ROLLING_PERIOD_HOURS", "2"));
		HourBasedRotationPolicy policy = new HourBasedRotationPolicy(period);
//		MinuteBasedRotationPolicy policy = new MinuteBasedRotationPolicy(period);
		policy.setLogger(Globals.LOGGER_ROTATION);
		s_logger.info("use the rolling period: {} hours", period);
		
		TopicExporter exporter = new TopicExporter(kafkaServers, appId, topics, exportTailDir,
													exportArchiveDir, ".json", policy);
		exporter.run();
	}
}
