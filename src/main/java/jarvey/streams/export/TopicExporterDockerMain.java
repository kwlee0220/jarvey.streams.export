package jarvey.streams.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.vlkan.rfos.policy.RotationPolicy;

import rhdfos.HdfsFile;
import utils.io.FileProxy;
import utils.io.LocalFile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TopicExporterDockerMain {
	private static final Logger s_logger = Globals.getLogger();
	
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
		
		FileProxy rootFile;
		String hdfsConfPath = envs.get("DNA_HDFS_CONF");
		if ( hdfsConfPath != null ) {
			Configuration conf = new Configuration();
			
			File confFile = new File(hdfsConfPath);
			try ( InputStream is = new FileInputStream(confFile) ) {
				conf.addResource(is);
				
				FileSystem fs = FileSystem.get(conf);
				rootFile = HdfsFile.of(fs, "/");
			}
		}
		else {
			rootFile = LocalFile.of("/");
		}
		
		String exportDirPath = envs.get("DNA_EXPORT_ARCHIVE_DIR");
		if ( exportDirPath == null ) {
			System.err.printf("Environment variable not specified: 'DNA_EXPORT_ARCHIVE_DIR'");
			System.exit(-1);
		}
		FileProxy exportArchiveDir = rootFile.proxy(exportDirPath);
		s_logger.info("use the export archive directory: {}", exportArchiveDir);
		
		String exportTailDirPath = envs.getOrDefault("DNA_EXPORT_TAIL_DIR", exportDirPath);
		FileProxy exportTailDir = rootFile.proxy(exportTailDirPath);
		s_logger.info("use the export tail directory: {}", exportTailDir);
		
		int period = Integer.parseInt(envs.getOrDefault("DNA_ROLLING_PERIOD_HOURS", "2"));
		RotationPolicy policy = new HourBasedRotationPolicy(period);
		s_logger.info("use the rolling period: {} hours", period);
		
		TopicExporter exporter = new TopicExporter(kafkaServers, appId, topics, exportTailDir,
													exportArchiveDir, ".json", policy);
		exporter.run();
	}
}
