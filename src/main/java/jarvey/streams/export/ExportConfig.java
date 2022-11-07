package jarvey.streams.export;

import java.io.File;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.common.TopicPartition;

import com.vlkan.rfos.RotatingFileOutputStream;
import com.vlkan.rfos.RotatingFilePattern;
import com.vlkan.rfos.RotationCallback;
import com.vlkan.rfos.RotationConfig;
import com.vlkan.rfos.policy.RotationPolicy;

import jarvey.FilePath;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ExportConfig {
	private final FilePath m_tailDir;
	private final FilePath m_archiveDir;
	private final String m_suffix;
	private final RotationPolicy m_policy;
	
	private final Duration m_pollTimeout;
	private final int m_bufferSize;
	
	ExportConfig(Duration pollTimeout, FilePath tailDir, FilePath archiveDir, String suffix,
					RotationPolicy policy, int bufSize) {
		m_pollTimeout = pollTimeout;
		m_tailDir = tailDir;
		m_archiveDir = archiveDir;
		m_suffix = suffix;
		m_policy = policy;
		m_bufferSize = bufSize;
	}
	
	public Duration getPollTimeout() {
		return m_pollTimeout;
	}
	
	public int getBufferSize() {
		return m_bufferSize;
	}
	
	RotatingFileOutputStream getOutputStream(TopicPartition key) {
		String topicTailFileName = String.format("%s_%d%s", key.topic(), key.partition(), m_suffix);
		FilePath topicTailFile = m_tailDir.getChild(topicTailFileName);

		FilePath topicArchiveDir = m_archiveDir.getChild(key.topic());
		String patFileName = String.format("%s_%d-%s%s", key.topic(), key.partition(),
											"%d{yyyyMMdd-HH}", m_suffix);
		String filePattern = topicArchiveDir.getAbsolutePath()
												+ File.separator + "%d{yyyy}"
												+ File.separator + "%d{MM}"
												+ File.separator + "%d{dd}"
												+ File.separator + patFileName;
		RotatingFilePattern pat = RotatingFilePattern.builder().pattern(filePattern).build();
		
		RotationConfig rconfig = RotationConfig.builder()
												.append(true)
												.filePattern(pat)
												.compress(true)
												.policy(m_policy)
												.callback(s_callback)
												.build();
		Globals.LOGGER_ROTATION.info("opening a new rotation-output-stream: file={}", topicTailFile);
		RotatingFileOutputStream rfos = new RotatingFileOutputStream(topicTailFile, rconfig);
		rfos.setLogger(Globals.LOGGER_ROTATION);
		
		return rfos;
	}
	
	private static final LoggingCallback s_callback = new LoggingCallback();
	static class LoggingCallback implements RotationCallback {
		@Override
		public void onTrigger(RotationPolicy policy, Instant instant) { }

		@Override
		public void onOpen(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onClose(RotationPolicy policy, Instant instant, OutputStream stream) { }

		@Override
		public void onSuccess(RotationPolicy policy, Instant instant, FilePath path) {
			Globals.LOGGER_ROTATION.info("created a rolling file: {}", path);
		}

		@Override
		public void onFailure(RotationPolicy policy, Instant instant, FilePath path, Exception error) {
			String msg = String.format("faild to create a rolling file: file=%s, cause=%s", path, error);
			Globals.LOGGER_ROTATION.error(msg, error);
		}
	}
}