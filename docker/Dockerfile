FROM sgrio/java:jre_8

ENV KAFKA_APPLICATION_ID_CONFIG			"jarvey.streams.export"
ENV KAFKA_BOOTSTRAP_SERVERS_CONFIG	"localhost:9192"
ENV DNA_TARGET_TOPICS								"node-tracks"
ENV DNA_EXPORT_DIR									"/var/jarvey"
ENV DNA_ROLLING_PERIOD_HOURS				2

RUN mkdir -p /jarvey
WORKDIR /jarvey
COPY jarvey.streams.export-0.1.0-all.jar ./jarvey.streams.jar

VOLUME ["$DNA_EXPORT_DIR"]

CMD ["java", "-cp", "/jarvey/jarvey.streams.jar", "jarvey.streams.export.TopicExporterDockerMain"]