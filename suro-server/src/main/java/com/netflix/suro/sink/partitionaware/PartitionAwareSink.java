package com.netflix.suro.sink.partitionaware;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.netflix.suro.event.PartitionEvent;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.kafka.KafkaSink;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.message.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;

import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink type that uses a {@link com.netflix.suro.sink.KafkaSink}, and falls back to a
 * {@link com.netflix.suro.sink.LocalFileSink} when kafka is not available
 *
 *  @author zdexter
 *
 * */
public class PartitionAwareSink implements Sink {
    public final static String TYPE = "PartitionAware";
    static Logger log = LoggerFactory.getLogger(PartitionAwareSink.class);
    private final String NO_SINK_SELECTED = "No sink selected";

    private final KafkaSink kafkaSink;
    private final LocalFileSink fileSink;
    private SinkType sinkType;
    private final KafkaPing kafkaPing;
    private boolean isDrainingFiles = false;
    private final EventBus eventBus;

    @JsonCreator
    public PartitionAwareSink(
            @JsonProperty("kafkaSink") KafkaSink kafkaSink,
            @JsonProperty("fileSink") LocalFileSink fileSink,
            @JacksonInject("async event bus") AsyncEventBus asyncEventBus
            ) {
        Preconditions.checkNotNull(kafkaSink, "Kafka sink needed");
        Preconditions.checkNotNull(fileSink, "Local file sink needed");
        Preconditions.checkNotNull(asyncEventBus, "Async event bus needed");

        kafkaPing = new KafkaPing("localhost", 9092, "suroserver-guestvm", "segfire_metrics");

        this.kafkaSink = kafkaSink;
        this.fileSink = fileSink;
        this.eventBus = asyncEventBus;

        eventBus.register(this);
        kafkaSink.setEventBus(asyncEventBus);
        sinkType = SinkType.KAFKA;
    }

    @Override
    public void writeTo(MessageContainer message) {
        if (sinkType == SinkType.KAFKA) {
            kafkaSink.writeTo(message);
        } else if (sinkType == SinkType.FILE) {
            fileSink.writeTo(message);
        }
    }

    @Override
    public void open() {
        kafkaSink.open();
        fileSink.open();
        if (!kafkaPing.isAlive()) {
            log.info("***** posting event");
            eventBus.post(new PartitionEvent(null));
        }
    }

    @Override
    public void close() {
        kafkaSink.close();
        fileSink.close();
    }

    @Override
    public String recvNotice() {
        String notice = NO_SINK_SELECTED;

        if (sinkType == SinkType.KAFKA) {
            notice = kafkaSink.recvNotice();
        } else if (sinkType == SinkType.FILE) {
            notice = fileSink.recvNotice();
        }

        return notice;
    }

    @Override
    public String getStat() {
        String stat = NO_SINK_SELECTED;

        if (sinkType == SinkType.KAFKA) {
            stat = kafkaSink.getStat();
        } else if (sinkType == SinkType.FILE) {
            stat = fileSink.getStat();
        }

        return stat;
    }

    private enum SinkType {
        KAFKA, FILE
    }

    @Subscribe
    @AllowConcurrentEvents
    public void partitionEvent(PartitionEvent e) {
        if (sinkType == SinkType.KAFKA) {
            log.info("**** Cannot reach Kafka, switching to file sink");
            sinkType = SinkType.FILE;
            List<Message> msgQueue = e.getMsgList();
            if (msgQueue != null) {
                kafkaSink.drainQueueTo(msgQueue);
                while (!msgQueue.isEmpty()) {
                    Message tmpMsg = msgQueue.iterator().next();
                    fileSink.writeTo(buildMessageContainer(tmpMsg));
                    msgQueue.remove(tmpMsg);
                }
            }
            runKafkaPinger(); // this runs until Kafka comes back online
            if (!isDrainingFiles) {
                drainLocalFiles(fileSink.getOutputDir());
            }
        }
    }

    private void runKafkaPinger() {
        while (sinkType == SinkType.FILE) {
            log.info("**** Pinging kafka...");
            if (kafkaPing.isAlive()) {
                log.info("**** Kafka is back!");
                sinkType = SinkType.KAFKA;
            } else {
                sleep(5000);
            }
        }
    }

    private void drainLocalFiles(String dirPath) {
        isDrainingFiles = true;
        BufferedReader br;
        String line;
        File dir = new File(dirPath);
        File[] files;
        PrintWriter writer;

        files = dir.listFiles();
        for (final File file : files) {
            String fileExt = LocalFileSink.getFileExt(file.toString());
            if (fileExt.equals(".done")) {
                try {
                    br = new BufferedReader(new FileReader(file));
                    while ((line = br.readLine()) != null) {
                        log.info("**** file: " + file.toString());
                        MessageContainer mContainer = buildMessageContainer(new Message("segfire_metrics", line.getBytes()));
                        if (sinkType == SinkType.KAFKA) {
                            log.info("**** writing to kafka sink...");
                            kafkaSink.writeTo(mContainer);
                            sleep(10);
                        } else {
                            log.info("*** writing to file sink...");
                            log.info("**** line: " + line);
                            fileSink.writeTo(mContainer);
                        }
                    }
                    br.close();
                    if (fileExt.equals(".done")) {
                        file.delete();
                    } else {
                        writer = new PrintWriter(file);
                        writer.print("");
                        writer.close();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        isDrainingFiles = false;
    }

    private MessageContainer buildMessageContainer(final Message message) {
        return new MessageContainer() {
            class Item {
                TypeReference<?> tr;
                Object obj;
            }

            private List<Item> cache;

            @Override
            public <T> T getEntity(Class<T> clazz) throws Exception {
                if (clazz.equals(byte[].class)){
                    return (T)message.getPayload();
                } else if (clazz.equals(String.class)) {
                    return (T)new String(message.getPayload());
                } else {
                    return null;
                }
            }

            @Override
            public String getRoutingKey() {
                return message.getRoutingKey();
            }

            @Override
            public Message getMessage() {
                return message;
            }
        };
    }

    private class KafkaPing {
        private final SimpleConsumer consumer;
        private final TopicMetadataRequest req;
        private TopicMetadataResponse resp;

        public KafkaPing(String brokerHost, int brokerPort, String clientId, String topic) {
            consumer = new SimpleConsumer(brokerHost, brokerPort, 1000, 64*1024, clientId);
            List<String> topics = Collections.singletonList(topic);
            req = new TopicMetadataRequest(topics);
        }

        public boolean isAlive() {
            try {
                resp = consumer.send(req);
            } catch (Exception e) {
                return false;
            }
            return true;
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}