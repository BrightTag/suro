package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.*;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.thrift.TMessageSet;

import kafka.admin.TopicCommand;
import kafka.api.FetchRequestBuilder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import scala.Option;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestKafkaSinkV2 {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    public static ZkExternalResource zk = new ZkExternalResource();
    public static KafkaServerExternalResource kafkaServer = new KafkaServerExternalResource(zk);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(zk)
            .around(kafkaServer);

    private static final String TOPIC_NAME = "routingKey";
    private static final String TOPIC_NAME_MULTITHREAD = "routingKeyMultithread";
    private static final String TOPIC_NAME_PARTITION_BY_KEY = "routingKey_partitionByKey";
    private static final String TOPIC_NAME_BACKWARD_COMPAT = "routingKey_backwardCompat";
    private static final String TOPIC_NAME_REQUEUEING = "routingKey_requeueing";


    @Test
    public void testDefaultParameters() throws IOException {
        KafkaSinkV2 sink = createSink( TOPIC_NAME, 1, false /* no fileQueue */ );
        sink.open();
        // create & send test messages to KafkaSink
        Iterator<Message> msgIterator = new MessageSetReader(createMessageSet(TOPIC_NAME, 2)).iterator();
        HashSet<String> sentPayloads = new HashSet<String>(); // track sent messages for comparison later
        while (msgIterator.hasNext()) {
            StringMessage next = new StringMessage(msgIterator.next());
            sink.writeTo(next); // send
            sentPayloads.add( new String( next.getMessage().getPayload() ) ); // record
        }
        sink.close();
        assertEquals(sink.getNumOfPendingMessages(), 0);
        System.out.println(sink.getStat());

        // get data back from Kafka
        List<MessageAndOffset> messageSet = getMessagesFromKafkaForTopic( TOPIC_NAME );
        assertEquals("Should have fetched 2 messages", 2, messageSet.size());

        for( int i=0; i<messageSet.size(); i++ ){
            // ensure that received message was one that was sent
            String receivedPayload = new String(extractMessage(messageSet, i));
            System.out.println( "Got message: " + new String( receivedPayload ) );
            assert( sentPayloads.remove( receivedPayload ) );
        }
        assertEquals( sentPayloads.size(), 0 ); // all sent messages should have been received
    }

    @Test
    public void testMultithread() throws IOException {
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME_MULTITHREAD,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1,\n" +
                "    \"batchSize\": 10,\n" +
                "    \"jobQueueSize\": 3\n" +
                "}";

        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSinkV2.class, "kafka"));
        KafkaSinkV2 sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        int msgCount = 10000;
        for (int i = 0; i < msgCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", Integer.toString(i))
                    .put("value", "message:" + i).build();
            sink.writeTo(new DefaultMessageContainer(
                    new Message(TOPIC_NAME_MULTITHREAD, jsonMapper.writeValueAsBytes(msgMap)),
                    jsonMapper));
        }
        assertTrue(sink.getNumOfPendingMessages() > 0);
        sink.close();
        System.out.println(sink.getStat());
        assertEquals(sink.getNumOfPendingMessages(), 0);

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid_multhread"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC_NAME_MULTITHREAD, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC_NAME_MULTITHREAD).get(0);
        for (int i = 0; i < msgCount; ++i) {
            stream.iterator().next();
        }

        try {
            stream.iterator().next();
            fail();
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
        }
    }

    @Test
    public void testFileBasedQueuePartitionByKey() throws Exception {
        int numPartitions = 9;

        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME_PARTITION_BY_KEY,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));
        String fileQueue = String.format(
                "    \"queue4Sink\": {\n" +
                "        \"type\": \"file\",\n" +
                "        \"path\": \"%s\",\n" +
                "        \"name\": \"testKafkaSink\"\n" +
                "    }\n", tempDir.newFolder().getAbsolutePath());
        String keyTopicMap = String.format("   \"keyTopicMap\": {\n" +
                "        \"%s\": \"key\"\n" +
                "    }", TOPIC_NAME_PARTITION_BY_KEY);

        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1,\n" +
                fileQueue + ",\n" +
                keyTopicMap + "\n" +
                "}";

        // setup sink
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSinkV2.class, "kafka"));
        KafkaSinkV2 sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();

        // create and send 10 test messages to Kafka
        int messageCount = 10;
        for (int i = 0; i < messageCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", Integer.toString(i % numPartitions))
                    .put("value", "message:" + i).build();
            sink.writeTo(new DefaultMessageContainer(
                    new Message(TOPIC_NAME_PARTITION_BY_KEY, jsonMapper.writeValueAsBytes(msgMap)),
                    jsonMapper));
        }
        sink.close();
        System.out.println(sink.getStat());

        // read data back from KafkaSink
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC_NAME_PARTITION_BY_KEY, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC_NAME_PARTITION_BY_KEY).get(0);
        Map<Integer, Set<Map<String, Object>>> resultSet = new HashMap<Integer, Set<Map<String, Object>>>();
        for (int i = 0; i < messageCount; ++i) {
            MessageAndMetadata<byte[], byte[]> msgAndMeta = stream.iterator().next();
            System.out.println(new String(msgAndMeta.message()));

            Map<String, Object> msg = jsonMapper.readValue(new String(msgAndMeta.message()), new TypeReference<Map<String, Object>>() {});
            Set<Map<String, Object>> s = resultSet.get(msgAndMeta.partition());
            if (s == null) {
                s = new HashSet<Map<String, Object>>();
                resultSet.put(msgAndMeta.partition(), s);
            }
            s.add(msg);
        }

        // verify we received what was sent
        int sizeSum = 0;
        for (Map.Entry<Integer, Set<Map<String, Object>>> e : resultSet.entrySet()) {
            sizeSum += e.getValue().size();
            String key = (String) e.getValue().iterator().next().get("key");
            for (Map<String, Object> ss : e.getValue()) {
                assertEquals(key, (String) ss.get("key"));
            }
        }
        assertEquals(sizeSum, messageCount);

        try {
            stream.iterator().next();
            fail();
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
        }
    }


    /** Tests backward compatability with old Kafka sink. */
    @Test
    public void testBackwardCompatability() throws Exception {
        int numPartitions = 9;

        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME_BACKWARD_COMPAT,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));
        String keyTopicMap = String.format("   \"keyTopicMap\": {\n" +
                "        \"%s\": \"key\"\n" +
                "    }", TOPIC_NAME_BACKWARD_COMPAT);

        String description1 = "{\n" +
            "    \"type\": \"kafkaV1\",\n" +
            "    \"client.id\": \"kafkasink\",\n" +
            "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
            "    \"request.required.acks\": 1,\n" +
            keyTopicMap + "\n" +
            "}";
        String description2 = "{\n" +
            "    \"type\": \"kafkaV2\",\n" +
            "    \"client.id\": \"kafkasink\",\n" +
            "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
            "    \"request.required.acks\": 1,\n" +
            keyTopicMap + "\n" +
            "}";

        // setup sinks, both old and new versions
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafkaV1"));
        jsonMapper.registerSubtypes(new NamedType(KafkaSinkV2.class, "kafkaV2"));
        KafkaSink   sinkV1 = jsonMapper.readValue(description1, new TypeReference<Sink>(){});
        KafkaSinkV2 sinkV2 = jsonMapper.readValue(description2, new TypeReference<Sink>(){});
        sinkV1.open();
        sinkV2.open();
        List<Sink> sinks = new ArrayList<Sink>();
        sinks.add(sinkV1);
        sinks.add(sinkV2);

        // setup Kafka consumer (to read back messages)
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
            createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC_NAME_BACKWARD_COMPAT, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = 
                                                consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC_NAME_BACKWARD_COMPAT).get(0);

        // Send 10 test message, using the old and new Kafka sinks.
        // Retrieve the messages and ensure that they are identical and sent to the same partition.
        Random rand = new Random();
        int messageCount = 10;
        for (int i = 0; i < messageCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", new Long( rand.nextLong() ) )
                    .put("value", "message:" + i).build();

            // send message to both sinks
            for( Sink sink : sinks ){
              sink.writeTo(new DefaultMessageContainer(
                    new Message(TOPIC_NAME_BACKWARD_COMPAT, jsonMapper.writeValueAsBytes(msgMap)),
                    jsonMapper));
            }

            // read two copies of message back from Kafka and check that partitions and data match
            MessageAndMetadata<byte[], byte[]> msgAndMeta1 = stream.iterator().next();
            MessageAndMetadata<byte[], byte[]> msgAndMeta2 = stream.iterator().next();
            System.out.println( "iteration: "+i+" partition1: "+msgAndMeta1.partition() );
            System.out.println( "iteration: "+i+" partition2: "+msgAndMeta2.partition() );
            assertEquals( msgAndMeta1.partition(), msgAndMeta2.partition() );
            String msg1Str = new String( msgAndMeta1.message() );
            String msg2Str = new String( msgAndMeta2.message() );
            System.out.println( "iteration: "+i+" message1: "+msg1Str );
            System.out.println( "iteration: "+i+" message2: "+msg2Str );
            assertEquals( msg1Str, msg2Str );
        }

        // close sinks
        sinkV1.close();
        sinkV2.close();
        // close consumer
        try {
            stream.iterator().next();
            fail(); // there should be no data left to consume
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
        }
    }


    /**
     * Take Kafka down and send a bunch of messages.  These should fail and be requeued.
     * Then take Kafka up and verify that all messages were received.
     */
    @Test
    public void testRequeueing() throws IOException {
        final int testDurationMillis = 60000;
        final int kafkaCycleMillis = 5000;  // time between kafka up/down cycles
        final AtomicInteger sentMessages = new AtomicInteger();

        // create lossy proxies to simulate failure connecting to kafka
        ArrayList<LossyKafkaProxy> proxies = new ArrayList<LossyKafkaProxy>();
        final int proxyStartPort = 12200;
        String localhost = "127.0.0.1"; //InetAddress.getLocalHost().getHostName();
        for( int i=0; i<2; i++ ){ // two Kafka servers
            LossyKafkaProxy lossyProxy = new LossyKafkaProxy( proxyStartPort+i, localhost,
                                                    kafkaServer.getServer(i).socketServer().port() );
            proxies.add( lossyProxy );
            lossyProxy.start();
        }
        String brokerProxies = localhost + ":" + proxyStartPort + "," 
                                + localhost + ":" + (proxyStartPort+1);

        final KafkaSinkV2 sink = createSink( TOPIC_NAME_REQUEUEING, 1, true /* with fileQueue */,
                                             brokerProxies );
        System.out.println("Created sink connected to proxies: "+brokerProxies);

        // create a thread that randomly sends test messages to KafkaSink
        Thread sender = new Thread(){
            @Override
            public void run() {
                long startTime = System.currentTimeMillis();
                sink.open();
                // run for testDurationMillis
                while( System.currentTimeMillis() - startTime < testDurationMillis ){
                    // send a message to the sink
                    sink.writeTo( new StringMessage( TOPIC_NAME_REQUEUEING, "test" ) );
                    sentMessages.incrementAndGet();
                    // sleep 1 ms
                    try{ Thread.sleep( 1 ); }catch(InterruptedException e){}
                }
                sink.close();
                System.out.println("Sent "+sentMessages+" messages");
                System.out.println(sink.getStat());
            }
        };
        // start sending
        sender.start();

        // wait for sending to finish
        try {
            System.out.println("Wait for KafkaSink to finish");
            sender.join();
        } catch (InterruptedException e) {
            fail("Interrupted");
        }
        assertEquals(sink.getNumOfPendingMessages(), 0);

        // stop proxies
        for( LossyKafkaProxy proxy : proxies ){
            try {
                proxy.shutdown();
            } catch (InterruptedException e) {
            }
        }

        // retrieve and count all messages from Kafka
        List<String> messageSet = getAllMessagesFromKafkaForTopic( TOPIC_NAME_REQUEUEING );
        assertEquals("Should have fetched "+sentMessages+" messages", sentMessages.intValue(), messageSet.size());
    }


    @Test
    public void testBlockingThreadPoolExecutor() {
        int jobQueueSize = 5;
        int corePoolSize = 3;
        int maxPoolSize = 3;

        try {
            testQueue(corePoolSize, maxPoolSize, new ArrayBlockingQueue<Runnable>(jobQueueSize));
            fail("RejectedExecutionException should be thrown");
        } catch (RejectedExecutionException e) {
            // good to go
        }

        BlockingQueue<Runnable> jobQueue = new ArrayBlockingQueue<Runnable>(jobQueueSize) {
            @Override
            public boolean offer(Runnable runnable) {
                try {
                    /* We override "offer" method to use "put" semantics.  This causes us to block
                     * on senders.execute(new Runnable(){}) instead of failing, because execute
                     * calls BlockingQueue#offer (which normally fails when full).
                     * For reference: http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html
                     */
                    put(runnable);
                } catch (InterruptedException e) {
                    // do nothing
                }
                return true;
            }
        };
        testQueue(corePoolSize, maxPoolSize, jobQueue);
    }


    /** Note that if this seems to have a limit to the number of messages returned, even if we 
     * increase the bufSize, below. */
    private List<MessageAndOffset> getMessagesFromKafkaForTopic( String topic ){
        // get the leader
        Option<Object> leaderOpt = ZkUtils.getLeaderForPartition(zk.getZkClient(), topic, 0);
        assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined());
        int leader = (Integer) leaderOpt.get();

        KafkaConfig config;
        if (leader == kafkaServer.getServer(0).config().brokerId()) {
            config = kafkaServer.getServer(0).config();
        } else {
            config = kafkaServer.getServer(1).config();
        }
        // get data back from Kafka
        int bufSize = 100000;
        SimpleConsumer consumer = new SimpleConsumer(config.hostName(), config.port(), 
                100000 /* timeout ms */, bufSize /* buffer size */, "clientId");
        FetchResponse response = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, bufSize).build());

        List<MessageAndOffset> messageSet = Lists.newArrayList(response.messageSet(topic, 0).iterator());
        return messageSet;
    }


    private List<String> getAllMessagesFromKafkaForTopic( String topic ){
        List<String> messages = new LinkedList<String>();
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        while(true){
            try{
                MessageAndMetadata<byte[], byte[]> msgAndMeta = stream.iterator().next();
                messages.add( new String( msgAndMeta.message() ) );
            }catch( ConsumerTimeoutException e ){
                // no more messages
                break;
            }
        }
        return messages;
    }


    private void testQueue(int corePoolSize, int maxPoolSize, BlockingQueue<Runnable> jobQueue) {
        ThreadPoolExecutor senders = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                10, TimeUnit.SECONDS,
                jobQueue);

        for (int i = 0; i < 100; ++i) {
            senders.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        fail();
                    }
                }
            });
        }
    }


    private KafkaSinkV2 createSink( String topic,
            long numPartitions,
            boolean withFileQueue
    ) throws IOException{
        return createSink( topic, numPartitions, withFileQueue, kafkaServer.getBrokerListStr() );
    }

    /** Creates sink.  You must call open() before using it. */
    private KafkaSinkV2 createSink( String topic,
                                    long numPartitions,
                                    boolean withFileQueue,
                                    String kafkaBrokers
        ) throws IOException{
        TopicCommand.createTopic(zk.getZkClient(),
            new TopicCommand.TopicCommandOptions(new String[]{
                  "--zookeeper", "dummy", "--create", "--topic", topic,
                  "--replication-factor", "2", "--partitions", new Long(numPartitions).toString()}));
        String fileQueue = String.format(
            "    \"queue4Sink\": {\n" +
            "        \"type\": \"file\",\n" +
            "        \"path\": \"%s\",\n" +
            "        \"name\": \"testKafkaSink\"\n" +
            "    }\n", tempDir.newFolder().getAbsolutePath());
        String keyTopicMap = String.format("   \"keyTopicMap\": {\n" +
            "        \"%s\": \"key\"\n" +
            "    }", topic);

        String description = "{\n" +
            "    \"type\": \"kafka\",\n" +
            "    \"client.id\": \"kafkasink\",\n" +
            "    \"metadata.broker.list\": \"" + kafkaBrokers + "\",\n" +
            "    \"request.required.acks\": 1,\n" +
            (withFileQueue? fileQueue + ",\n" : "") +
            keyTopicMap + "\n" +
            "}";

        // setup sink
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSinkV2.class, "kafka"));
        KafkaSinkV2 sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        return sink;
    }


    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "20000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("consumer.timeout.ms", "3000");
        return new ConsumerConfig(props);
    }

    private byte[] extractMessage(List<MessageAndOffset> messageSet, int offset) {
        ByteBuffer bb = messageSet.get(offset).message().payload();
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes, 0, bytes.length);
        return bytes;
    }

    public static TMessageSet createMessageSet(String topic, int numMsgs) {
        MessageSetBuilder builder = new MessageSetBuilder(new ClientConfig()).withCompression(Compression.LZF);
        for (int i = 0; i < numMsgs; ++i) {
            builder.withMessage(topic, ("testMessage" + i).getBytes());
        }

        return builder.build();
    }
}
