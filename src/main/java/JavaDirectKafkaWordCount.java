/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.drools.examples.helloworld.HelloWorldExample.Message;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.io.KieResources;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

//import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */

public final class JavaDirectKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        //org.apache.spark.examples.streaming.StreamingExamples.setStreamingLogLevels();
        Logger.getRootLogger().setLevel(Level.WARN);

        String brokers = args[0];
        String topics = args[1];

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));




        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);

        lines.foreachRDD(line -> {
            line.foreach(w -> {
                serializeToJavaModel(w);
            });
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    private static void serializeToJavaModel(String line){
        JsonElement jelement = new JsonParser().parse(line);
        JsonObject jobject = jelement.getAsJsonObject();
        JsonObject content = jobject.getAsJsonObject("content");
        JsonElement countryCode = content.get("CONUTRY_CODE");
        runDrools(countryCode.getAsString());
    }

    private static void runDrools(String word){
        StatelessKieSession ksession = KieSessionFactory.getKieSession("/home/centos/sparkStreaming/helloDrools.drl");

        // The application can insert facts into the session
        final Message message = new Message();

        message.setMessage(word);
        ksession.execute(message);
    }


    public static class KieSessionFactory  implements Serializable {

        static StatelessKieSession statelessKieSession;

        public static StatelessKieSession getKieSession(String filename) {
            if (statelessKieSession == null)
                statelessKieSession = getNewKieSession(filename);
            return statelessKieSession;
        }

        public String readFile(String filename, Charset encoding) {
            String res = "";
            try {
                byte[] encoded = Files.readAllBytes(Paths.get(filename));
                res = new String(encoded, encoding);
            } catch (IOException e) {
                System.out.println("Error reading rules file " + filename);
                e.printStackTrace();
            }
            return res;
        }

        public static StatelessKieSession getNewKieSession(String drlFileName) {
            System.out.println("creating a new kie session");

            KieServices kieServices = KieServices.Factory.get();
            KieResources kieResources = kieServices.getResources();

            KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
            KieRepository kieRepository = kieServices.getRepository();

            Resource resource = kieResources.newFileSystemResource(drlFileName);
            kieFileSystem.write(resource);

            KieBuilder kb = kieServices.newKieBuilder(kieFileSystem);

            kb.buildAll();

            if (kb.getResults().hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
                throw new RuntimeException("Build Errors:\n"
                        + kb.getResults().toString());
            }

            KieContainer kContainer = kieServices.newKieContainer(kieRepository
                    .getDefaultReleaseId());
            return kContainer.newStatelessKieSession();
        }
    }
}