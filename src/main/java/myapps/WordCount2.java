/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount2 {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        
        // 1) Properties should have application.id ,bootstrap.servers,default.key.serde,defalut.value.serde 
        
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-first-by-article-3");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "X:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // 2) should create StreamBuilder which will have processing logic
        final StreamsBuilder builder = new StreamsBuilder();
        
//        KStream<String, Long>  countStream= builder
//        .<String,String>stream("paintext")
//        .flatMapValues((text)->{
//        	return Arrays.asList(text.split(" "));
//        })
//        .selectKey((k,v)->{
//        	return v;
//        })
//        .groupByKey()
//        .count()
//        .toStream();
//        
//        countStream.foreach((k,v)->{
//        	System.out.println(k +" " + v);
//        });
//      
        class WC{
        	String article;
        	String word;
			public WC(String article, String word) {
				super();
				this.article = article;
				this.word = word;
			}
        	
        	
        	
        }
        
        
        builder.<String,String>stream("paintext")
        	   .flatMapValues((text)->{
        		   return Arrays.asList(text.split(" "));
        	   })
        	   .groupByKey()
        	   .count()
        	   .toStream()
        	   .foreach((k,v)->{
             	System.out.println(k +" " + v);
               });
        	   
        
        
        // 3) bulid the topology out of builder using build() 
        final Topology topology = builder.build();
        // 4) create Kafkastreams using topology and props
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
        // 6) close the stream    	
                streams.close();
                latch.countDown();
            }
        });

        // 5) start the stream 
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
