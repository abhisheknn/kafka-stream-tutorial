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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import myapps.MinPurchaseAsEvent.Min;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MaxPurchaseAsEvent {

   static class SerdeFactory {
        public static <T> Serde<T> createSerde(Class<T> clazz, Map<String, Object> serdeProps) {
            Serializer<T> serializer = new JsonPOJOSerializer<>();
            serdeProps.put("JsonPOJOClass", clazz);
            serializer.configure(serdeProps, false);

            Deserializer<T> deserializer = new JsonPOJODeserializer<>();
            serdeProps.put("JsonPOJOClass", clazz);
            deserializer.configure(serdeProps, false);

            return Serdes.serdeFrom(serializer, deserializer);
        }
    }
   
   
   static class Max{
	   Double maximum=0d;
	   Map<String, Object> record;
	
	public Double getMaximum() {
		return maximum;
	}
	public void setMaximum(Double maximum) {
		this.maximum = maximum;
	}
	public Map<String, Object> getRecord() {
		return record;
	}
	public void setRecord(Map<String, Object> record) {
		this.record = record;
	}
	
	public static Max max=null;
	
	public static Max getInstance() {
		if(max==null) max= new Max();
		return max;
	}
	
}
	

	public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "max_purchase_4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "X:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Map<String, Object> serdeProps = new HashMap<>();
        Serde<Map> hashMapSerde = SerdeFactory.createSerde(Map.class, serdeProps);
        Serde<Double> doubleSerde = SerdeFactory.createSerde(Double.class, serdeProps);
        Serde<Max> maxSerde = SerdeFactory.createSerde(Max.class, serdeProps);
        Gson gson  = new Gson();
        Type hashMapType= new TypeToken<Map<String, Object>>(){}.getType(); 
        final StreamsBuilder builder = new StreamsBuilder();
        
        builder
        .<String,String>stream("orders")
        . mapValues((v)->{
       	 Map map =gson.fromJson(v, hashMapType);
			return map;
        }).selectKey((k,v)->{return "";})
        .groupByKey(Grouped.with(Serdes.String(), hashMapSerde))
        .aggregate(()->{return Max.getInstance();}, (kev,val,agg)->{
        	double purch_amt=(Double)val.get("purch_amt");
        	if(purch_amt>agg.getMaximum()) { agg.setMaximum(purch_amt); agg.setRecord(val);} return agg;
        },Materialized.with(Serdes.String(), maxSerde))
        .toStream()
        .foreach((k,v)->{System.out.println(k+"_"+gson.toJson(v));});
        ;		
        
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
