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

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class AvgPurchaseAsEvent {

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
	
   
   static class Avg{
	   Long count=0L;
	   Double amt=0d;
	  static Avg avg= null;
	  public static Avg getInstance() {
		  if(avg==null) avg=new Avg();
		  return avg;
	  }
	public Long getCount() {
		return count;
	}
	public void setCount(Long count) {
		this.count = count;
	}
	public Double getAmt() {
		return amt;
	}
	public void setAmt(Double amt) {
		this.amt = amt;
	}
	   
   }
   
	public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "avg_purchase_5");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "X:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Map<String, Object> serdeProps = new HashMap<>();
        Serde<HashSet> hashSetSerde = SerdeFactory.createSerde(HashSet.class, serdeProps);
        Serde<Double> doubleSerde = SerdeFactory.createSerde(Double.class, serdeProps);
        Serde<Avg> avgSerde = SerdeFactory.createSerde(Avg.class, serdeProps);
        Gson gson  = new Gson();
       Type hashMapType= new TypeToken<Map<String, Object>>(){}.getType(); 
        final StreamsBuilder builder = new StreamsBuilder();
        
        
        builder
        .<String,String>stream("orders")
        .mapValues((v)->{
        	 Map map =gson.fromJson(v, hashMapType);
        			return map;
        }).mapValues((k)->{
        	return (Double)k.get("purch_amt");
        })
        //.selectKey((k,v)->{return "";})
        .groupByKey(Grouped.with(Serdes.String(), doubleSerde))
        .aggregate(()->{
        	return Avg.getInstance();
        }, (key,newVal,agg)->{
        	Long count= agg.getCount();
        	agg.setCount(++count);
        	Double amnt= agg.getAmt();
        	amnt=amnt+newVal;
        	agg.setAmt(amnt);
        	return agg;
        	},Materialized.with(Serdes.String(), avgSerde))
        	.toStream().foreach((k,v)->{System.out.println(v.getAmt()/v.getCount());});
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
