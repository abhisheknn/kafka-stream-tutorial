package myapps;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Materialized;

public class CityToTemperature {

    public static <T> Serde<T> createSerde(Class<T> clazz, Map<String, Object> serdeProps) {
        Serializer<T> serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", clazz);
        serializer.configure(serdeProps, false);

        Deserializer<T> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", clazz);
        deserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(serializer, deserializer);
    }

	
	public static void main(String[] args) {
		
		// properties should have application.id and bootstrap broker 
		
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		
		
		Properties properties= new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "city-to-temprature_1");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "X:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		Map<String, Object> serdeProps = new HashMap<>();
		Serde<HashMap> hashMapSerde=CityToTemperature.createSerde(HashMap.class, serdeProps);
		
		// StreamBuilder
		
		StreamsBuilder builder = new StreamsBuilder();
		
		//logic
		builder
		.<String,String>stream("city-with-temperatue")
		.groupByKey()
		.aggregate(()->new HashMap<String,Integer>(),
				   (key,newValue,agg)->{
					   Integer temp=(Integer)agg.get(key);
					   if(temp==null) {temp=Integer.MIN_VALUE;}
					   Integer newVal=Integer.parseInt(newValue);
					   if(temp<newVal)
						   agg.put(key, newVal);
					   return agg;
				   },Materialized.with(Serdes.String(), hashMapSerde))
		
			.toStream()
			.foreach((k,v)->{
				System.out.println(k+" "+v);
			});
		
		Topology topology = builder.build();
		KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				kafkaStreams.close();
				 countDownLatch.countDown();
			}
		});
		try {
			kafkaStreams.start();
		} catch (StreamsException e) {
			e.printStackTrace();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		}
	}
}
