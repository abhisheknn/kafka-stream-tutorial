package myapps;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;

public class NoOfProductsSoldInEachCountry {

	public static void main(String[] args) {
final CountDownLatch countDownLatch = new CountDownLatch(1);
		
		
		Properties properties= new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-country");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "X:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		Map<String, Object> serdeProps = new HashMap<>();
		StreamsBuilder builder = new StreamsBuilder();
	
		builder
		.stream("product-country")
		.selectKey((k,v)->{return v;})
		.groupByKey()
		.count()
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
