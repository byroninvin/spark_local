//package kafka;
//
//import kafka.javaapi.producer.Producer;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
//
//import java.util.Properties;
//
//
//public class ProducerDemo {
//
//	public static void main(String[] args) {
//		Properties props = new Properties();
//		props.put("metadata.broker.list", "node-4:9092,node-5:9092,node-6:9092");
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		ProducerConfig config = new ProducerConfig(props);
//		Producer<String, String> producer = new Producer<String, String>(config);
//		for (int i = 1001; i <= 1100; i++)
//			producer.send(new KeyedMessage<String, String>("xiaoniu", "xiaoniu-msg" + i));
//	}
//}