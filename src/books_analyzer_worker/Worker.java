package books_analyzer_worker;
import com.rabbitmq.client.*;

import books_analyzer_dao.Book;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.HashMap;

public class Worker {
  private static final String QUEUE_NAME = "jobs_queue";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
	factory.setHost("fox.rmq.cloudamqp.com");
	factory.setUsername("xhffrluv");
	factory.setPassword("zIU2WWJtvjsDUv_BrwCoU60RWcekbpvP");
	factory.setVirtualHost("xhffrluv");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    channel.basicQos(1); // 1 is a prefetch number: if the worker is already processing 1 message, give it to another one

    final Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + message + "'");
        try {
          process(message);
        } finally {
          System.out.println(" [x] Done");
          channel.basicAck(envelope.getDeliveryTag(), false);
        }
      }
    };
    channel.basicConsume(QUEUE_NAME, false, consumer);
  }
  
  private static void process(String idBook) {
	  // The message contains one id of a book in db.
	  DBBookExporter dbExporter = new DBBookExporter();
	  HashMap<String,String> metadata = dbExporter.getMetadata(idBook);

	  Book book = new Book(metadata.get("title"), metadata.get("author"), metadata.get("url"));
	  dbExporter.updateFlag(idBook, 1);
	  String content = getTxtFromUrl(metadata.get("url"));
	  
	  dbExporter.updateFlag(idBook, 2);
	  book.generateSentences(content);
	  dbExporter.exportSentences(idBook, book);
	  
	  dbExporter.updateFlag(idBook, 3);
	  book.generateCharacters();
	  dbExporter.exportCharacters(idBook, book);
	  
	  dbExporter.updateFlag(idBook, 4);
	  book.generateAssociations();
	  dbExporter.exportCharacterSentences(idBook, book);
	  
	  dbExporter.updateFlag(idBook, 4);

	  dbExporter.updateFlag(idBook, 5);
  }
  
  private static String getTxtFromUrl(String urlString) {
		System.out.println("Fetching content from:" + urlString);
		StringBuilder sb = new StringBuilder();
		URLConnection urlConn = null;
		InputStreamReader in = null;
		try {
			URL url = new URL(urlString);
			urlConn = url.openConnection();
			urlConn.setRequestProperty("User-Agent", "Chrome/23.0.1271.95");
			if (urlConn != null)
				urlConn.setReadTimeout(60000);
			if (urlConn != null && urlConn.getInputStream() != null) {
				in = new InputStreamReader(urlConn.getInputStream(),
						Charset.defaultCharset());
				BufferedReader bufferedReader = new BufferedReader(in);
				if (bufferedReader != null) {
					int cp;
					while ((cp = bufferedReader.read()) != -1) {
						sb.append((char) cp);
					}
					bufferedReader.close();
				}
			}
		in.close();
		} catch (Exception e) {
			throw new RuntimeException("A problem occured while calling URL:"+ urlString, e);
		} 

		return sb.toString();
	}
}