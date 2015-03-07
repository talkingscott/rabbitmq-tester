package talkingscott.rabbitmq_tester;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Somewhat configurable RabbitMQ test publisher
 * 
 * @author talkingscott
 */
public class PublisherApp 
{
	private static Logger log = LoggerFactory.getLogger(PublisherApp.class);
	
	private static volatile boolean shutdown = false;
	private static Object shutdown_event = new Object();

    public static void main(String[] args)
    {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("PublisherApp")
                .defaultHelp(true)
                .description("Somewhat configurable RabbitMQ test publisher.");
        parser.addArgument("-p", "--properties")
                .setDefault("default.properties")
                .help("Specify the (resource) file from which to read properties");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        
        try {
	        Properties properties = loadProperties(ns.getString("properties"));
	
			ExecutionModel em = new ExecutionModelBuilder()
				.largeMessageFraction(Float.parseFloat(properties.getProperty("large_message_fraction")))
				.minLargeMessageBytes(Integer.parseInt(properties.getProperty("min_large_message_bytes")))
				.maxLargeMessageBytes(Integer.parseInt(properties.getProperty("max_large_message_bytes")))
				.minRegularMessageBytes(Integer.parseInt(properties.getProperty("min_regular_message_bytes")))
				.maxRegularMessageBytes(Integer.parseInt(properties.getProperty("max_regular_message_bytes")))
				.maxPrepareMs(Long.parseLong(properties.getProperty("max_prepare_ms")))
				.minPrepareMs(Long.parseLong(properties.getProperty("min_prepare_ms")))
				.maxPauseMs(Long.parseLong(properties.getProperty("max_pause_ms")))
				.minPauseMs(Long.parseLong(properties.getProperty("min_pause_ms")))
				.pauseFrequency(Float.parseFloat(properties.getProperty("pause_frequency")))
				.build();

			Thread t = startPublisher(properties.getProperty("host"), 
	        		Integer.parseInt(properties.getProperty("port")),
	        		properties.getProperty("vhost"),
	        		properties.getProperty("username"),
	        		properties.getProperty("password"),
	        		properties.getProperty("exchange"),
	        		Integer.parseInt(properties.getProperty("delivery_mode")),
	        		em);
	        
	        log.info("Wait for keyboard input");
	        System.out.println("Press ENTER to terminate");
			try {
				System.in.read();
				log.info("Keyboard input received");
			} catch (IOException e) {
				log.error("Reading System.in", e);
			}

			log.info("Start shutdown");
			synchronized (shutdown_event) {
				shutdown = true;
				shutdown_event.notifyAll();
			}

			log.info("Join publisher");
			try {
				t.join(60000);
			} catch (InterruptedException e) {
				log.error("Joining", e);
			}

			log.info("Shutdown complete");
        } catch (Exception e) {
        	log.error("In main", e);
        	e.printStackTrace();
        }
    }
    
    private static Properties loadProperties(String path) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) {
        	is = new FileInputStream(path);
        }
    	try (Reader r = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")))) {
    		Properties properties = new Properties();
    		properties.load(r);
    		return properties;
    	}
    }
    
    private static Thread startPublisher(String host, int port, String vhost, String username, String password, String exchange, int delivery_mode, ExecutionModel em) throws IOException {
    	ConnectionFactory cf = new ConnectionFactory();
    	cf.setHost(host);
    	cf.setPort(port);
    	cf.setVirtualHost(vhost);
    	cf.setUsername(username);
    	cf.setPassword(password);
    	
    	Connection conn = cf.newConnection();
    	
    	Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					long msg_id = ThreadLocalRandom.current().nextLong(100000000, 200000000);
					Channel ch = conn.createChannel();
					log.info("Start publisher loop");
					while (!shutdown) {
						try {
							String message_id = Long.toString(msg_id);
							AMQP.BasicProperties props = new AMQP.BasicProperties();
							props = props.builder()
									.contentType("application/octet-stream")
									.deliveryMode(delivery_mode)
									.messageId(message_id)
									.priority(0)
									.build();
							int body_length = em.bodyLength();
							byte[] body = new byte[body_length];
							long prepare_time = em.prepareTime();
							if (prepare_time > 0) {
								synchronized (shutdown_event) {
									if (!shutdown) {
										shutdown_event.wait(prepare_time);
									}
								}
							}
							long millis = System.currentTimeMillis();
							ch.basicPublish(exchange, "tester.tester." + message_id, false, false, props, body);
							long publish_ms = System.currentTimeMillis() - millis;
							log.info("publish msg: " + message_id + " length: " + body_length + " publish: " + publish_ms);
							++msg_id;
							long pause_time = em.pauseTime();
							if (pause_time > 0) {
								synchronized (shutdown_event) {
									if (!shutdown) {
										log.info("pause publish: " + pause_time);
										shutdown_event.wait(pause_time);
									}
								}
							}
						} catch (InterruptedException e) {
							log.error("Waiting", e);
						}
					}
				} catch (IOException e) {
					log.error("Publisher loop", e);
				}
				log.info("Publisher shutting down");
				try {
					conn.close();
				} catch (IOException e) {
					log.error("Closing connection", e);
				}
				log.info("Publisher shut down");
			}
    	}, "Publisher");
    	t.start();
    	return t;
    }

    private interface ExecutionModel {
    	/**
    	 * Models the length of message bodies
    	 */
        int bodyLength();

        /**
         * Models the pause time between bursts of messages
         */
        long pauseTime();
        
        /**
         * Models the time required for the publisher to prepare the message
         */
        long prepareTime();
    }
    
    /**
     * Build the ExecutionModel rather than construct it to avoid a
     * ctor with a pathologically large argument count.
     */
    private static class ExecutionModelBuilder {
    
    	private float large_message_fraction = (float) 0.25;
    	private int min_large_message_bytes = 100000;
    	private int max_large_message_bytes = 200000;
    	private int min_regular_message_bytes = 2000;
    	private int max_regular_message_bytes = 8000;
    	
		private long min_prepare_ms = 0;
		private long max_prepare_ms = 10;

		private long min_pause_ms = 30000;
		private long max_pause_ms = 300000;
		private float pause_frequency = (float) 0.00001;

		ExecutionModel build() {
			return new ExecutionModel() {
				@Override
				public int bodyLength() {
			    	// TODO: model this better
			    	if (ThreadLocalRandom.current().nextFloat() < large_message_fraction) {
			    		return ThreadLocalRandom.current().nextInt(min_large_message_bytes, max_large_message_bytes);
			    	}
					return ThreadLocalRandom.current().nextInt(min_regular_message_bytes, max_regular_message_bytes);
				}

				@Override
				public long pauseTime() {
			    	// TODO: model this better
			    	if (ThreadLocalRandom.current().nextFloat() < pause_frequency) {
			        	return ThreadLocalRandom.current().nextLong(min_pause_ms, max_pause_ms);
			    	}
					return 0;
				}

				@Override
				public long prepareTime() {
			    	// TODO: model this better
			    	return ThreadLocalRandom.current().nextLong(min_prepare_ms, max_prepare_ms);
				}
				
			};
    	}
    	
    	ExecutionModelBuilder largeMessageFraction(float large_message_fraction) {
    		this.large_message_fraction = large_message_fraction;
    		return this;
    	}

    	ExecutionModelBuilder maxLargeMessageBytes(int max_large_message_bytes) {
    		this.max_large_message_bytes = max_large_message_bytes;
    		return this;
    	}
 
    	ExecutionModelBuilder minLargeMessageBytes(int min_large_message_bytes) {
    		this.min_large_message_bytes = min_large_message_bytes;
    		return this;
    	}
 
    	ExecutionModelBuilder maxRegularMessageBytes(int max_regular_message_bytes) {
    		this.max_regular_message_bytes = max_regular_message_bytes;
    		return this;
    	}
 
    	ExecutionModelBuilder minRegularMessageBytes(int min_regular_message_bytes) {
    		this.min_regular_message_bytes = min_regular_message_bytes;
    		return this;
    	}
 
    	ExecutionModelBuilder maxPrepareMs(long max_prepare_ms) {
    		this.max_prepare_ms = max_prepare_ms;
    		return this;
    	}
 
    	ExecutionModelBuilder minPrepareMs(long min_prepare_ms) {
    		this.min_prepare_ms = min_prepare_ms;
    		return this;
    	}
 
    	ExecutionModelBuilder maxPauseMs(long max_pause_ms) {
    		this.max_pause_ms = max_pause_ms;
    		return this;
    	}
 
    	ExecutionModelBuilder minPauseMs(long min_pause_ms) {
    		this.min_pause_ms = min_pause_ms;
    		return this;
    	}
 
    	ExecutionModelBuilder pauseFrequency(float pause_frequency) {
    		this.pause_frequency = pause_frequency;
    		return this;
    	}
    }
}
