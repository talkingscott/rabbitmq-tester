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
import java.util.concurrent.atomic.AtomicLong;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

import com.google.common.util.concurrent.AtomicDouble;
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

	private static AtomicDouble large_message_fraction_bias = new AtomicDouble();
	private static AtomicLong prepare_ms_bias = new AtomicLong();

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
			.maxInterPauseMsgs(Long.parseLong(properties.getProperty("max_interpause_msgs")))
			.minInterPauseMsgs(Long.parseLong(properties.getProperty("min_interpause_msgs")))
			.build();

			Thread bias_updater = startBiasUpdater(em);

			Thread publisher_master = startPublisher(properties.getProperty("host"), 
					Integer.parseInt(properties.getProperty("port")),
					properties.getProperty("vhost"),
					properties.getProperty("username"),
					properties.getProperty("password"),
					properties.getProperty("exchange"),
					Integer.parseInt(properties.getProperty("delivery_mode")),
					Integer.parseInt(properties.getProperty("publishers")),
					Boolean.parseBoolean(properties.getProperty("connection_per_publisher")),
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
				publisher_master.join(60000);
			} catch (InterruptedException e) {
				log.error("Joining", e);
			}

			log.info("Join bias updater");
			try {
				bias_updater.join(60000);
			} catch (InterruptedException e) {
				log.error("Joining", e);
			}

			log.info("Shutdown complete");
			
			LoggerContext logger_context = (LoggerContext) LoggerFactory.getILoggerFactory();
			logger_context.stop();
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

	private static Thread startBiasUpdater(ExecutionModel em) {
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				double max_large_message_fraction_bias = em.getLargeMessageFraction() * 0.5;
				double min_large_message_fraction_bias = -max_large_message_fraction_bias;
				long max_prepare_ms_bias = (1 + em.getMaxPrepareMs() - em.getMinPrepareMs()) / 4;
				long min_prepare_ms_bias = -max_prepare_ms_bias;
				log.info("Start bias updater loop");
				while (!shutdown) {
					if (min_large_message_fraction_bias < max_large_message_fraction_bias) {
						large_message_fraction_bias.set(ThreadLocalRandom.current().nextDouble(min_large_message_fraction_bias, max_large_message_fraction_bias));
					} else {
						large_message_fraction_bias.set(min_large_message_fraction_bias);
					}
					if (max_prepare_ms_bias > 0) {
						prepare_ms_bias.set(ThreadLocalRandom.current().nextLong(min_prepare_ms_bias, max_prepare_ms_bias + 1));
					}
					synchronized (shutdown_event) {
						try {
							shutdown_event.wait(5000);
						} catch (InterruptedException e) {
							log.error("waiting", e);
						}
					}
				}
				log.info("Bias updater shut down");
			}
		}, "BiasUpdater");
		t.start();
		return t;
	}

	private static Thread startPublisher(String host, int port, String vhost, String username, String password, String exchange, int delivery_mode, int publishers, boolean connection_per_publisher, ExecutionModel em) throws IOException {
		final ConnectionFactory cf = new ConnectionFactory();
		cf.setHost(host);
		cf.setPort(port);
		cf.setVirtualHost(vhost);
		cf.setUsername(username);
		cf.setPassword(password);
		cf.setAutomaticRecoveryEnabled(true);

		final Connection conn = connection_per_publisher ? null : cf.newConnection();

		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				// This is the publisher "master" thread

				// Create the publisher threads
				log.info("Publisher master creating publishers");
				Thread[] publisher_threads = new Thread[publishers];
				for (int i = 0; i < publishers; i++) {
					final long msg_id_base = i * 200000000;
					publisher_threads[i] = new Thread(new Runnable() {
						@Override
						public void run() {
							// This is an actual publisher thread
							try {
								long msg_id = ThreadLocalRandom.current().nextLong(100000000 + msg_id_base, 200000000 + msg_id_base);
								int msg_count = 0;
								long msgs_until_pause = em.messagesUntilPause();
								Connection p_conn = connection_per_publisher ? cf.newConnection() : null;
								Channel ch = connection_per_publisher ? p_conn.createChannel() : conn.createChannel();

								log.info("Enter publisher loop on channel " + ch.getChannelNumber());
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
										++msg_count;
										--msgs_until_pause;
										if (msgs_until_pause <= 0) {
											long pause_time = em.pauseTime();
											synchronized (shutdown_event) {
												if (!shutdown) {
													log.info("pause publish: " + pause_time + " messages: " + msg_count);
													shutdown_event.wait(pause_time);
												}
											}
											msgs_until_pause = em.messagesUntilPause();
										}
									} catch (InterruptedException e) {
										log.error("Waiting", e);
									}
								}

								// clean up
								log.info("Close channel " + ch.getChannelNumber());
								ch.close();

								if (connection_per_publisher) {
									log.info("Close connection");
									p_conn.close();
								}
							} catch (IOException e) {
								log.error("Publisher loop", e);
							}
							log.info("Publisher done");
						}
					}, "Publisher-" + i);
					publisher_threads[i].start();
				}

				// wait for shutdown
				log.info("Publisher master waiting for shutdown");
				while (!shutdown) {
					synchronized (shutdown_event) {
						try {
							shutdown_event.wait(60000);
						} catch (InterruptedException e) {
							log.error("Waiting", e);
						}
					}
				}

				// wait for the publisher threads to exit
				log.info("Consumer master joining publisher threads");
				for (int i = 0; i < publishers; i++) {
					if (publisher_threads[i].isAlive()) {
						try {
							log.info("Join publisher " + i);
							publisher_threads[i].join(60000);
						} catch (InterruptedException e) {
							log.error("Joining publisher thread " + i, e);
						}
					} else {
						log.info("Publisher " + i + " not alive");
					}
				}

				// clean up
				if (!connection_per_publisher) {
					log.info("Publisher master closing connection");
					try {
						conn.close();
					} catch (IOException e) {
						log.error("Closing connection", e);
					}
				}

				log.info("Publisher master done");
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
		 * Models the length of message burst until the next pause
		 */
		long messagesUntilPause();

		/**
		 * Models the pause time between bursts of messages
		 */
		long pauseTime();

		/**
		 * Models the time required for the publisher to prepare the message
		 */
		long prepareTime();

		float getLargeMessageFraction();

		long getMaxPrepareMs();

		long getMinPrepareMs();
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
		private long min_interpause_msgs = 100000;
		private long max_interpause_msgs = 200000;

		ExecutionModel build() {
			return new ExecutionModel() {
				@Override
				public int bodyLength() {
					// TODO: model this better
					if (ThreadLocalRandom.current().nextFloat() < large_message_fraction + large_message_fraction_bias.get()) {
						if (min_large_message_bytes < max_large_message_bytes) {
							return ThreadLocalRandom.current().nextInt(min_large_message_bytes, max_large_message_bytes);
						}
						return min_large_message_bytes;
					}
					if (min_regular_message_bytes < max_regular_message_bytes) {
						return ThreadLocalRandom.current().nextInt(min_regular_message_bytes, max_regular_message_bytes);
					}
					return min_regular_message_bytes;
				}

				@Override
				public long messagesUntilPause() {
					// TODO: model this better
					return ThreadLocalRandom.current().nextLong(min_interpause_msgs, max_interpause_msgs + 1);
				}

				@Override
				public long pauseTime() {
					// TODO: model this better
					return ThreadLocalRandom.current().nextLong(min_pause_ms, max_pause_ms + 1);
				}

				@Override
				public long prepareTime() {
					// TODO: model this better
					long prepare_ms = ThreadLocalRandom.current().nextLong(min_prepare_ms, max_prepare_ms + 1) + prepare_ms_bias.get();
					return prepare_ms >= 0 ? prepare_ms : 0;
				}

				@Override
				public float getLargeMessageFraction() {
					return large_message_fraction;
				}

				@Override
				public long getMaxPrepareMs() {
					return max_prepare_ms;
				}

				@Override
				public long getMinPrepareMs() {
					return min_prepare_ms;
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

		ExecutionModelBuilder maxInterPauseMsgs(long max_interpause_msgs) {
			this.max_interpause_msgs = max_interpause_msgs;
			return this;
		}

		ExecutionModelBuilder minInterPauseMsgs(long min_interpause_msgs) {
			this.min_interpause_msgs = min_interpause_msgs;
			return this;
		}

	}
}
