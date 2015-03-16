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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;

import com.google.common.util.concurrent.AtomicDouble;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Somewhat configurable RabbitMQ test consumer
 * 
 * @author talkingscott
 */
public class ConsumerApp 
{
	private static Logger log = LoggerFactory.getLogger(ConsumerApp.class);

	private static volatile boolean shutdown = false;
	private static Object shutdown_event = new Object();

	private static AtomicDouble long_process_fraction_bias = new AtomicDouble();
	private static AtomicLong regular_process_ms_bias = new AtomicLong();

	public static void main(String[] args)
	{
		ArgumentParser parser = ArgumentParsers.newArgumentParser("ConsumerApp")
				.defaultHelp(true)
				.description("Somewhat configurable RabbitMQ test consumer.");
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
			.longProcessFraction(Float.parseFloat(properties.getProperty("long_process_fraction")))
			.minLongProcessMs(Long.parseLong(properties.getProperty("min_long_process_ms")))
			.maxLongProcessMs(Long.parseLong(properties.getProperty("max_long_process_ms")))
			.minRegularProcessMs(Long.parseLong(properties.getProperty("min_regular_process_ms")))
			.maxRegularProcessMs(Long.parseLong(properties.getProperty("max_regular_process_ms")))
			.build();

			Thread bias_updater = startBiasUpdater(em);

			Thread consumer_master = startConsumer(properties.getProperty("host"), 
					Integer.parseInt(properties.getProperty("port")),
					properties.getProperty("vhost"),
					properties.getProperty("username"),
					properties.getProperty("password"),
					properties.getProperty("queue"),
					Integer.parseInt(properties.getProperty("prefetch")),
					Integer.parseInt(properties.getProperty("consumers")),
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

			log.info("Join consumer");
			try {
				consumer_master.join(60000);
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
				double max_long_process_fraction_bias = em.getLongProcessFraction() * 0.5;
				double min_long_process_fraction_bias = -max_long_process_fraction_bias;
				long max_regular_process_ms_bias = (1 + em.getMaxRegularProcessMs() - em.getMinRegularProcessMs()) / 4;
				long min_regular_process_ms_bias = -max_regular_process_ms_bias;
				log.info("Start bias updater loop");
				while (!shutdown) {
					if (min_long_process_fraction_bias < max_long_process_fraction_bias) {
						long_process_fraction_bias.set(ThreadLocalRandom.current().nextDouble(min_long_process_fraction_bias, max_long_process_fraction_bias));
					} else {
						long_process_fraction_bias.set(min_long_process_fraction_bias);
					}
					if (max_regular_process_ms_bias > 0) {
						regular_process_ms_bias.set(ThreadLocalRandom.current().nextLong(min_regular_process_ms_bias, max_regular_process_ms_bias + 1));
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

	private static Thread startConsumer(String host, int port, String vhost, String username, String password, String queue, int prefetch, int consumers, ExecutionModel em) throws IOException {
		final ConnectionFactory cf = new ConnectionFactory();
		cf.setHost(host);
		cf.setPort(port);
		cf.setVirtualHost(vhost);
		cf.setUsername(username);
		cf.setPassword(password);
		cf.setAutomaticRecoveryEnabled(true);

		final Connection conn = cf.newConnection();

		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				// This is the consumer "master" thread

				// Create the consumer threads
				log.info("Consumer master creating consumers");
				Thread[] consumer_threads = new Thread[consumers];
				for (int i = 0; i < consumers; i++) {
					consumer_threads[i] = new Thread(new Runnable() {
						@Override
						public void run() {
							// This is an actual consumer thread
							try {
								// Create a channel and start consuming
								log.info("Create a channel and start consuming");
								Channel ch = conn.createChannel();
								ch.basicQos(prefetch);
								QueueingConsumer qc = new QueueingConsumer(ch);
								String consumer_tag = ch.basicConsume(queue, false, qc);
								long delivery_wait = 100;
								int no_delivery = 0;

								log.info("Enter consumer loop for consumer " + consumer_tag + " on channel " + ch.getChannelNumber());
								while (!shutdown) {
									try {
										long millis = System.currentTimeMillis();
										Delivery delivery = qc.nextDelivery(delivery_wait);
										long poll_ms = System.currentTimeMillis() - millis;
										if (delivery != null) {
											long process_ms = em.processTime(delivery.getBody());
											if (process_ms > 0) {
												synchronized (shutdown_event) {
													if (!shutdown) {
														shutdown_event.wait(process_ms);
													}
												}
											}
											millis = System.currentTimeMillis();
											ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
											long ack_ms = System.currentTimeMillis() - millis;
											log.info("consume msg: " + delivery.getProperties().getMessageId() + " poll: " + (poll_ms + (no_delivery * delivery_wait)) + " process: " + process_ms + " ack: " + ack_ms);
											no_delivery = 0;
										} else {
											++no_delivery;
										}
									} catch (InterruptedException e) {
										log.error("Waiting", e);
									}
								}

								// clean up
								log.info("Left consumer loop so cancel consumer " + consumer_tag);
								ch.basicCancel(consumer_tag);

								log.info("Close channel " + ch.getChannelNumber());
								ch.close();
							} catch (IOException e) {
								log.error("Consumer loop", e);
							}
							log.info("Consumer done");
						}
					}, "Consumer-" + i);
					consumer_threads[i].start();
				}

				// wait for shutdown
				log.info("Consumer master waiting for shutdown");
				while (!shutdown) {
					synchronized (shutdown_event) {
						try {
							shutdown_event.wait(60000);
						} catch (InterruptedException e) {
							log.error("Waiting", e);
						}
					}
				}

				// wait for the consumer threads to exit
				log.info("Consumer master joining consumer threads");
				for (int i = 0; i < consumers; i++) {
					if (consumer_threads[i].isAlive()) {
						try {
							log.info("Join consumer " + i);
							consumer_threads[i].join(60000);
						} catch (InterruptedException e) {
							log.error("Joining consumer thread " + i, e);
						}
					} else {
						log.info("Consumer " + i + " not alive");
					}
				}

				// clean up
				log.info("Consumer master closing connection");
				try {
					conn.close();
				} catch (IOException e) {
					log.error("Closing connection", e);
				}

				log.info("Consumer master done");
			}
		}, "ConsumerMaster");
		t.start();
		return t;
	}

	private interface ExecutionModel {
		/**
		 * Models the time required for a consumer to process a message
		 */
		long processTime(byte[] body);

		float getLongProcessFraction();

		long getMaxRegularProcessMs();

		long getMinRegularProcessMs();
	}

	/**
	 * Build the ExecutionModel rather than construct it to avoid a
	 * ctor with a pathologically large argument count.
	 */
	private static class ExecutionModelBuilder {

		private float long_process_fraction = (float) 0.001;
		private long min_long_process_ms = 200;
		private long max_long_process_ms = 2000;
		private long min_regular_process_ms = 20;
		private long max_regular_process_ms = 80;

		ExecutionModel build() {
			return new ExecutionModel() {
				@Override
				public long processTime(byte[] body) {
					// TODO: model this better, possibly with Gaussion distribution and accounting for body length
					if (ThreadLocalRandom.current().nextFloat() < long_process_fraction - long_process_fraction_bias.get()) {
						return ThreadLocalRandom.current().nextLong(min_long_process_ms, max_long_process_ms + 1);
					}
					long process_ms = ThreadLocalRandom.current().nextLong(min_regular_process_ms, max_regular_process_ms + 1) - regular_process_ms_bias.get();
					return process_ms >= 0 ? process_ms : 0;
				}

				@Override
				public float getLongProcessFraction() {
					return long_process_fraction;
				}

				@Override
				public long getMaxRegularProcessMs() {
					return max_regular_process_ms;
				}

				@Override
				public long getMinRegularProcessMs() {
					return min_regular_process_ms;
				}
			};
		}

		ExecutionModelBuilder longProcessFraction(float long_process_fraction) {
			this.long_process_fraction = long_process_fraction;
			return this;
		}

		ExecutionModelBuilder maxLongProcessMs(long max_long_process_ms) {
			this.max_long_process_ms = max_long_process_ms;
			return this;
		}

		ExecutionModelBuilder minLongProcessMs(long min_long_process_ms) {
			this.min_long_process_ms = min_long_process_ms;
			return this;
		}

		ExecutionModelBuilder maxRegularProcessMs(long max_regular_process_ms) {
			this.max_regular_process_ms = max_regular_process_ms;
			return this;
		}

		ExecutionModelBuilder minRegularProcessMs(long min_regular_process_ms) {
			this.min_regular_process_ms = min_regular_process_ms;
			return this;
		}
	}
}
