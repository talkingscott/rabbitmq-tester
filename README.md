# rabbitmq-tester
Somewhat configurable publisher and consumer for running RabbitMQ tests

## Introduction
This was written to provide a configurable workload for a RabbitMQ exchange and bound queue.  I wanted to be able to reproduce flow control and consumer starvation issues I was seeing on "real" systems I work on.  It is not suitable for all-out stress testing using transient deliveries, because it is designed with notions that

* Publishers are bursty and pause for longish periods of time
* Publishers require some amount of time to produce the messages they publish
* Consumers require some amount of time to process the messages they consume
 
The test publisher and consumer are implemented in Java.  I wanted to be able to use Windows, OS X or Linux client boxes, and I've found the RabbitMQ Java client to be rock solid, whereas the python client I use (pika) is occasionally flaky.

The test consumer uses QueueingConsumer.  That may not be the ideal consumer implementation, but it nicely allows the measurement of consumer "poll time", which is more-or-less time the consumer spends waiting for a message from the server (which I call "consumer starvation").

## Building
This is a maven project.  I work with it using eclipse, but it can be built from the command line with maven.  I use the shade plug-in to produce a shaded jar that includes all dependencies.  My eclipse project specifies Java 8, but I doubt there is any code that won't work on Java 7.

## Running

Run-time behavior is configured by a properties file.  The command line -p argument specifies a properties file to read.  If none is specified, default.properties from the jar file will be used.  Not surprisingly, the properties files I use are in my project and thus in the jar.  You can specify a properties file in the filesystem as well.  The project includes Windows bat files to run the consumer and publisher which can get you started with sample command lines.

The apps run until ENTER is pressed on the keyboard or they are otherwise killed.

The apps write diagnostic and performance information to log files via slf4j.  The project includes a logback configuration file that puts publisher output into a file named PublisherApp.log and consumer output into a file named ConsumerApp.log.  You can change this behavior without changing any code by including your own logback configuration file before the jar in your classpath, or by including the appropriate slf4j adapter and logging jars with config file in your classpath.

There is a simple python script to parse the log files.
