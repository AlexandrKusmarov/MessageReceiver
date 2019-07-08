import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * This app. reading messages from the queue.
 * You can change params for connection to the RabbitMQ server.
 */

public class MessageReceiver {

    private static final String EXCHANGE_NAME = "genericEx";
    private static final String QUEUE_NAME = "genericQ";
    private static final String ROUTING_KEY_NAME = "key1";

    private ConnectionFactory factory = new ConnectionFactory();

    {
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        factory.setHost("localhost");
        factory.setPort(5672);
    }

    public void messageConsumer(Integer delayBetweenMessages) throws IOException, TimeoutException {

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY_NAME);

        System.out.println("Waiting for messages.");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                Thread.sleep(delayBetweenMessages);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Received message'" + message + "'");
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        MessageReceiver receiver = new MessageReceiver();
        receiver.messageConsumer(50);
        System.out.println("Queue is empty.\nReady for new messages...");
    }
}

