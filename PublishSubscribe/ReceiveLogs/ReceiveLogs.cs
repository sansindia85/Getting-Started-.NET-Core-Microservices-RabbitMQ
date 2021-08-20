using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace ReceiveLogs
{
    class ReceiveLogs
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs",
                    type: ExchangeType.Fanout);

                //In the .NET client, when we supply no parameters to QueueDeclare() we create a
                ////non-durable, exclusive, autodelete queue with a generated name:
                var queueName = channel.QueueDeclare().QueueName;

                channel.QueueBind(queue: queueName,
                                  exchange: "logs",
                                  routingKey: "");

                Console.WriteLine(" [*] Waiting for logs.");

                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += Consumer_Received;

                channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static void Consumer_Received(object sender, BasicDeliverEventArgs basicDeliverEventArgs)
        {
            var body = basicDeliverEventArgs.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine(" [x] {0}", message);
        }
    }
}
