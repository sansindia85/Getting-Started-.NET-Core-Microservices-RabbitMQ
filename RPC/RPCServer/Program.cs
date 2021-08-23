using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RPCServer
{
    class Program
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
                channel.QueueDeclare(queue: "rpc_queue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.BasicQos(0, 1, false);

                var consumer = new EventingBasicConsumer(channel);

                channel.BasicConsume(queue: "rpc_queue",
                                     autoAck: false, 
                                     consumer: consumer);

                Console.WriteLine(" [x] Awaiting RPC requets");

                consumer.Received += (model, ea) =>
                {
                    string response = null;

                    var body = ea.Body.ToArray();
                    var properties = ea.BasicProperties;
                    var replyProperties = channel.CreateBasicProperties();
                    replyProperties.CorrelationId = properties.CorrelationId;


                    try
                    {
                        var message = Encoding.UTF8.GetString(body);
                        int number = int.Parse(message);
                        Console.WriteLine(" [.] fib({0}", message);
                        response = Fibonacci(number).ToString();
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine(" [.] " + exception.Message);
                        response = "";
                    }
                    finally
                    {
                        var responseBytes = Encoding.UTF8.GetBytes(response);

                        channel.BasicPublish(exchange: "",
                                             routingKey: properties.ReplyTo,
                                             basicProperties: replyProperties,
                                             body: responseBytes);

                        channel.BasicAck(deliveryTag: ea.DeliveryTag,
                            multiple: false);

                    }
                };

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }         
        }
        
        /// Assumes only valid positive integer input.
        /// Don't expect this one to work for big numbers, and it's
        /// probably the slowest recursive implementation possible.        
        private static int Fibonacci(int number)
        {
            if (number == 0 || number == 1)
                return number;

            return Fibonacci(number - 1) + Fibonacci(number - 2);

        }
    }
}
