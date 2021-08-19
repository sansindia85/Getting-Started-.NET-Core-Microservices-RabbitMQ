using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;
using System.Threading.Channels;

namespace Worker
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "task_queue",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //This tells RabbitMQ not to give more than one message to a worker at a time.
                //Or, in other words, don't dispatch a new message to a worker until it has
                //processed and acknowledged the previous one. Instead, it will dispatch it to
                //the next worker that is not still busy.
                channel.BasicQos(prefetchSize: 0,
                                 prefetchCount: 1,
                                 global: false);

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += Consumer_Received;

                channel.BasicConsume(queue: "task_queue",
                                     autoAck: false,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();       
            }         
        }

        private static void Consumer_Received(object sender, BasicDeliverEventArgs basicDeliverEventArgs)
        {
            var body = basicDeliverEventArgs.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine(" [x] Received {0}", message);

            int dots = message.Split('.').Length - 1;
            Thread.Sleep(dots * 1000);

            Console.WriteLine(" [x] Done");

            ((EventingBasicConsumer)sender).Model.BasicAck(deliveryTag: basicDeliverEventArgs.DeliveryTag,
                multiple: false);
       
        }
    }
}
