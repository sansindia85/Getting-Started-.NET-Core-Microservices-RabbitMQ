using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RPCClient
{
    public class RpcClient
    {
        private readonly IConnection Connection;
        private readonly IModel Channel;
        private readonly string ReplyQueueName;
        private readonly EventingBasicConsumer Consumer;
        private readonly BlockingCollection<string> ResponseQueue =
            new BlockingCollection<string>();
        private readonly IBasicProperties Properties;

       public RpcClient()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };

            Connection = factory.CreateConnection();
            Channel = Connection.CreateModel();
            ReplyQueueName = Channel.QueueDeclare().QueueName;
            Consumer = new EventingBasicConsumer(Channel);

            Properties = Channel.CreateBasicProperties();
            var correlationId = Guid.NewGuid().ToString();
            Properties.CorrelationId = correlationId;
            Properties.ReplyTo = ReplyQueueName;

            Consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var response = Encoding.UTF8.GetString(body);

                if (ea.BasicProperties.CorrelationId == correlationId)
                {
                    ResponseQueue.Add(response);
                }
            };

            Channel.BasicConsume(consumer: Consumer,
                                 queue: ReplyQueueName,
                                 autoAck: true);
        }
        
        public string Call(string message)
        {
            var messageBytes = Encoding.UTF8.GetBytes(message);

            Channel.BasicPublish(
                exchange: "",
                routingKey: "rpc_queue",
                basicProperties: Properties,
                body: messageBytes);

            return ResponseQueue.Take();
        }

        public void Close()
        {
            Connection.Close();
        }
    }
}
