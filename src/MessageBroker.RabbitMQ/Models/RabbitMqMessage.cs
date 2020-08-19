using Abstractions.MessageBroker.Models;

namespace MessageBroker.RabbitMQ.Models
{
    public class RabbitMqMessage
    {
        public string Id { get; set; }
        public IMessage Data { get; set; }
    }
}