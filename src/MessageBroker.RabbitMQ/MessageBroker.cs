using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Abstractions.MessageBroker;
using Abstractions.MessageBroker.Models;
using MessageBroker.RabbitMQ.Configuration;
using MessageBroker.RabbitMQ.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessageBroker.RabbitMQ
{
    public class MessageBroker : IMessageBus
    {
        /**
         * The internal dictionary that will hold all the message handlers for a specific message ID
         * _subscriptionDictionary[event_identifier] = handler1
         */
        private readonly IDictionary<string, object> _subscriptionDictionary;
        private readonly IServiceProvider _serviceProvider;
        
        private IConnection _rabbitMqConnection;
        private IModel _rabbitMqModel;
        private string _exchangeName;

        public MessageBroker(IOptions<RabbitMqConfiguration> rabbitMqConfigurationOptions, 
                             IServiceProvider serviceProvider)
        {
            if (rabbitMqConfigurationOptions == null) 
                throw new ArgumentNullException(
                    nameof(RabbitMqConfiguration), 
                    $"{nameof(RabbitMqConfiguration)} options object not correctly setup.");

            var rabbitMqConfiguration = rabbitMqConfigurationOptions.Value;
            _serviceProvider = serviceProvider;
            _subscriptionDictionary = new Dictionary<string, object>();
            
            InitializeRabbitMq(rabbitMqConfiguration);
            InitializeBroadcastExchange(rabbitMqConfiguration);
        }
        
        /// <inheritdoc cref="IPublisher.Publish"/>
        public void Publish(IMessage message)
        {
            var wrappingObject = new RabbitMqMessage
            {
                Id = message.GetType().Name,
                Data = message
            };
            var serializedEvent = JsonSerializer.Serialize(wrappingObject);
            var body = Encoding.UTF8.GetBytes(serializedEvent);
            _rabbitMqModel.BasicPublish(_exchangeName, "", null, body);
        }

        /// <inheritdoc cref="ISubscriber.Subscribe{TMessage,TMessageHandler}"/>
        public void Subscribe<TMessage, TMessageHandler>() 
            where TMessage : IMessage 
            where TMessageHandler : IMessageHandler<TMessage>
        {
            var service = _serviceProvider.GetRequiredService<TMessageHandler>();
            var typeName = typeof(TMessage).Name.ToLowerInvariant();
            _subscriptionDictionary[typeName] = service;
        }

        /// <inheritdoc cref="ISubscriber.Unsubscribe{TMessage,TMessageHandler}"/>
        public void Unsubscribe<TMessage, TMessageHandler>() 
            where TMessage : IMessage 
            where TMessageHandler : IMessageHandler<TMessage>
        {
            var typeName = typeof(TMessage).Name.ToLowerInvariant();
            _subscriptionDictionary.Remove(typeName);
        }

        // private helper methods
        #region PRIVATE METHODS
        private void InitializeRabbitMq(RabbitMqConfiguration rabbitMqConfiguration)
        {
            var connection = new ConnectionFactory
            {
                HostName = string.IsNullOrEmpty(rabbitMqConfiguration.Hostname)
                    ? throw new ArgumentException(
                        $"{nameof(RabbitMqConfiguration)} hostname cannot be null or empty.",
                        nameof(RabbitMqConfiguration))
                    : rabbitMqConfiguration.Hostname,
                UserName = rabbitMqConfiguration.Username,
                Password = rabbitMqConfiguration.Password
            };

            _rabbitMqConnection = connection.CreateConnection();
            _rabbitMqModel = _rabbitMqConnection.CreateModel();
        }
        
        private void InitializeBroadcastExchange(RabbitMqConfiguration rabbitMqConfiguration)
        {
            _exchangeName = string.IsNullOrEmpty(rabbitMqConfiguration.ExchangeName)
                ? throw new ArgumentException(
                    $"{nameof(RabbitMqConfiguration)} exchange name string cannot be null or empty.",
                    nameof(RabbitMqConfiguration))
                : rabbitMqConfiguration.ExchangeName;

            // declare the exchange
            _rabbitMqModel.ExchangeDeclare(_exchangeName, ExchangeType.Fanout);

            // declare the temporary queue for this service and bind it to the exchange
            var queueName = _rabbitMqModel.QueueDeclare().QueueName;
            _rabbitMqModel.QueueBind(queueName, rabbitMqConfiguration.ExchangeName, string.Empty);

            var consumerEventingService = new EventingBasicConsumer(_rabbitMqModel);
            consumerEventingService.Received += async (model, ea) =>
            {
                var jsonWrappingObject = Encoding.UTF8.GetString(ea.Body.ToArray());
                var wrappingObject = JsonSerializer.Deserialize<RabbitMqMessage>(jsonWrappingObject);
                
                var messageId = wrappingObject.Id;
                _subscriptionDictionary.TryGetValue(messageId, out var rawHandler);
                if (rawHandler == null) return;
                
                var handler = (IMessageHandler<IMessage>) rawHandler;
                await handler.HandleAsync(wrappingObject.Data);
            };
        }
        #endregion
        
        // implementation of the dispose pattern
        #region DISPOSE PATTERN
        private bool _disposed;
        
        /**
         * Consumer dispose method
         */
        public void Dispose()
        {
            Dispose(true);
        }
        
        /**
         * How we dispose an object
         */
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                _subscriptionDictionary?.Clear();
                _rabbitMqModel?.Dispose();
                _rabbitMqConnection?.Dispose();
            }
            _disposed = true;
        }
        #endregion
    }
}