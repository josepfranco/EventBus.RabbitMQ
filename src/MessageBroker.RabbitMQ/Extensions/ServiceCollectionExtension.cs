using Abstractions.MessageBroker;
using Abstractions.MessageBroker.Models;
using MessageBroker.RabbitMQ.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MessageBroker.RabbitMQ.Extensions
{
    /// <summary>
    /// 
    /// </summary>
    public static class ServiceCollectionExtension
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public static IServiceCollection AddRabbitMqMessageBroker(this IServiceCollection services, 
                                                                  IConfiguration configuration)
        {
            services.Configure<RabbitMqConfiguration>(configuration.GetSection(nameof(RabbitMqConfiguration)));
            RegisterMessageBroker(services);
            return services;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="services"></param>
        /// <typeparam name="TMessage"></typeparam>
        /// <typeparam name="TMessageHandler"></typeparam>
        /// <returns></returns>
        public static IServiceCollection AddMessageHandler<TMessage, TMessageHandler>(this IServiceCollection services)
            where TMessage : IMessage
            where TMessageHandler : class, IMessageHandler<TMessage>
        {
            // register message handler in case it doesn't exist yet
            RegisterMessageBroker(services);
            
            var provider = services.BuildServiceProvider();
            services.TryAddSingleton<IMessageHandler<TMessage>, TMessageHandler>();
            provider.GetRequiredService<IMessageBus>()?
                .Subscribe<TMessage, TMessageHandler>();
            return services;
        }
        
        private static void RegisterMessageBroker(IServiceCollection services)
        {
            services.TryAddSingleton(ServiceDescriptor.Singleton<IMessageBus, MessageBroker>());
            services.TryAddSingleton(ServiceDescriptor.Singleton<ISubscriber, MessageBroker>());
            services.TryAddSingleton(ServiceDescriptor.Singleton<IPublisher, MessageBroker>());
        }
    }
}