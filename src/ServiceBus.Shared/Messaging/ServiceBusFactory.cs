using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using ServiceBus.Shared.Configuration;

namespace ServiceBus.Shared.Messaging;

/// <summary>Creates Service Bus clients from settings, using SAS or Microsoft Entra ID.</summary>
public static class ServiceBusFactory
{
    public static ServiceBusClient CreateClient(ServiceBusSettings settings)
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = settings.UseWebSockets ? ServiceBusTransportType.AmqpWebSockets : ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions
            {
                Mode = ServiceBusRetryMode.Exponential,
                MaxRetries = settings.Retry.MaxRetries,
                Delay = settings.Retry.Delay,
                MaxDelay = settings.Retry.MaxDelay,
                TryTimeout = settings.Retry.TryTimeout,
            },
            Identifier = $"{AppDomain.CurrentDomain.FriendlyName}-{Environment.MachineName}",
        };

        return settings.UsesConnectionString
            ? new ServiceBusClient(settings.ConnectionString, options)
            : new ServiceBusClient(settings.FullyQualifiedNamespace, new DefaultAzureCredential(), options);
    }

    public static ServiceBusAdministrationClient CreateAdministrationClient(ServiceBusSettings settings) =>
        settings.UsesConnectionString
            ? new ServiceBusAdministrationClient(settings.ConnectionString)
            : new ServiceBusAdministrationClient(settings.FullyQualifiedNamespace, new DefaultAzureCredential());
}
