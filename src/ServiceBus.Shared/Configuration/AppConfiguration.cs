using System.Reflection;
using Microsoft.Extensions.Configuration;

namespace ServiceBus.Shared.Configuration;

public static class AppConfiguration
{
    /// <summary>
    /// Loads settings from (lowest to highest precedence): appsettings.json,
    /// appsettings.{DOTNET_ENVIRONMENT}.json, user secrets, environment variables
    /// (e.g. ServiceBus__ConnectionString) and command-line switches
    /// (e.g. --ServiceBus:QueueName=orders).
    /// </summary>
    public static ServiceBusSettings Load(string[] args, Assembly userSecretsAssembly)
    {
        var environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production";

        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true)
            .AddJsonFile($"appsettings.{environment}.json", optional: true)
            .AddUserSecrets(userSecretsAssembly, optional: true)
            .AddEnvironmentVariables()
            .AddCommandLine(args.Where(a => a.StartsWith("--", StringComparison.Ordinal)).ToArray())
            .Build();

        var settings = configuration.GetSection(ServiceBusSettings.SectionName).Get<ServiceBusSettings>() ?? new ServiceBusSettings();

        var errors = settings.Validate();
        if (errors.Count > 0)
        {
            throw new InvalidOperationException("Invalid configuration:" + Environment.NewLine + string.Join(Environment.NewLine, errors.Select(e => " - " + e)));
        }

        return settings;
    }
}
