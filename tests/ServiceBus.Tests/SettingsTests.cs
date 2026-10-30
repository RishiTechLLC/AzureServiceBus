using ServiceBus.Shared.Configuration;

namespace ServiceBus.Tests;

public class SettingsTests
{
    [Fact]
    public void MissingCredentials_IsInvalid() =>
        Assert.Contains(new ServiceBusSettings().Validate(), e => e.Contains("ConnectionString"));

    [Theory]
    [InlineData("Endpoint=sb://x.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v", null)]
    [InlineData(null, "x.servicebus.windows.net")]
    public void EitherCredentialIsValid(string? connectionString, string? fqns) =>
        Assert.Empty(new ServiceBusSettings { ConnectionString = connectionString, FullyQualifiedNamespace = fqns }.Validate());

    [Fact]
    public void InvalidNumbers_AreReported()
    {
        var settings = new ServiceBusSettings { FullyQualifiedNamespace = "x" };
        settings.Processing.MaxConcurrentCalls = 0;
        settings.DeadLetter.MaxResubmitAttempts = 0;
        Assert.Equal(2, settings.Validate().Count);
    }
}
