using System.Globalization;

namespace ServiceBus.Receiver.Processing;

/// <summary>
/// Deferred messages can only be retrieved by sequence number, so they are persisted to a
/// small local file per entity. In production use a database or cache instead.
/// </summary>
public sealed class DeferredMessageStore
{
    private readonly Lock _sync = new();

    private static string PathFor(string entity) =>
        Path.Combine(AppContext.BaseDirectory, $"deferred-{entity.Replace('/', '_')}.txt");

    public void Add(string entity, long sequenceNumber)
    {
        lock (_sync)
        {
            File.AppendAllLines(PathFor(entity), [sequenceNumber.ToString(CultureInfo.InvariantCulture)]);
        }
    }

    public IReadOnlyList<long> GetAll(string entity)
    {
        lock (_sync)
        {
            var path = PathFor(entity);
            return File.Exists(path)
                ? File.ReadAllLines(path).Select(l => long.TryParse(l, CultureInfo.InvariantCulture, out var n) ? n : -1).Where(n => n >= 0).Distinct().ToList()
                : [];
        }
    }

    public void Remove(string entity, IEnumerable<long> sequenceNumbers)
    {
        lock (_sync)
        {
            var remaining = GetAll(entity).Except(sequenceNumbers).Select(n => n.ToString(CultureInfo.InvariantCulture));
            File.WriteAllLines(PathFor(entity), remaining);
        }
    }
}
