using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using CloudFabric.EventSourcing.Tests.CosmosDb;
using CloudFabric.EventSourcing.Tests.InMemory;
using CloudFabric.EventSourcing.Tests.Postgresql;

namespace CloudFabric.EventSourcing.Benchmarks;

[CsvMeasurementsExporter]
[HtmlExporter]
[PlainExporter]
[RPlotExporter]
[SimpleJob(launchCount: 1, warmupCount: 1, invocationCount: 100, id: "Benchmark")]
public class EventStoreBenchmarks
{
    private readonly OrderTestsCosmosDb _orderTestsCosmosDb = new OrderTestsCosmosDb();
    private readonly OrderTestsInMemory _orderTestsInMemory = new OrderTestsInMemory();
    private readonly OrderTestsPostgresql _orderTestsPostgresql = new OrderTestsPostgresql();

    public EventStoreBenchmarks()
    {
    }

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await _orderTestsCosmosDb.SetUp();
    }

    [Benchmark]
    public async Task CreateOrderInMemory()
    {
        await _orderTestsInMemory.TestPlaceOrderAndAddItem();
    }

    [Benchmark]
    public async Task CreateOrderCosmosDb()
    {
        await _orderTestsCosmosDb.TestPlaceOrderAndAddItem();
    }

    [Benchmark]
    public async Task CreateOrderPostgresql()
    {
        await _orderTestsPostgresql.TestPlaceOrderAndAddItem();
    }
}

public class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<EventStoreBenchmarks>();
    }
}