using CloudFabric.EventSourcing.EventStore;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests;

public abstract class SequenceGeneratorTests
{
    protected abstract Task<ISequenceGenerator> GetSequenceGenerator();
    private ISequenceGenerator _sequenceGenerator;

    [TestInitialize]
    public async Task Initialize()
    {
        _sequenceGenerator = await GetSequenceGenerator();
    }

    [TestCleanup]
    public async Task Cleanup()
    {
        await _sequenceGenerator.DeleteAll();
    }

    [TestMethod]
    public async Task GetNextValue_FirstCall_ReturnsStartingNumber()
    {
        var value = await _sequenceGenerator.GetNextValue("test-sequence", "partition1");
        value.Should().Be(1);
    }

    [TestMethod]
    public async Task GetNextValue_SecondCall_ReturnsIncremented()
    {
        await _sequenceGenerator.GetNextValue("test-sequence", "partition1");
        var value = await _sequenceGenerator.GetNextValue("test-sequence", "partition1");
        value.Should().Be(2);
    }

    [TestMethod]
    public async Task GetNextValue_CustomStartingNumber()
    {
        var value = await _sequenceGenerator.GetNextValue("test-sequence", "partition1", startingNumber: 100);
        value.Should().Be(100);
    }

    [TestMethod]
    public async Task GetNextValue_CustomIncrement()
    {
        var first = await _sequenceGenerator.GetNextValue("test-sequence", "partition1", increment: 5);
        first.Should().Be(1);

        var second = await _sequenceGenerator.GetNextValue("test-sequence", "partition1", increment: 5);
        second.Should().Be(6);

        var third = await _sequenceGenerator.GetNextValue("test-sequence", "partition1", increment: 5);
        third.Should().Be(11);
    }

    [TestMethod]
    public async Task GetNextValue_MultipleSequences_Independent()
    {
        var a1 = await _sequenceGenerator.GetNextValue("sequence-a", "partition1");
        var b1 = await _sequenceGenerator.GetNextValue("sequence-b", "partition1");
        var a2 = await _sequenceGenerator.GetNextValue("sequence-a", "partition1");
        var b2 = await _sequenceGenerator.GetNextValue("sequence-b", "partition1");

        a1.Should().Be(1);
        b1.Should().Be(1);
        a2.Should().Be(2);
        b2.Should().Be(2);
    }

    [TestMethod]
    public async Task GetNextValue_DifferentPartitionKeys_Independent()
    {
        var p1v1 = await _sequenceGenerator.GetNextValue("test-sequence", "tenant-a");
        var p2v1 = await _sequenceGenerator.GetNextValue("test-sequence", "tenant-b");
        var p1v2 = await _sequenceGenerator.GetNextValue("test-sequence", "tenant-a");
        var p2v2 = await _sequenceGenerator.GetNextValue("test-sequence", "tenant-b");

        p1v1.Should().Be(1);
        p2v1.Should().Be(1);
        p1v2.Should().Be(2);
        p2v2.Should().Be(2);
    }

    [TestMethod]
    public async Task GetNextValue_ConcurrentCalls_NoDuplicates()
    {
        const int concurrency = 50;
        var tasks = Enumerable.Range(0, concurrency)
            .Select(_ => _sequenceGenerator.GetNextValue("concurrent-sequence", "partition1"))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().OnlyHaveUniqueItems();
        results.Should().HaveCount(concurrency);
        results.Min().Should().Be(1);
        results.Max().Should().Be(concurrency);
    }

    [TestMethod]
    public async Task DeleteAll_ResetsSequences()
    {
        await _sequenceGenerator.GetNextValue("test-sequence", "partition1");
        await _sequenceGenerator.GetNextValue("test-sequence", "partition1");

        await _sequenceGenerator.DeleteAll();

        var value = await _sequenceGenerator.GetNextValue("test-sequence", "partition1");
        value.Should().Be(1);
    }
}
