using CloudFabric.Projections.Postgresql;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudFabric.EventSourcing.Tests.Postgresql;

[TestClass]
public class DistributedLockTestsPostgresql
{
    private PostgresqlDistributedLock CreateLock()
    {
        return new PostgresqlDistributedLock(TestsConnectionStrings.CONNECTION_STRING);
    }

    [TestMethod]
    public async Task TryAcquire_SucceedsOnFirstAttempt()
    {
        var distributedLock = CreateLock();

        var handle = await distributedLock.TryAcquireAsync("test:lock:acquire");
        handle.Should().NotBeNull("should acquire lock on first attempt");

        await handle!.DisposeAsync();
    }

    [TestMethod]
    public async Task TryAcquire_AfterRelease_SucceedsAgain()
    {
        var distributedLock = CreateLock();

        var handle1 = await distributedLock.TryAcquireAsync("test:lock:reacquire");
        handle1.Should().NotBeNull();
        await handle1!.DisposeAsync();

        // After releasing, we should be able to acquire again
        var handle2 = await distributedLock.TryAcquireAsync("test:lock:reacquire");
        handle2.Should().NotBeNull("should be able to re-acquire after release");
        await handle2!.DisposeAsync();
    }

    [TestMethod]
    public async Task TryAcquire_ConcurrentSameKey_SecondFails()
    {
        var lock1 = CreateLock();
        var lock2 = CreateLock();

        var handle1 = await lock1.TryAcquireAsync("test:lock:contention");
        handle1.Should().NotBeNull();

        // Second attempt with same key should fail (lock is held)
        var handle2 = await lock2.TryAcquireAsync("test:lock:contention");
        handle2.Should().BeNull("lock is already held by another connection");

        // Release first lock
        await handle1!.DisposeAsync();

        // Now second should succeed
        var handle3 = await lock2.TryAcquireAsync("test:lock:contention");
        handle3.Should().NotBeNull("lock should be available after first holder released it");
        await handle3!.DisposeAsync();
    }

    [TestMethod]
    public async Task TryAcquire_DifferentKeys_DoNotConflict()
    {
        var distributedLock = CreateLock();

        var handleA = await distributedLock.TryAcquireAsync("test:lock:key-A");
        handleA.Should().NotBeNull();

        var handleB = await distributedLock.TryAcquireAsync("test:lock:key-B");
        handleB.Should().NotBeNull("different lock keys should not conflict");

        await handleA!.DisposeAsync();
        await handleB!.DisposeAsync();
    }
}
