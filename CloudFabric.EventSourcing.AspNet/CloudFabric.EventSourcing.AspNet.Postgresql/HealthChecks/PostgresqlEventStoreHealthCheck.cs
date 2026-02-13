using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace CloudFabric.EventSourcing.AspNet.Postgresql.HealthChecks;

public class PostgresqlEventStoreHealthCheck : IHealthCheck
{
    private readonly string _connectionString;
    private readonly string _tableName;

    public PostgresqlEventStoreHealthCheck(string connectionString, string tableName)
    {
        _connectionString = connectionString;
        _tableName = tableName;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(
                $"SELECT 1 FROM \"{_tableName}\" LIMIT 1", conn);
            await cmd.ExecuteScalarAsync(cancellationToken);

            return HealthCheckResult.Healthy("PostgreSQL event store is accessible.");
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01") // UndefinedTable
        {
            return HealthCheckResult.Degraded(
                $"Event store table '{_tableName}' does not exist. Call Initialize() first.",
                ex);
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                "PostgreSQL event store is unreachable.", ex);
        }
    }
}
