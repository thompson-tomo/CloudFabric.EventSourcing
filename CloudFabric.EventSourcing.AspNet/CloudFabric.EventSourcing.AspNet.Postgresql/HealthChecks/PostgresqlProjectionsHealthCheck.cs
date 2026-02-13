using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace CloudFabric.EventSourcing.AspNet.Postgresql.HealthChecks;

public class PostgresqlProjectionsHealthCheck : IHealthCheck
{
    private readonly string _connectionString;

    public PostgresqlProjectionsHealthCheck(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            await cmd.ExecuteScalarAsync(cancellationToken);

            return HealthCheckResult.Healthy("PostgreSQL projections store is accessible.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                "PostgreSQL projections store is unreachable.", ex);
        }
    }
}
