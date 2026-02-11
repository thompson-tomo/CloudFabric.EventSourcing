using System.Diagnostics;
using CloudFabric.EventSourcing.AspNet.Postgresql.Extensions;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using ToDoList.Domain;
using ToDoList.Domain.Projections.TaskLists;
using ToDoList.Domain.Projections.UserAccounts;
using ToDoList.Services.Implementations;
using ToDoList.Services.Interfaces;
using ToDoList.Web.Extensions;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

ConfigureOpenTelemetry(builder);

// Add services to the container.
builder.Services.AddControllersWithViews().AddRazorRuntimeCompilation();

builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie();

builder.Services.AddAuthorization();


#region EventSourcing

var eventSourcingBuilder = builder.Services
    .AddPostgresqlEventStore(builder.Configuration.GetConnectionString("Default"), "todolist-events", "todolist-metadata")
    
    .AddRepository<AggregateRepository<UserAccount>>()
    .AddRepository<AggregateRepository<UserAccountEmailAddress>>()

    .AddRepository<AggregateRepository<ToDoList.Domain.Task>>()
    .AddRepository<AggregateRepository<TaskList>>()

    .AddPostgresqlProjections(
        builder.Configuration.GetConnectionString("Default"),
        true,
        (factory, selector) => new UserAccountsProjectionBuilder(factory, selector),
        (factory, selector) => new TasksProjectionBuilder(factory, selector),
        (factory, selector) => new TaskListsProjectionBuilder(factory, selector)
    )
    .AddProjectionsRebuildProcessor();

#endregion


builder.Services.AddAutoMapper(typeof(UserAccountsService));

builder.Services.AddScoped<IUserAccountsService, UserAccountsService>();
builder.Services.AddScoped<IUserAccessTokensService, UserAccessTokensService>();
builder.Services.AddScoped<ITaskListsService, TaskListsService>();
builder.Services.AddUserInfoProvider();


builder.Services.AddHttpContextAccessor();


var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}"
);


# region Database init

await eventSourcingBuilder.InitializeEventStore(app.Services);

#endregion

#if DEBUG
File.WriteAllText("browsersync-update.txt", DateTime.Now.ToString());
var npmProcess = new Process
{
    StartInfo = new ProcessStartInfo
    {
        FileName = "npm",
        Arguments = "run-script watch",
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true
    }
};

var sp = builder.Services.BuildServiceProvider();
var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("gulp");

npmProcess.OutputDataReceived += (sender, e) => {
    if (!string.IsNullOrEmpty(e.Data))
    {
        logger.LogInformation(e.Data.Trim());
    }
};

npmProcess.ErrorDataReceived += (sender, e) => {
    if (!string.IsNullOrEmpty(e.Data))
    {
        logger.LogError(e.Data.Trim());
    }
};

npmProcess.Start();
npmProcess.BeginOutputReadLine();
npmProcess.BeginErrorReadLine();
// Ensure the npm process is terminated when the application stops
AppDomain.CurrentDomain.ProcessExit += (s, e) => npmProcess.Kill();
#endif

app.Run();

static IHostApplicationBuilder ConfigureOpenTelemetry(IHostApplicationBuilder builder)
{
    ActivitySource activitySource = new ActivitySource("CloudFabric.EventSourcing.ToDoList", "1.0.0");

    builder.Services.AddSingleton(sp => activitySource);

    builder.Logging.AddOpenTelemetry(logging =>
    {
        logging.IncludeFormattedMessage = true;
        logging.IncludeScopes = true;
    });

    builder.Services.AddOpenTelemetry()
        .ConfigureResource(c => c.AddService("CloudFabric.EventSourcing.ToDoList"))
        .WithMetrics(metrics =>
        {
            metrics.AddHttpClientInstrumentation()
                .AddRuntimeInstrumentation();
        })
        .WithTracing(tracing =>
        {
            tracing.AddHttpClientInstrumentation();
        });

    // Use the OTLP exporter if the endpoint is configured.
    var useOtlpExporter = !string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]);
    if (useOtlpExporter)
    {
        builder.Services.AddOpenTelemetry().UseOtlpExporter();
    }

    return builder;
}