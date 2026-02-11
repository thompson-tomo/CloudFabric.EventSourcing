using System.Text;
using CloudFabric.EventSourcing.AspNet.Postgresql.Extensions;
using CloudFabric.EventSourcing.Domain;
using CloudFabric.EventSourcing.EventStore;
using CloudFabric.EventSourcing.EventStore.Postgresql;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using ToDoList.Api.Extensions;
using ToDoList.Api.Middleware;
using ToDoList.Domain;
using ToDoList.Domain.Projections.TaskLists;
using ToDoList.Domain.Projections.UserAccounts;
using ToDoList.Services.Implementations;
using ToDoList.Services.Interfaces;
using ToDoList.Services.Interfaces.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAuthentication(options =>
{
    options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
    options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
    options.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
}).AddJwtBearer(o =>
{
    o.TokenValidationParameters = new TokenValidationParameters
    {
        ValidIssuer = builder.Configuration["UserAccessTokensServiceOptions:Issuer"],
        ValidAudience = builder.Configuration["UserAccessTokensServiceOptions:Audience"],
        IssuerSigningKey = new SymmetricSecurityKey
            (Encoding.UTF8.GetBytes(builder.Configuration["UserAccessTokensServiceOptions:TokenSigningKey"])),
        ValidateIssuer = true,
        ValidateAudience = true,
        ValidateLifetime = true,
        ValidateIssuerSigningKey = true
    };
});

builder.Services.AddControllers(options => {
    options.Filters.Add<GlobalExceptionProblemDetailsFilter>();
});

//builder.Services.AddAutoMapper(typeof(UserAccountsService));

builder.Services.AddScoped<IUserAccountsService, UserAccountsService>();
builder.Services.AddScoped<IUserAccessTokensService, UserAccessTokensService>();
builder.Services.AddScoped<ITaskListsService, TaskListsService>();
builder.Services.AddUserInfoProvider();

builder.Services.Configure<UserAccessTokensServiceOptions>(builder.Configuration.GetSection("UserAccessTokensServiceOptions"));

#region User Accounts Projections

builder.Services.AddPostgresqlEventStore(builder.Configuration.GetConnectionString("Default"), "todolist-events", "todolist-metadata")
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

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

// DO not enable this since we want to run the app behind a gateway or a reverse proxy
// app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}"
);

# region Database init
var initScope = app.Services.CreateScope();
var eventStore = initScope.ServiceProvider.GetRequiredService<IEventStore>();
await eventStore.Initialize();
#endregion

app.Run();

class P : IPostgresqlEventStoreConnectionInformationProvider
{
    private readonly IServiceProvider _serviceProvider;
    public P(IServiceProvider sp)
    {
        _serviceProvider = sp;
    }

    EventStoreConnectionInformation IEventStoreConnectionInformationProvider.GetConnectionInformation(string? connectionId)
    {
        return GetConnectionInformation(connectionId);
    }

    public PostgresqlEventStoreConnectionInformation GetConnectionInformation(string? connectionId = null)
    {
        var httpContext = _serviceProvider.GetRequiredService<IHttpContextAccessor>();

        var tenantId = httpContext.HttpContext.User.Claims.First(c => c.Type == "TenantId");
        var configuration = _serviceProvider.GetRequiredService<IConfiguration>(); 

        return new PostgresqlEventStoreConnectionInformation()
        {
            ConnectionId = tenantId.ToString(),
            ConnectionString = configuration.GetConnectionString("Default"),
            TableName = $"event_store_tenant_{tenantId}"
        };
    }
}