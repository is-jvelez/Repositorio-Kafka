using NotificationWorker;
using NotificationWorker.Handlers;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

builder.Services.AddSingleton<LogHandler>();
builder.Services.AddSingleton<EmailHandler>();
builder.Services.AddSingleton<AuditHandler>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
