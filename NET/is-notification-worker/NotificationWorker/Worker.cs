using Confluent.Kafka;
using System.Text.Json;
using NotificationWorker.Events;
using NotificationWorker.Handlers;
namespace NotificationWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;
    private readonly LogHandler _logHandler;
    private readonly EmailHandler _emailHandler;
    private readonly AuditHandler _auditHandler;

    public Worker(
        ILogger<Worker> logger,
        IConfiguration config,
        LogHandler logHandler,
        EmailHandler emailHandler,
        AuditHandler auditHandler)
    {
        _logger = logger;
        _config = config;
        _logHandler = logHandler;
        _emailHandler = emailHandler;
        _auditHandler = auditHandler;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _config["Kafka:BootstrapServers"],
            GroupId = "notification-group",       // consumer group
            AutoOffsetReset = AutoOffsetReset.Earliest,  // desde el inicio si no hay offset
            EnableAutoCommit = false              // commit manual = control total
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) =>
                _logger.LogError("Error Kafka consumer: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_config["Kafka:Topic"]);
        _logger.LogInformation("NotificationWorker suscrito al topic: {Topic}", _config["Kafka:Topic"]);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);

                if (consumeResult?.Message?.Value is null) continue;

                _logger.LogInformation(
                    "Mensaje recibido → Partición: {P} | Offset: {O}",
                    consumeResult.Partition.Value, consumeResult.Offset.Value);

                var evt = JsonSerializer.Deserialize<UserRegisteredEvent>(consumeResult.Message.Value);

                if (evt is not null)
                {
                    _logHandler.Handle(evt);
                    await _emailHandler.SendWelcomeEmailAsync(evt);
                    await _auditHandler.SaveAuditAsync(evt);
                }

                // Commit manual: solo confirma offset si procesó bien
                consumer.Commit(consumeResult);
            }
            catch (OperationCanceledException)
            {
                break; // shutdown limpio
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error procesando mensaje Kafka");
                await Task.Delay(1000, stoppingToken); // backoff simple
            }
        }

        consumer.Close();
        _logger.LogInformation("NotificationWorker detenido limpiamente");
    }
}
