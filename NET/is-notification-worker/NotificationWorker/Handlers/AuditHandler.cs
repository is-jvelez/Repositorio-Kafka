using NotificationWorker.Events;
namespace NotificationWorker.Handlers;

public class AuditHandler(ILogger<AuditHandler> logger)
{
    public Task SaveAuditAsync(UserRegisteredEvent evt)
    {
        // Simulado: aquí conectarías tu SQL Server / PostgreSQL
        logger.LogInformation(
            "[AUDITORÍA] Registro en tabla audit_log → UserId: {UserId} | Acción: USER_REGISTERED | Timestamp: {Ts}",
            evt.UserId, DateTime.UtcNow);

        return Task.CompletedTask;
    }
}