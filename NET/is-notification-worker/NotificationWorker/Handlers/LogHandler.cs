using NotificationWorker.Events;
namespace NotificationWorker.Handlers;

public class LogHandler(ILogger<LogHandler> logger)
{
    public void Handle(UserRegisteredEvent evt)
    {
        logger.LogInformation(
            "[LOG] Nuevo usuario registrado → Id: {UserId} | Email: {Email} | Fecha: {Date}",
            evt.UserId, evt.Email, evt.RegisteredAt);
    }
}