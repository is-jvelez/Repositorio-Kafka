using NotificationWorker.Events;
namespace NotificationWorker.Handlers;

public class EmailHandler(ILogger<EmailHandler> logger)
{
    public Task SendWelcomeEmailAsync(UserRegisteredEvent evt)
    {
        // Simulado: en producción aquí va SendGrid, SMTP, etc.
        logger.LogInformation(
            "[EMAIL] Enviando bienvenida a {Email} → Asunto: '¡Bienvenido {Username}!'",
            evt.Email, evt.Username);

        return Task.CompletedTask;
    }
}