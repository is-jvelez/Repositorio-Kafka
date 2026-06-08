namespace NotificationWorker.Events;
public record UserRegisteredEvent(
    Guid UserId,
    string Email,
    string Username,
    DateTime RegisteredAt
);