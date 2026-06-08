namespace AuthService.Application.Events;

public record UserRegisteredEvent(
    Guid UserId,
    string Email,
    string Username,
    DateTime RegisteredAt
);