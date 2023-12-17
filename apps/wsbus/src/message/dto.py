from orwynn import DTO


class SubscribeSystemRMessageValue(DTO):
    """
    No need to have service code field, since it will be taken from the
    message's owner.
    """

    legacysendercodes: list[str] | None = None
    """
    Additional codes identifying the past of the message's sender.
    """

    roomids: str | None = None
    """
    Rooms the service is part of.
    """
