from orwynn import DTO


class SubscribeSystemRMessageValue(DTO):
    """
    No need to have service code field, since it will be taken from the
    message's owner.
    """

    roomids: str | None = None
    """
    Rooms the service is part of.
    """
