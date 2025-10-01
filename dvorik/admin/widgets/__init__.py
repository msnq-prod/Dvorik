"""Widget registration stubs for the admin interface."""


def register_builtin_widgets() -> None:
    """Hook for registering built-in widgets.

    Ticket 4.1 only requires that the server imports this hook during
    application initialisation. Concrete widget implementations will be
    provided in subsequent tasks.
    """

    # No built-in widgets yet.
    return None
