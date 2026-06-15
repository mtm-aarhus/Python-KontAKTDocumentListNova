"""This module handles resetting the state of the computer so the robot can work with a clean slate.

For this robot the "state" is the Nova bearer token and the cached OO
credentials. ``open_all`` opens them and returns a :class:`Client`; ``reset``
re-opens them, so the queue framework can reconnect on a retry instead of
re-validating the token for every queue element.
"""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from oomtm import nova as oomtm_nova


class Client:
    """Live Nova token + cached KontAKT credentials, opened by ``open_all`` and
    reused across every queue element (multiple cases share one token)."""

    def __init__(self, orchestrator_connection: OrchestratorConnection):
        self.nova_url = orchestrator_connection.get_constant("KMDNovaURL").value
        self.token = _get_kmd_token(orchestrator_connection)
        kontakt = orchestrator_connection.get_credential("KontAKTAPI")
        self.kontakt_base = kontakt.username
        self.kontakt_key = kontakt.password


def reset(orchestrator_connection: OrchestratorConnection) -> Client:
    """Clean up, close/kill all programs, then (re)open the connections.

    Returns the freshly-opened :class:`Client` so the queue framework can reuse
    it across queue elements (and reconnect by calling ``reset`` again)."""
    orchestrator_connection.log_trace("Resetting.")
    clean_up(orchestrator_connection)
    close_all(orchestrator_connection)
    kill_all(orchestrator_connection)
    return open_all(orchestrator_connection)


def clean_up(orchestrator_connection: OrchestratorConnection) -> None:
    """Do any cleanup needed to leave a blank slate."""
    orchestrator_connection.log_trace("Doing cleanup.")


def close_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Gracefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Closing all applications.")


def kill_all(orchestrator_connection: OrchestratorConnection) -> None:
    """Forcefully close all applications used by the robot."""
    orchestrator_connection.log_trace("Killing all applications.")


def open_all(orchestrator_connection: OrchestratorConnection) -> Client:
    """Open all connections used by the robot and return them as a :class:`Client`."""
    orchestrator_connection.log_trace("Opening Nova connection.")
    return Client(orchestrator_connection)


# ----- KMD token caching -----------------------------------------------------


def _get_kmd_token(orchestrator_connection: OrchestratorConnection) -> str:
    """Fetch a valid KMD bearer token, refreshing if older than 90 min.

    Thin wrapper around ``oomtm.nova.get_token``: pulls the cached values out
    of OO, calls the lib, and writes back only if the lib returned a refresh.
    """
    ts_const = orchestrator_connection.get_constant("KMDTokenTimestamp")
    token_cred = orchestrator_connection.get_credential("KMDAccessToken")
    client_cred = orchestrator_connection.get_credential("KMDClientSecret")

    result = oomtm_nova.get_token(
        current_token=token_cred.password,
        current_timestamp_str=ts_const.value,
        token_url=token_cred.username,
        client_id="aarhus_kommune",
        client_secret=client_cred.password,
    )

    if result.refreshed:
        orchestrator_connection.update_credential(
            "KMDAccessToken", token_cred.username, result.token
        )
        orchestrator_connection.update_constant("KMDTokenTimestamp", result.timestamp_str)

    return result.token
