"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement, QueueStatus

from robot_framework.process import process
from robot_framework import reset
import os
import json
from typing import Optional


def make_queue_element_with_payload(
    payload: dict | list,
    queue_name: str,
    reference: Optional[str] = None,
    created_by: Optional[str] = None,
    status: QueueStatus = QueueStatus.NEW, 
) -> QueueElement:
    # Validate & serialize
    data_str = json.dumps(payload, ensure_ascii=False)
    if len(data_str) > 2000:
        raise ValueError("data exceeds 2000 chars (column limit)")

    return QueueElement(
        queue_name=queue_name,
        status=status,
        data=data_str,
        reference=reference,
        created_by=created_by,
    )

# pylint: disable-next=unused-argum
orchestrator_connection = OrchestratorConnection(
    "KontAKTDocumentListNova",
    os.getenv("OpenOrchestratorSQL"),
    os.getenv("OpenOrchestratorKey"),
    None,
    None
)


qe = make_queue_element_with_payload(
    payload={
        "kontakt_case_id": 11,
        "kontakt_reference_id": 12,
        "source_case_id": "GEO-2024-000170",
        "source_case_title": "Test geosag [dato]"
    },
    queue_name="KontAKTDocumentListNova",
    reference="Sandbox",
    status=QueueStatus.NEW,
)

client = reset.reset(orchestrator_connection)

process(orchestrator_connection, qe, client)