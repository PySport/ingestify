from dataclasses import dataclass
from typing import Protocol

from ingestify.domain import DatasetCreated


#
# class EventRepository:
#     def __init__(self):
#         self.events = []
#
#     def save(self, event):
#         self.events.append(event)
#
#
# class EventWriter:
#     def __init__(self, event_repository: EventRepository):
#         self.event_repository = event_repository
#
#     def dispatch(self, event):
#         self.event_repository.save(event)
