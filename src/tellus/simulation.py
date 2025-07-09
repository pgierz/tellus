#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Simulation objects for Earth System Models"""

from .location import Location, create_location_handler


class Simulation:
    """A Earth System Model Simulation"""

    def __init__(self, path):
        self.path = path
        self.attrs = {}
        self.data = None
        self.namelists = {}
        self.locations: dict[str, dict[str, object]] = {}

    def set_location(self, location: Location):
        handler = create_location_handler(location)
        self.locations[location.name] = {"location": location, "handler": handler}

    def get_location(self, name: str) -> Location | None:
        entry = self.locations.get(name)
        return entry["location"] if entry else None

    def post_to_location(self, name: str, data):
        entry = self.locations.get(name)
        if not entry:
            raise ValueError(f"Location {name} not set.")
        entry["handler"].post(data)

    def fetch_from_location(self, name: str, identifier):
        entry = self.locations.get(name)
        if not entry:
            raise ValueError(f"Location {name} not set.")
        return entry["handler"].get(identifier)

    def list_locations(self):
        return list(self.locations.keys())

    def remove_location(self, name: str):
        if name in self.locations:
            del self.locations[name]
