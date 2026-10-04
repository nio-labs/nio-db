"""
nio-db-py — Lightweight Python SDK for NioDB (The Agentic Database)
"""

from .client import NioDB, NioDBError, Collection, Session, Sessions, Tasks

__all__ = ["NioDB", "NioDBError", "Collection", "Session", "Sessions", "Tasks"]
__version__ = "0.1.1"

