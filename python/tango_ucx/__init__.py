# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

class Failed(Exception):
    """Raised when a subscription ends before delivering all entitled frames."""

    def __init__(self, message, *, last_index):
        super().__init__(message)
        self.last_index = last_index


from .subscription import Batch, GpuView, Subscription, every, latest, pull

__all__ = ["Batch", "Failed", "GpuView", "Subscription", "every", "latest", "pull"]
