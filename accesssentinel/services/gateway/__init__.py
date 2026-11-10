"""Proxy / Access-Control Gateway (D3 component 2).

Run:  uvicorn --factory accesssentinel.services.gateway.main:build --host 0.0.0.0 --port 8000
The in-process `accesssentinel/gateway.py` remains the simulator's model of the same component.
"""
