#!/usr/bin/env python3
"""bridge — TCP <> WS relay."""
"""

Écoute sur 127.0.0.1:<port> et relaie chaque connexion vers un endpoint
WebSocket (variable RELAY_WS) qui pipe vers sa cible fixe. Utilisé pour
faire transiter un flux TLS de bout en bout par un relais applicatif.

Usage : python3 ws_bridge.py <port>
"""

import asyncio
import os
import sys

from websockets.asyncio.client import connect as ws_connect


async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9051
    uri = os.environ["RELAY_WS"]
    proxy = os.environ.get("RELAY_PROXY") or None  # ex: socks5h://127.0.0.1:9050

    async def handler(reader, writer):
        try:
            ws = await ws_connect(uri, proxy=proxy, max_size=2**22)
        except Exception as e:  # noqa: BLE001
            print(f"ws KO: {e}", file=sys.stderr)
            writer.close()
            return

        async def c2w():
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await ws.send(data)

        async def w2c():
            async for msg in ws:
                writer.write(msg)
                await writer.drain()

        try:
            await asyncio.gather(c2w(), w2c())
        except Exception:  # noqa: BLE001
            pass
        finally:
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
            writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
