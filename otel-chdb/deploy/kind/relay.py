#!/usr/bin/env python3
"""TCP relay: LISTEN_HOST:PORT -> 127.0.0.1:PORT, so pods in a kind cluster
reach a host service bound to loopback (SeaweedFS's S3 on :18333) through
the kind network's gateway address. SIGUSR1 toggles "down" (new connections
are refused and live ones cut: an S3 outage for the publishers); SIGUSR2
toggles "slow" (each chunk is delayed 2 s).

  relay.py 172.18.0.1 18333
"""
import asyncio, signal, sys

host, port = sys.argv[1], int(sys.argv[2])
target = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
state = {"down": False, "slow": False}
live = set()


async def pipe(r, w):
    try:
        while True:
            b = await r.read(65536)
            if not b:
                break
            if state["slow"]:
                await asyncio.sleep(2)
            w.write(b)
            await w.drain()
    except Exception:
        pass
    finally:
        try:
            w.close()
        except Exception:
            pass


async def handle(cr, cw):
    if state["down"]:
        cw.close()
        return
    try:
        ur, uw = await asyncio.open_connection(target, port)
    except Exception:
        cw.close()
        return
    live.add(cw)
    live.add(uw)
    await asyncio.gather(pipe(cr, uw), pipe(ur, cw))
    live.discard(cw)
    live.discard(uw)


def toggle(k):
    state[k] = not state[k]
    print(f"{k} -> {state[k]}", flush=True)
    if k == "down" and state[k]:
        for w in list(live):
            try:
                w.transport.abort()
            except Exception:
                pass


async def main():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, toggle, "down")
    loop.add_signal_handler(signal.SIGUSR2, toggle, "slow")
    srv = await asyncio.start_server(handle, host, port)
    print(f"relay {host}:{port} -> {target}:{port}", flush=True)
    async with srv:
        await srv.serve_forever()

asyncio.run(main())
