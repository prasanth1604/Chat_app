
import asyncio
import json
import sys
import time

import websockets

SERVER_WS = "ws://localhost:8000/ws"


async def receiver(ws):
    try:
        async for message in ws:
            try:
                obj = json.loads(message)
                t = obj.get("type")
                if t == "message":
                    print(f"[MSG] {obj['timestamp']} | {obj['username']}: {obj['message']}")
                elif t == "ack":
                    print(f"[ACK] delivered at {obj.get('timestamp')}")
                elif t == "topic_list":
                    print("--- Topics (server) ---")
                    print(obj.get("data"))
                    print("----------------------")
                elif obj.get("error"):
                    print(f"[ERROR] {obj['error']}")
                else:
                    print("[INFO]", obj)
            except Exception as e:
                print("[RECV_PARSE_ERR]", message, e)
    except websockets.exceptions.ConnectionClosed:
        print("Connection closed")


async def sender(ws):
    # read from stdin and send
    loop = asyncio.get_event_loop()
    while True:
        text = await loop.run_in_executor(None, sys.stdin.readline)
        if not text:
            break
        text = text.rstrip("\n")
        if text.strip() == "/quit":
            await ws.close()
            return
        if text.strip() == "/list":
            # send plain text /list
            await ws.send("/list")
            continue
        # otherwise send JSON message
        payload = {"message": text}
        await ws.send(json.dumps(payload))


async def main():
    username = input("username: ").strip()
    topic = input("topic: ").strip()
    if not username or not topic:
        print("username and topic required")
        return

    async with websockets.connect(SERVER_WS) as ws:
        # send initial payload
        await ws.send(json.dumps({"username": username, "topic": topic}))

        # start receiver and sender
        recv_task = asyncio.create_task(receiver(ws))
        send_task = asyncio.create_task(sender(ws))

        done, pending = await asyncio.wait([recv_task, send_task], return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Client exiting")