
import asyncio
import json
import logging
import time
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

# In-memory state
# topics: topic_name -> dict(username -> WebSocket)
topics: Dict[str, Dict[str, WebSocket]] = {}
# topic_messages: topic_name -> list of message dicts (kept for up to 30s)
topic_messages: Dict[str, List[dict]] = {}
# global lock to protect shared state
state_lock = asyncio.Lock()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chat_server")

MESSAGE_TTL_SECONDS = 30


async def make_unique_username(topic: str, desired: str) -> str:
    """Return a unique username in the topic by appending #N when needed."""
    async with state_lock:
        users = topics.get(topic, {})
        if desired not in users:
            return desired
        # find highest suffix
        n = 2
        while f"{desired}#{n}" in users:
            n += 1
        return f"{desired}#{n}"


async def broadcast_to_topic(topic: str, payload: dict, exclude_username: str = None):
    """Broadcast a JSON-serializable payload to all users in a topic except exclude_username."""
    async with state_lock:
        users = topics.get(topic, {}).items()
        websockets = [(u, ws) for u, ws in users if u != exclude_username]
    # send without holding lock
    text = json.dumps(payload)
    for username, ws in websockets:
        try:
            await ws.send_text(text)
        except Exception as e:
            logger.error("Error sending to %s in topic %s: %s", username, topic, e)


async def schedule_message_expiry(topic: str, message: dict):
    """Remove message from in-memory list after MESSAGE_TTL_SECONDS."""
    await asyncio.sleep(MESSAGE_TTL_SECONDS)
    async with state_lock:
        msgs = topic_messages.get(topic)
        if not msgs:
            return
        # remove the specific message object if present
        try:
            msgs.remove(message)
            logger.debug("Expired message removed from topic %s: %s", topic, message)
        except ValueError:
            # already removed
            pass


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    username = None
    topic = None
    try:
        # Expect the very first message to be the connection payload (JSON)
        init_text = await websocket.receive_text()
        try:
            data = json.loads(init_text)
            username_raw = data.get("username")
            topic = data.get("topic")
            if not username_raw or not topic:
                await websocket.send_text(json.dumps({"error": "username and topic required"}))
                await websocket.close()
                return
        except json.JSONDecodeError:
            await websocket.send_text(json.dumps({"error": "invalid JSON in connect payload"}))
            await websocket.close()
            return

        # Make username unique within topic
        username = await make_unique_username(topic, username_raw)

        # Add to topic
        async with state_lock:
            if topic not in topics:
                topics[topic] = {}
                topic_messages[topic] = []
            topics[topic][username] = websocket

        logger.info("User connected: %s in topic %s", username, topic)

        # Notify (optional) — we won't broadcast join messages to meet requirements, but could be added.

        # Listen loop
        while True:
            try:
                text = await websocket.receive_text()
            except WebSocketDisconnect:
                raise
            except Exception as e:
                # fatal receive error for this socket
                logger.error("Receive error from %s: %s", username, e)
                break

            # Handle commands or messages
            # Try parse as JSON message payload. If invalid, send an error but continue.
            # Special case: /list command as plain text string
            if text.strip() == "/list":
                # respond only to this user with active topics and counts
                async with state_lock:
                    snapshot = {t: len(u) for t, u in topics.items()}
                lines = ["Active Topics:"] + [f"{t} ({c} user{"s" if c==1 else "s"})" for t, c in snapshot.items()]
                # fix small grammar: we'll construct properly
                lines = ["Active Topics:"] + [f"{t} ({c} user{'s' if c != 1 else ''})" for t, c in snapshot.items()]
                await websocket.send_text(json.dumps({"type": "topic_list", "data": "\n".join(lines)}))
                continue

            # Otherwise expect JSON with "message"
            try:
                msg_obj = json.loads(text)
                message_text = msg_obj.get("message")
                if message_text is None:
                    await websocket.send_text(json.dumps({"error": "missing 'message' field"}))
                    continue
            except json.JSONDecodeError:
                # invalid JSON payload — handle gracefully
                await websocket.send_text(json.dumps({"error": "invalid JSON payload"}))
                logger.warning("Invalid JSON from %s in topic %s: %s", username, topic, text)
                continue

            # Build message
            ts = int(time.time())
            message = {"username": username, "message": message_text, "timestamp": ts}

            # Store message and schedule expiry
            async with state_lock:
                if topic not in topic_messages:
                    topic_messages[topic] = []
                topic_messages[topic].append(message)
            asyncio.create_task(schedule_message_expiry(topic, message))

            # Broadcast to others
            await broadcast_to_topic(topic, {"type": "message", **message}, exclude_username=username)

            # Send ack to sender
            try:
                await websocket.send_text(json.dumps({"type": "ack", "status": "delivered", "timestamp": ts}))
            except Exception as e:
                logger.error("Error sending ack to %s: %s", username, e)

    except WebSocketDisconnect:
        logger.info("WebSocketDisconnect: %s disconnected from topic %s", username, topic)
    except Exception as e:
        logger.exception("Unexpected error for user %s in topic %s: %s", username, topic, e)
    finally:
        # Session cleanup
        if username and topic:
            async with state_lock:
                users = topics.get(topic)
                if users and username in users:
                    users.pop(username)
                # remove topic if empty
                if users is None or len(users) == 0:
                    topics.pop(topic, None)
                    topic_messages.pop(topic, None)
                    logger.info("Removed empty topic: %s", topic)
        try:
            await websocket.close()
        except Exception:
            pass


@app.get("/")
async def root():
    return HTMLResponse("<h3>FastAPI WebSocket Chat Server</h3><p>Use a WebSocket client to connect to /ws</p>")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
