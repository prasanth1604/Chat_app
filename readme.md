# FastAPI WebSocket Topic Chat

A lightweight **real-time topic-based chat server** built with **FastAPI** and **WebSockets**. Messages auto-expire after 30 seconds, and topics are removed when all users leave.

---

## 🧩 Features

* Join topic-based chat rooms via WebSocket
* Auto-unique usernames within a topic (`alice#2`, etc.)
* Broadcast messages to all users in the same topic
* Message expiry after 30 seconds (in-memory only)
* `/list` command to view active topics and user counts

---

## ⚙️ Requirements

* Dependencies listed in `requirements.txt`:

Install all dependencies:

```bash
pip install -r requirements.txt
```

---

## 🚀 Running the Server

Run the FastAPI WebSocket server using **Uvicorn**:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

or simply:

```bash
python main.py
```

Server starts at:

```
http://localhost:8000
```

WebSocket endpoint:

```
ws://localhost:8000/ws
```

---

## 💬 Running the Client

Run the Python WebSocket client example:

```bash
python client_example.py
```

Enter the prompts:

```
username: alice
topic: sports
```

Then in another terminal:

```
username: bob
topic: sports
```

Now both users can chat in real time within the same topic.

---

## 🧠 Client Commands

| Command  | Description                              |
| -------- | ---------------------------------------- |
| Any text | Sends message to users in the same topic |
| `/list`  | Lists all active topics with user counts |
| `/quit`  | Disconnects from chat                    |

---

## ✅ Testing Scenarios

1. Two users join the same topic and exchange messages.
2. `/list` shows all active topics and user counts.
3. Messages expire automatically after **30 seconds**.
4. Topics disappear when all users leave.
5. Invalid JSON payloads are handled gracefully.

---

## 🧹 Cleanup & State Management

* User is removed when disconnected.
* Topic is deleted when no users remain.
* All state is stored in memory (no external DB or broker).
* Messages are purged after 30 seconds using `asyncio.create_task`.

---

## 🧰 Tech Stack

* **FastAPI** — lightweight backend framework
* **Uvicorn** — ASGI server for running FastAPI
* **WebSockets** — real-time communication

---

## 🔒 Constraints

* No databases or message brokers used
* No Socket.IO or high-level WebSocket wrappers
* Pure FastAPI + Python asyncio solution

---

## 📂 Project Structure

```
├── main.py              # FastAPI WebSocket server
├── client_example.py    # Sample interactive client
├── requirements.txt     # Dependencies
└── README.md            # This file
```

