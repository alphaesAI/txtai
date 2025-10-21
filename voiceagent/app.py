from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from voiceagent.agent import agent

app = FastAPI()

@app.websocket("/chat")
async def chat_endpoint(ws: WebSocket):
    await ws.accept()
    print("Client connected to chat")
    conversation_history = ""

#    greeting = agent("conversation_agent", text="start the conversation with a friendly greeting.")

    greeting = agent(text="start the conversation with a friendly greeting. say simply")
    await ws.send_text(greeting)

    conversation_history += f"Agent: {greeting}\n"

    try:
        while True:
            user_message = await ws.receive_text()
            print(f"User said: {user_message}")

            conversation_history += f"User: {user_message}\n"
        
            # agent_response = agent("conversation_agent", text=conversation_history)

            agent_response = agent(text=conversation_history)
            conversation_history += f"Agent: {agent_response}\n"
            
            await ws.send_text(agent_response)
            print(f"Agent replied: {agent_response}")

    except WebSocketDisconnect:
        print("Disconnected chat")