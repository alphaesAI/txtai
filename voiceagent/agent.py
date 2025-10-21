from src.python.txtai.agent.base import Agent
import yaml

with open(r"/home/logi/txtai/agentconfig.yml") as f:
    config = yaml.safe_load(f)

llm_config = config.get("llm", {})
agent_config = config.get("agent", {})

full_config = {
    **agent_config,
    "model": llm_config,
    "tools": []
}

agent = Agent(**full_config)

if __name__ == "__main__":
    conversation_history = ""
    print("Agent is ready. Starting terminal chat:")

    while True:
        try:
            question = agent("conversation_agent", text=conversation_history)
            print("Agent:", question)

            answer = input("You: ")
            conversation_history += f"Agent: {question}\nUser: {answer}\n"

        except Exception as e:
            print("Error during agent processing:", e)
            break