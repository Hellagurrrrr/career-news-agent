from langchain_deepseek import ChatDeepSeek

from settings.config import DEEPSEEK_API_KEY

def get_base_model(temperature: float = 0.0) -> ChatDeepSeek:
    return ChatDeepSeek(
        model="deepseek-v4-pro",
        api_key=DEEPSEEK_API_KEY,
        temperature=temperature,
        extra_body={"thinking": {"type": "disabled"}},
    )