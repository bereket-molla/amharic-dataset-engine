import re

def clean_text(text: str) -> str:
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"[^\u1200-\u137F፡።፣፤፥፦፧\s]", "", text)
    return text.strip()

def contains_fidel(text: str) -> bool:
    return bool(re.search(r"[\u1200-\u137F]", text))
