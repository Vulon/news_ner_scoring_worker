import re

def clean_text(text: str):
    text = re.sub("\@.*\s", "", text)
    text = re.sub("\&\#8217;", "'", text)
    text = re.sub("\&\#8220;", ' " ', text)
    text = re.sub("\&\#8221;", ' " ', text)
    text = re.sub("\&\#8230;", ' ... ', text)
    return text