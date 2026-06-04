import requests

url = "https://sci.bban.top/pdf/10.1016/j.fm.2018.02.007.pdf"

headers = {
    "User-Agent": "Mozilla/5.0"
}

r = requests.get(url, headers=headers, timeout=60)
r.raise_for_status()

with open("paper.pdf", "wb") as f:
    f.write(r.content)

print("Downloaded: paper.pdf")