import json

from bs4 import BeautifulSoup


def parse_metadata(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    return {
        "title": _title(soup),
        "meta_description": _meta_desc(soup),
        "meta_keywords": _meta_keywords(soup),
        "og_tags": _og_tags(soup),
        "canonical_url": _canonical(soup),
        "h1_tags": _h1_tags(soup),
        "structured_data": _structured_data(soup),
        "html_lang": _html_lang(soup),
    }


def _title(soup: BeautifulSoup) -> str:
    tag = soup.find("title")
    if tag and tag.get_text(strip=True):
        return tag.get_text(strip=True)
    og = soup.find("meta", property="og:title")
    return og.get("content", "").strip() if og else ""


def _meta_desc(soup: BeautifulSoup) -> str:
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        return tag["content"].strip()
    og = soup.find("meta", property="og:description")
    return og.get("content", "").strip() if og else ""


def _meta_keywords(soup: BeautifulSoup) -> list[str]:
    tag = soup.find("meta", attrs={"name": "keywords"})
    if not tag or not tag.get("content"):
        return []
    return [k.strip() for k in tag["content"].split(",") if k.strip()]


def _og_tags(soup: BeautifulSoup) -> dict[str, str]:
    result = {}
    for tag in soup.find_all("meta", attrs={"property": True}):
        prop = tag["property"]
        if prop.startswith(("og:", "twitter:")):
            result[prop] = tag.get("content", "")
    return result


def _canonical(soup: BeautifulSoup) -> str:
    tag = soup.find("link", rel="canonical")
    return tag.get("href", "").strip() if tag else ""


def _h1_tags(soup: BeautifulSoup) -> list[str]:
    return [h.get_text(strip=True) for h in soup.find_all("h1") if h.get_text(strip=True)]


def _structured_data(soup: BeautifulSoup) -> list:
    result = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            result.append(json.loads(script.string or ""))
        except (json.JSONDecodeError, TypeError):
            pass
    return result


def _html_lang(soup: BeautifulSoup) -> str:
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        return html_tag["lang"].split("-")[0].lower()
    return ""
