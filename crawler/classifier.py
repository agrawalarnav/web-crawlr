import yake

# Single shared extractor — YAKE is stateless and thread-safe
_extractor = yake.KeywordExtractor(
    lan="en",
    n=3,          # up to trigrams
    dedupLim=0.7,
    dedupFunc="seqm",
    windowsSize=1,
    top=15,
)


def extract_topics(
    title: str,
    meta_description: str,
    body_text: str,
    max_topics: int = 10,
) -> list[str]:
    # Repeat title and description to weight them more heavily than body
    combined = f"{title}. {title}. {meta_description}. {body_text[:2000]}"
    combined = combined.strip()

    if len(combined) < 20:
        return []

    try:
        # YAKE returns (keyword, score); lower score = more relevant
        keywords = _extractor.extract_keywords(combined)
        return [kw for kw, _ in keywords][:max_topics]
    except Exception:
        return []
