import srt
from itertools import islice
from sklearn.feature_extraction.text import HashingVectorizer


def substring2fragments(sub_string, n=5, with_time=False):
    parsed = srt.parse(sub_string)
    windowed_subs = window(parsed, n=n)

    def window2text(win):
        text = '\n\n'.join(
            map(
                lambda s: s.content,
                win
            )
        )
        if with_time:
            return (win[0].start, text)
        return text

    return list(map(window2text, windowed_subs))


def window(seq, n=2):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


def string2vector(string):
    vectorizer = HashingVectorizer(n_features=2**4)
    return vectorizer.transform([string])[0]
