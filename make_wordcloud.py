"""Very basic wordcloud example"""
from wordcloud import WordCloud

text_count = {
    "cocks": 33,
    "chickens":22,
    "revenue": 11,
    "gibber":3,
    "gabber":2
}

my_wordcloud = WordCloud().generate_from_frequencies(text_count)
my_wordcloud.to_file("./data/wordcloud2.png")
