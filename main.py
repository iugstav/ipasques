#!/usr/bin/env python
import os
import uuid
import pandas as pd
import nltk
import re
from nltk import corpus
from nltk.stem import WordNetLemmatizer
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

_ = nltk.download("omw-1.4")
_ = nltk.download("wordnet")
_ = nltk.download("wordnet2022")
_ = nltk.download("stopwords")


def pre_process_text(
    text,
    remove_stopwords=True,
    use_stemming=False,
    use_lemmatization=True,
    custom_stopwords=None,
):
    text = str(text).lower().strip()
    text = re.sub(r"[^\w\s]", "", text)
    tokens = text.split()

    if remove_stopwords:
        if custom_stopwords is None:
            custom_stopwords = set(corpus.stopwords.words("english"))
        tokens = [word for word in tokens if word not in custom_stopwords]
    if use_lemmatization:
        lemmatizer = WordNetLemmatizer()
        tokens = [lemmatizer.lemmatize(word) for word in tokens]
    if use_stemming:
        stemmer = PorterStemmer()
        tokens = [stemmer.stem(word) for word in tokens]

    return " ".join(tokens)


def load_data(path):
    df = pd.read_csv(path)
    unique_df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)

    new_df = unique_df.copy()[["title", "content_path", "tags"]]
    new_df["content_path"] = df.apply(
        lambda r: os.path.join("crawler/", r["content_path"]), axis=1
    )

    new_df["id"] = new_df.index
    new_df["content_text"] = new_df.apply(read_content, axis=1)
    new_df.drop(columns=["content_path"], inplace=True)

    return new_df


def read_content(row):
    try:
        with open(row["content_path"]) as content:
            return content.read().lower()
    except Exception as e:
        print(f"Error reading {row['content_path']} : {e}")
        return ""


def similarity(df, text_column, use_custom_preprocess=True, **preproc_params):
    if use_custom_preprocess:
        df["clean_blog_content"] = df["content_text"].apply(
            lambda r: pre_process_text(r, **preproc_params)
        )

        tfidf = TfidfVectorizer(stop_words=None)
        tfidf_matrix = tfidf.fit_transform(df["clean_blog_content"])
    else:
        tfidf = TfidfVectorizer(stop_words="english")
        tfidf_matrix = tfidf.fit_transform(df[text_column])

    cosine_sim = cosine_similarity(tfidf_matrix)
    return df, cosine_sim


def rank_posts(df, cosine_sim, ids, sim_threshold=0.95):
    recommended = set()
    for id in ids:
        sim_scores = cosine_sim[id]
        similar_indices = [
            i for i, score in enumerate(sim_scores) if score > sim_threshold and i != id
        ]
        for idx in similar_indices:
            recommended.add(idx)

    return list(recommended)


def get_similar_posts(post_id, df, cosine_sim, sim_threshold=0.95):
    sim_scores = cosine_sim[post_id]
    similar_indices = [
        i
        for i, score in enumerate(sim_scores)
        if score > similarity_threshold and i != post_id
    ]
    return similar_indices


if __name__ == "__main__":
    data = load_data("output.csv")

    # Define os parâmetros de pré-processamento
    preproc_params = {
        "remove_stopwords": True,
        "use_stemming": False,
        "use_lemmatization": True,
        "custom_stopwords": None,  # Utiliza as stopwords padrão do NLTK
    }

    # Computa a similaridade entre os blogs utilizando o pré-processamento customizado
    data, cosine_sim = similarity(
        data,
        text_column="content_text",
        use_custom_preprocess=True,
        **preproc_params,
    )

    # Define os IDs dos blogs (assumindo que a coluna "id" corresponde ao índice)
    blog_ids = list(data["id"].values)

    # Define o limiar de similaridade (pode ser parametrizado)
    similarity_threshold = 0.9
    recommended_ids = rank_posts(
        data, cosine_sim, blog_ids, sim_threshold=similarity_threshold
    )

    # Exibe os blogs recomendados, se houver
    if recommended_ids:
        print("Blogs recomendados:")
        print(data.loc[recommended_ids])
    else:
        print("Nenhum blog recomendado encontrado.")
