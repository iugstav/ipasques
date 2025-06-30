#!/usr/bin/env python
import os
import re
import json
import multiprocessing
from nltk import download
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
import numpy as np
import pandas as pd

_ = download("stopwords")
_ = download("wordnet")

NUM_PROCESSES = multiprocessing.cpu_count()
MIN_SCORE = 0.4
MIN_DF = 2
MAX_DF = 0.8

_LEMMATIZER = WordNetLemmatizer()
_STOPWORDS = set(stopwords.words("english"))
_NON_WORD_RE = re.compile(r"[^\w\s]")


def read_content(df):
    return df.apply(lambda x: open(x, "r", encoding="utf-8").read().lower())


def parallel_read_content(df, num_processes=8):
    df_split = np.array_split(df, num_processes)
    pool = multiprocessing.Pool(num_processes)
    data = pool.map(read_content, df_split)
    pool.close()
    pool.join()

    return pd.concat(data)


def load_and_read_data(path, base_folder="crawler/"):
    print(f"Carregando CSV de {path}...")
    df = pd.read_csv(path, delimiter=",")
    df["content_path"] = df["content_path"].apply(
        lambda p: os.path.join(base_folder, p)
    )

    unique_df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)
    unique_df["content_text"] = parallel_read_content(unique_df["content_path"])

    print("Leitura concluída.")
    return unique_df


def preprocess_text(txt):
    txt = txt.lower()
    txt = _NON_WORD_RE.sub("", txt)
    tokens = txt.split()
    tokens = [
        _LEMMATIZER.lemmatize(t) for t in tokens if len(t) > 2 and t not in _STOPWORDS
    ]

    return " ".join(tokens)


def parallel_preprocess_text(texts):
    print("Pré-processando textos em paralelo com multiprocessing.Pool...")
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        processed = pool.map(preprocess_text, texts.tolist())

    return pd.Series(processed, index=texts.index)


def build_tfidf_matrix(texts):
    preprocessed = parallel_preprocess_text(texts)

    print("Construindo matriz TF-IDF...")
    vec = TfidfVectorizer(
        preprocessor=None,
        tokenizer=lambda s: s.split(),
        min_df=MIN_DF,
        max_df=MAX_DF,
    )
    mat = vec.fit_transform(preprocessed)
    print(f"TF-IDF pronta: {mat.shape[0]} documentos x {mat.shape[1]} termos.")

    return mat


def get_top_k_blocks(matrix, k=5, block_size=500):
    n_docs = matrix.shape[0]
    recs = {}

    print("Calculando similaridades em blocos...")
    for start in tqdm(range(0, n_docs, block_size), desc="Blocos"):
        end = min(start + block_size, n_docs)
        block = matrix[start:end]

        sims_block = cosine_similarity(block, matrix)
        for offset, sims in enumerate(sims_block):
            i = start + offset
            sims[i] = -1  # ignora identidade

            candidates = [
                j for j, score in enumerate(sims) if score >= MIN_SCORE and j != i
            ]
            # seleciona top-k dentre candidatos
            if len(candidates) > k:
                scores = sims[candidates]
                top_idx = np.argpartition(-scores, k)[:k]
                sorted_idxs = np.array(candidates)[top_idx][
                    np.argsort(-scores[top_idx])
                ]
            else:
                sorted_idxs = sorted(candidates, key=lambda j: sims[j], reverse=True)[
                    :k
                ]
            recs[i] = [(int(j), float(sims[j])) for j in sorted_idxs]

    print("Similaridades calculadas.")
    return recs


if __name__ == "__main__":
    data = load_and_read_data("output.csv")
    tfid_mat = build_tfidf_matrix(data["content_text"])
    recommendations = get_top_k_blocks(tfid_mat)

    # for post_id, recs in recommendations.items():
    #     print(f"\nPara o post {post_id} ({data.loc[post_id,'title']}):")
    #     for idx, score in recs:
    #         print(f"  → {idx}: {data.loc[idx,'title']} (score {score:.3f})")

    output_file = "recommendations.jsonl"
    with open(output_file, "w", encoding="utf-8") as f:
        for doc_id, recs in recommendations.items():
            record = {
                "post_id": int(doc_id),
                "title": data.loc[doc_id, "title"],
                "recommendations": [
                    {"id": int(r_id), "title": data.loc[r_id, "title"], "score": score}
                    for r_id, score in recs
                ],
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Recomendações salvas em {output_file}")
