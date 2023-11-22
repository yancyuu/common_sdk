# -*- coding: utf-8 -*-

import openai
import backoff
import tiktoken
import pandas as pd
from openai.embeddings_utils import get_embeddings
from ..base_class.singleton import SingletonMetaThreadSafe as SingletonMetaclass
from ..system.sys_env import get_env
import random


"""Openai的client类"""
class OpenaiClient(metaclass=SingletonMetaclass):

    def __init__(self):
        api_keys = get_env("OPENAI_API_KEY").split(",")
        # 从 api_keys 列表中随机选择一个密钥
        selected_api_key = random.choice(api_keys)
        openai.api_key = selected_api_key
        openai.api_base = "https://api.openai-proxy.com/v1"

    def get_df_by_csv(self, csv_path):
        embedding_encoding = "cl100k_base"
        df = pd.read_csv(csv_path, sep='_!_', names=['产品型号', 'code', 'category', 'title', 'keywords'])
        df = df.fillna("")
        df["combined"] = "标题: " + df.title.str.strip() + "; 关键字: " + df.keywords.str.strip()
        print("Lines of text before filtering: ", len(df))

        encoding = tiktoken.get_encoding(embedding_encoding)
        df["n_tokens"] = df.combined.apply(lambda x: len(encoding.encode(x)))

        return df

    @staticmethod
    def get_embeddings_with_backoff(prompts, engine):
        embeddings = []
        for batch in prompts:
            embeddings += get_embeddings(list_of_text=batch, engine=engine)
        return embeddings

    @backoff.on_exception(backoff.expo, openai.error.RateLimitError)
    def get_embeddings(self, df, save_path, embedding_model="text-embedding-ada-002"):
        max_tokens = 8000
        df_all = df
        embeddings = []
        current_batch = []
        current_tokens = 0

        for _, row in df_all.iterrows():
            if current_tokens + row.n_tokens > max_tokens:
                # 获取当前批次的 embeddings
                batch_embeddings = self.get_embeddings_with_backoff(prompts=[current_batch], engine=embedding_model)
                embeddings += batch_embeddings
                # 重置批次
                current_batch = [row.combined]
                current_tokens = row.n_tokens
            else:
                current_batch.append(row.combined)
                current_tokens += row.n_tokens

        # 获取最后一个批次的 embeddings
        if current_batch:
            batch_embeddings = self.get_embeddings_with_backoff(prompts=[current_batch], engine=embedding_model)
            embeddings += batch_embeddings

        df_all["embedding"] = embeddings
        df_all.to_parquet(save_path, index=True)


    def list_openai_models(self):
        """
        列出 OpenAI 提供的所有模型。

        :param api_key: OpenAI API 密钥。
        :return: 模型列表。
        """
        try:
            models = openai.Model.list()
            return models
        except Exception as e:
            print(f"Error in listing models: {e}")
            return None

openai_client = OpenaiClient()

if __name__ == "__main__":
    openai_client = OpenaiClient()
    csv_path = "./data/combined_columns_corrected.csv"
    save_path = "./data/toutiao_cat_data_all_with_embeddings.parquet"
    df = openai_client.get_df_by_csv(csv_path)
    openai_client.get_embeddings(df, save_path)
    
