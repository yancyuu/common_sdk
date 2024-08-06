
from common_sdk.base_class.singleton import SingletonMetaThreadSafe
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
import seaborn as sns
import io
import base64

class SentenceCluster(metaclass=SingletonMetaThreadSafe):
    """
    SentenceCluster类用于对输入的短语进行聚类，以及生成相似度矩阵和聚类结果。
    """

    def __init__(self, model_name):
        self._model = None
        self.model_name = model_name

    @property
    def model(self):
        if self._model is None:
            self._model = SentenceTransformer(self.model_name)
        return self._model
    
    def encode_phrases(self, phrases, batch_size=8):
        embeddings = []
        for i in range(0, len(phrases), batch_size):
            batch = phrases[i:i + batch_size]
            batch_embeddings = self.model.encode(batch)
            embeddings.extend(batch_embeddings)
        return np.array(embeddings)
    
    def cluster_phrases(self, attention_phrases, identity, eps=0.06, min_samples=1):
        if not attention_phrases or not identity:
            return []
        attention_vectors = self.encode_phrases(attention_phrases)
        identity_vector = self.encode_phrases([identity])[0]

        #identity_similarities = cosine_similarity(attention_vectors, identity_vector.reshape(1, -1))
        #identity_sim_img = self.plot_similarity(identity_similarities, attention_phrases, "Similarity to Identity")

        cosine_sim_matrix = cosine_similarity(attention_vectors)
        #cosine_sim_img = self.plot_similarity(cosine_sim_matrix, attention_phrases, "Cosine Similarity Matrix")

        # 将余弦相似度矩阵标准化到 [0, 1] 范围
        norm_cosine_sim_matrix = (cosine_sim_matrix + 1) / 2

        # 计算距离矩阵（1 - 归一化的余弦相似度）
        distance_matrix = 1 - norm_cosine_sim_matrix

        # 确保没有负值
        distance_matrix[distance_matrix < 0] = 0

        # 使用DBSCAN进行聚类
        dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='precomputed')
        labels = dbscan.fit_predict(distance_matrix)

        # 打印聚类结果
        clusters = {}
        for phrase, label in zip(attention_phrases, labels):
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(phrase)

        return  [phrases for cluster_id, phrases in clusters.items()]
