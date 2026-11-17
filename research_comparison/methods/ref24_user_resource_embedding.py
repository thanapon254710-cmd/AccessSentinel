"""Reference [24] user-resource graph embedding baseline.

Implements the paper's core bipartite spectral-embedding + nearest-neighbour
anomaly score, adapted to AccessSentinel's user/application/resource events.

Paper-specific core:
  * user-resource bipartite adjacency
  * regularized bi-Laplacian L_tau
  * rank-d SVD; U S^(1/2) user embeddings, V S^(1/2) resource embeddings
  * unit-sphere normalization of user embeddings
  * anomaly score = distance to nearest previous user of that resource

Benchmark adaptations are documented in research_comparison/README.md.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable
import numpy as np
from scipy.linalg import svd
from scipy.spatial.distance import cdist

@dataclass
class Ref24Config:
    embedding_dim: int = 8  # Explicit benchmark setting; paper recommends scree-plot elbow.
    distance: str = "cosine"  # Paper permits Euclidean or cosine; selected and reported here.
    eps: float = 1e-12

class UserResourceSpectralNN:
    def __init__(self, config: Ref24Config | None = None):
        self.config = config or Ref24Config()
        self.user_to_i = {}
        self.resource_to_i = {}
        self.user_emb = None
        self.resource_emb = None
        self.prior_users_by_resource = {}
        self.tau = None
        self.actual_dim = None

    def fit(self, train_events: Iterable):
        events = list(train_events)
        users = sorted({str(e.u) for e in events})
        resources = sorted({f"{e.tenant}/{e.a}:{e.r}" for e in events})
        if not users or not resources:
            raise ValueError("Reference [24] requires at least one training user-resource interaction")
        self.user_to_i = {u:i for i,u in enumerate(users)}
        self.resource_to_i = {r:i for i,r in enumerate(resources)}
        A = np.zeros((len(users), len(resources)), dtype=np.float64)
        for e in events:
            A[self.user_to_i[str(e.u)], self.resource_to_i[f"{e.tenant}/{e.a}:{e.r}"]] = 1.0
        du = A.sum(axis=1)
        dr = A.sum(axis=0)
        # The paper recommends average degree as regularization.
        self.tau = float(A.sum() * 2.0 / (A.shape[0] + A.shape[1]))
        Lu = np.diag(1.0 / np.sqrt(du + self.tau))
        Lr = np.diag(1.0 / np.sqrt(dr + self.tau))
        L = Lu @ A @ Lr
        U, s, Vt = svd(L, full_matrices=False, check_finite=False, lapack_driver="gesdd")
        d = max(1, min(int(self.config.embedding_dim), len(s)))
        root = np.sqrt(np.maximum(s[:d], 0.0))
        Xu = U[:, :d] * root[None, :]
        Yr = Vt[:d, :].T * root[None, :]
        Xu /= np.maximum(np.linalg.norm(Xu, axis=1, keepdims=True), self.config.eps)
        Yr /= np.maximum(np.linalg.norm(Yr, axis=1, keepdims=True), self.config.eps)
        self.user_emb, self.resource_emb = Xu, Yr
        self.prior_users_by_resource = {r:set() for r in resources}
        return self

    def score_and_update(self, event) -> float:
        """Score an event using only users seen on this resource before this event."""
        u, r = str(event.u), f"{event.tenant}/{event.a}:{event.r}"
        if u not in self.user_to_i or r not in self.resource_to_i:
            return float("nan")  # cold-start node: original method does not define out-of-sample scoring
        prior = self.prior_users_by_resource.setdefault(r, set())
        if u in prior:
            score = 0.0
        elif not prior:
            # No prior resource users: method has no nearest-neighbour reference set.
            score = float("nan")
        else:
            q = self.user_emb[self.user_to_i[u]][None, :]
            refs = self.user_emb[[self.user_to_i[x] for x in sorted(prior) if x in self.user_to_i]]
            if len(refs) == 0:
                score = float("nan")
            else:
                metric = self.config.distance
                score = float(np.min(cdist(q, refs, metric=metric)))
        prior.add(u)
        return score
