"""Task 6 — ABSA (Aspect-Based Sentiment Analysis) 본 실행.

What this script does
---------------------
For every document in ``reviews_clean`` that has aspect-tagged sentences
(``review_data.aspects_sentences`` — produced by Task 5), classify each
aspect's sentences as 긍정 (positive) or 부정 (negative), and write the
result to a *separate* collection ``reviews_absa`` so the source
collection is never mutated.

Model
-----
``matthewburke/korean_sentiment`` — ELECTRA-based binary sentiment
classifier. Selected in Phase 1 (see ``DB/phase1_model_eval/REPORT.md``)
based on the accuracy × efficiency trade-off on a 910-sample evaluation.

Output document (one per source document)
-----------------------------------------
.. code-block:: json

   {
     "_id": "<mirrors reviews_clean._id>",
     "product_id": "...",
     "persona": "...",
     "rating": 5,
     "absa_version": "v1_130k",
     "absa_result": {
       "소재": {"label": "긍정", "score": 0.91},
       "핏":   {"label": "부정", "score": 0.78}
     }
   }

Only aspects matched in the source document appear in ``absa_result``.

Resume semantics
----------------
Already-processed documents (``_id`` present in ``reviews_absa``) are
skipped automatically. Re-running the script is safe and picks up where
it left off — useful for long runs that may be interrupted.

Usage
-----
    python task6_absa.py
    python task6_absa.py --limit 1000         # smoke test
    python task6_absa.py --version v2_300k    # tag output when rerun on enlarged data
    python task6_absa.py --reset              # wipe reviews_absa and start over

Environment
-----------
Requires ``.env`` in the parent ``DB/`` directory with ``MONGO_URI``,
``MONGO_DB``, ``MONGO_COLLECTION_CLEAN`` (output collection name is
hard-coded as ``reviews_absa``; override via ``--output-collection`` if
needed).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping, Sequence

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from pymongo.operations import UpdateOne

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL_ID: str = "matthewburke/korean_sentiment"
POSITIVE_LABEL: str = "긍정"
NEGATIVE_LABEL: str = "부정"

OUTPUT_COLLECTION_DEFAULT: str = "reviews_absa"
ABSA_VERSION_DEFAULT: str = "v1_130k"

BATCH_DOCS: int = 64   # Mongo → pipeline: how many *documents* per pass
BATCH_INFER: int = 8   # transformers pipeline batch_size (CPU friendly)
LOG_EVERY_DOCS: int = 500

POSITIVE_HINTS: tuple[str, ...] = ("pos", "positive", "긍정", "label_1", "1")
NEGATIVE_HINTS: tuple[str, ...] = ("neg", "negative", "부정", "label_0", "0")


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AspectTask:
    """One aspect's text extracted from a source document."""

    doc_id: object          # Mongo _id (ObjectId | str)
    aspect: str
    aspect_text: str        # sentences for this aspect, joined by " | "


@dataclass(frozen=True)
class AspectResult:
    aspect: str
    label: str
    score: float


@dataclass(frozen=True)
class DocResult:
    doc_id: object
    product_id: str
    persona: str
    rating: int | None
    aspect_results: tuple[AspectResult, ...]


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
def connect(output_collection_name: str) -> tuple[MongoClient, Collection, Collection]:
    uri: str | None = os.environ.get("MONGO_URI")
    db_name: str = os.environ.get("MONGO_DB", "musinsa_db")
    src_name: str = os.environ.get("MONGO_COLLECTION_CLEAN", "reviews_clean")

    if not uri:
        raise RuntimeError("MONGO_URI not set in .env")

    client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    try:
        client.admin.command("ping")
    except ConnectionFailure as exc:
        raise RuntimeError(f"MongoDB connection failed: {exc}") from exc

    db = client[db_name]
    return client, db[src_name], db[output_collection_name]


# ---------------------------------------------------------------------------
# Inference
# ---------------------------------------------------------------------------
def _pick_device() -> tuple[str, int]:
    """Return ('cuda', 0) if a CUDA GPU is present, else ('cpu', -1)."""
    try:
        import torch  # type: ignore[import-not-found]
    except ImportError:
        return ("cpu", -1)
    if torch.cuda.is_available():
        return ("cuda", 0)
    return ("cpu", -1)


class SentimentRunner:
    """Thin wrapper around transformers' text-classification pipeline.

    - Auto-maps raw model labels (e.g. ``LABEL_0``, ``positive``) to
      our Korean labels ``긍정`` / ``부정``.
    - Truncates at the model's max length to avoid position-embedding
      overflow on long concat-ed aspect text.
    - Falls back to a placeholder for empty texts so the pipeline never
      receives an empty string.
    """

    def __init__(self, model_id: str, batch_size: int) -> None:
        from transformers import pipeline  # type: ignore[import-not-found]

        device_name, device_index = _pick_device()
        self._device_name = device_name
        self._batch_size = batch_size
        self._pipe = pipeline(
            "text-classification",
            model=model_id,
            device=device_index,
            top_k=None,
            truncation=True,
        )
        self._label_map: dict[str, str] = self._build_label_map()

    @property
    def device(self) -> str:
        return self._device_name

    def _build_label_map(self) -> dict[str, str]:
        config = self._pipe.model.config
        id2label: dict[int, str] = getattr(config, "id2label", {}) or {}
        mapping: dict[str, str] = {}
        for raw_label in id2label.values():
            norm = str(raw_label).lower()
            if any(hint in norm for hint in POSITIVE_HINTS):
                mapping[str(raw_label)] = POSITIVE_LABEL
            elif any(hint in norm for hint in NEGATIVE_HINTS):
                mapping[str(raw_label)] = NEGATIVE_LABEL
            else:
                raise RuntimeError(
                    f"Cannot classify raw label '{raw_label}' as 긍정/부정 "
                    f"(model id2label={id2label})"
                )
        if set(mapping.values()) != {POSITIVE_LABEL, NEGATIVE_LABEL}:
            raise RuntimeError(
                f"Label map incomplete: got {mapping} from {id2label}"
            )
        return mapping

    def _sanitize(self, text: str) -> str:
        safe = text.strip()
        return safe if safe else "."

    def predict(self, texts: Sequence[str]) -> list[tuple[str, float]]:
        sanitized = [self._sanitize(t) for t in texts]
        raw = self._pipe(sanitized, batch_size=self._batch_size)

        out: list[tuple[str, float]] = []
        for entry in raw:
            candidates = entry if isinstance(entry, list) else [entry]
            best: tuple[str, float] | None = None
            for item in candidates:
                mapped = self._label_map.get(str(item["label"]))
                if mapped is None:
                    continue
                score = float(item["score"])
                if best is None or score > best[1]:
                    best = (mapped, score)
            if best is None:
                raise RuntimeError(f"No valid label in output: {entry}")
            out.append(best)
        return out


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def _extract_aspect_tasks(doc: Mapping[str, object]) -> Iterator[AspectTask]:
    """Yield one AspectTask per (doc, aspect) pair that has real text."""
    review_data = doc.get("review_data")
    if not isinstance(review_data, Mapping):
        return
    aspects_sentences = review_data.get("aspects_sentences")
    if not isinstance(aspects_sentences, Mapping) or not aspects_sentences:
        return

    doc_id = doc.get("_id")
    for aspect, sentences in aspects_sentences.items():
        if not isinstance(sentences, list) or not sentences:
            continue
        text = " | ".join(str(s) for s in sentences if isinstance(s, str)).strip()
        if not text:
            continue
        yield AspectTask(doc_id=doc_id, aspect=str(aspect), aspect_text=text)


def _coerce_rating(raw: object) -> int | None:
    if raw is None:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def _load_already_processed(output_coll: Collection) -> set[object]:
    """Return the set of _ids already present in the output collection."""
    ids: set[object] = set()
    for doc in output_coll.find({}, {"_id": 1}):
        ids.add(doc["_id"])
    return ids


def _iter_source_batches(
    src: Collection,
    already: set[object],
    batch_size: int,
    limit: int | None,
) -> Iterator[list[Mapping[str, object]]]:
    query: dict[str, object] = {
        "review_data.aspects_sentences": {"$exists": True, "$ne": {}},
    }
    projection: dict[str, int] = {
        "_id": 1,
        "product_id": 1,
        "persona": 1,
        "rating": 1,
        "review_data.aspects_sentences": 1,
    }

    batch: list[Mapping[str, object]] = []
    seen = 0
    for doc in src.find(query, projection, batch_size=1_000):
        if doc["_id"] in already:
            continue
        batch.append(doc)
        seen += 1
        if len(batch) >= batch_size:
            yield batch
            batch = []
        if limit is not None and seen >= limit:
            break
    if batch:
        yield batch


def _classify_batch(
    runner: SentimentRunner,
    source_docs: Sequence[Mapping[str, object]],
) -> list[DocResult]:
    """Flatten source docs → aspect tasks → classify → regroup by doc."""
    tasks: list[AspectTask] = []
    doc_task_ranges: list[tuple[Mapping[str, object], int, int]] = []
    for doc in source_docs:
        start = len(tasks)
        tasks.extend(_extract_aspect_tasks(doc))
        end = len(tasks)
        if end > start:
            doc_task_ranges.append((doc, start, end))

    if not tasks:
        return []

    predictions = runner.predict([t.aspect_text for t in tasks])

    results: list[DocResult] = []
    for doc, start, end in doc_task_ranges:
        aspect_results = tuple(
            AspectResult(
                aspect=tasks[i].aspect,
                label=predictions[i][0],
                score=predictions[i][1],
            )
            for i in range(start, end)
        )
        results.append(
            DocResult(
                doc_id=doc["_id"],
                product_id=str(doc.get("product_id", "")),
                persona=str(doc.get("persona", "")),
                rating=_coerce_rating(doc.get("rating")),
                aspect_results=aspect_results,
            )
        )
    return results


def _write_results(
    output_coll: Collection,
    results: Sequence[DocResult],
    version: str,
) -> int:
    if not results:
        return 0

    operations: list[UpdateOne] = []
    for r in results:
        absa_result: dict[str, dict[str, object]] = {}
        for ar in r.aspect_results:
            absa_result[ar.aspect] = {"label": ar.label, "score": ar.score}
        operations.append(
            UpdateOne(
                {"_id": r.doc_id},
                {
                    "$set": {
                        "product_id": r.product_id,
                        "persona": r.persona,
                        "rating": r.rating,
                        "absa_version": version,
                        "absa_result": absa_result,
                    }
                },
                upsert=True,
            )
        )
    output_coll.bulk_write(operations, ordered=False)
    return len(operations)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only this many source documents (for smoke tests).",
    )
    parser.add_argument(
        "--version",
        type=str,
        default=ABSA_VERSION_DEFAULT,
        help=f"absa_version tag on each output doc (default: {ABSA_VERSION_DEFAULT}).",
    )
    parser.add_argument(
        "--batch-docs",
        type=int,
        default=BATCH_DOCS,
        help=f"Documents per pipeline pass (default: {BATCH_DOCS}).",
    )
    parser.add_argument(
        "--batch-infer",
        type=int,
        default=BATCH_INFER,
        help=f"transformers pipeline batch size (default: {BATCH_INFER}).",
    )
    parser.add_argument(
        "--output-collection",
        type=str,
        default=OUTPUT_COLLECTION_DEFAULT,
        help=f"Output Mongo collection (default: {OUTPUT_COLLECTION_DEFAULT}).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop the output collection before running. DESTRUCTIVE.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    client, src, dst = connect(args.output_collection)
    try:
        if args.reset:
            dst.drop()
            print(f"[reset] dropped collection '{dst.name}'")

        total_candidates: int = src.count_documents(
            {"review_data.aspects_sentences": {"$exists": True, "$ne": {}}}
        )
        print(f"source candidates (aspect-matched docs): {total_candidates:,}")

        already = _load_already_processed(dst)
        if already:
            print(f"already processed (resume): {len(already):,}")

        runner = SentimentRunner(MODEL_ID, args.batch_infer)
        print(f"model loaded: {MODEL_ID} on {runner.device}")

        processed = 0
        written = 0
        failed_batches = 0
        t_start = time.perf_counter()

        for batch in _iter_source_batches(src, already, args.batch_docs, args.limit):
            try:
                results = _classify_batch(runner, batch)
            except Exception as exc:
                failed_batches += 1
                print(f"  [warn] batch failed (skipped {len(batch)} docs): {exc}")
                continue

            written += _write_results(dst, results, args.version)
            processed += len(batch)

            if processed % LOG_EVERY_DOCS == 0 or processed == args.limit:
                elapsed = time.perf_counter() - t_start
                rate = processed / elapsed if elapsed > 0 else 0.0
                print(
                    f"  processed {processed:,} docs "
                    f"/ written {written:,} "
                    f"/ elapsed {elapsed:.1f}s "
                    f"/ {rate:.1f} docs/sec"
                )

        elapsed_total = time.perf_counter() - t_start
        print("\n=== Task 6 완료 ===")
        print(f"  이번 세션 처리: {processed:,} docs")
        print(f"  결과 저장: {written:,} docs (collection: {dst.name})")
        print(f"  실패 배치: {failed_batches}")
        print(f"  총 시간: {elapsed_total:.1f}s")

    finally:
        client.close()


if __name__ == "__main__":
    main()
