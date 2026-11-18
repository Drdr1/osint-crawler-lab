"""Raw page storage in MinIO (S3 API), addressed by content hash.

Content addressing gives de-duplication and idempotency for free: the same
bytes always land on the same key, so a re-processed message overwrites
nothing new and mirror pages cost one object, not N.
"""

from __future__ import annotations

import gzip
import hashlib
from typing import Any

from botocore.exceptions import ClientError


def content_hash(body: str) -> str:
    return hashlib.sha256(body.encode()).hexdigest()


def key_for(sha: str) -> str:
    return f"html/{sha[:2]}/{sha}.html.gz"


class BlobStore:
    def __init__(self, s3: Any, bucket: str) -> None:
        self.s3 = s3
        self.bucket = bucket
        self.writes = 0  # visible to tests: proves de-duplication

    async def ensure_bucket(self) -> None:
        try:
            await self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            await self.s3.create_bucket(Bucket=self.bucket)

    async def put_html(self, body: str) -> str:
        sha = content_hash(body)
        key = key_for(sha)
        try:
            await self.s3.head_object(Bucket=self.bucket, Key=key)
            return sha  # already stored: identical content seen before
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                raise
        await self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=gzip.compress(body.encode()),
            ContentType="text/html",
            ContentEncoding="gzip",
        )
        self.writes += 1
        return sha

    async def get_html(self, sha: str) -> str:
        obj = await self.s3.get_object(Bucket=self.bucket, Key=key_for(sha))
        async with obj["Body"] as stream:
            return gzip.decompress(await stream.read()).decode()
