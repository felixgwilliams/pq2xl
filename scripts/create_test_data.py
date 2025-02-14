# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "numpy>=2,<3",
#     "polars>=1,<2",
# ]
# ///
from  pathlib import Path

import numpy as np
import polars as pl

LOREM_WORDS = (
    "ad",
    "adipiscing",
    "aliqua",
    "aliquip",
    "amet",
    "anim",
    "aute",
    "cillum",
    "commodo",
    "consectetur",
    "consequat",
    "culpa",
    "cupidatat",
    "deserunt",
    "do",
    "dolor",
    "dolore",
    "duis",
    "ea",
    "eiusmod",
    "elit",
    "enim",
    "esse",
    "est",
    "et",
    "eu",
    "ex",
    "excepteur",
    "exercitation",
    "fugiat",
    "id",
    "in",
    "incididunt",
    "ipsum",
    "irure",
    "labore",
    "laboris",
    "laborum",
    "lorem",
    "magna",
    "minim",
    "mollit",
    "nisi",
    "non",
    "nostrud",
    "nulla",
    "occaecat",
    "officia",
    "pariatur",
    "proident",
    "qui",
    "quis",
    "reprehenderit",
    "sed",
    "sint",
    "sit",
    "sunt",
    "tempor",
    "ullamco",
    "ut",
    "velit",
    "veniam",
    "voluptate",
)


def main() -> None:
    gen = np.random.default_rng(seed=123)
    n = 1_000_000
    randints = gen.integers(-(1 << 31), 1 << 31, size=n)
    randints2 = gen.integers(-(1 << 31), 1 << 31, size=n)
    randints3 = gen.integers(-(1 << 31), 1 << 31, size=n)
    randints4 = gen.integers(-(1 << 31), 1 << 31, size=n)
    randfloats = gen.uniform(size=n)
    randbools = gen.choice((True, False), replace=True, size=n)
    randwords = gen.choice(LOREM_WORDS, replace=True, size=n)
    df = pl.DataFrame(
        {
            "integers": randints,
            "strings": randwords,
            "floats": randfloats,
            "datetimes_us": randints2,
            "dates": randints3,
            "times": randints4,
            "bools": randbools,
        }
    )
    df = df.with_columns(
        pl.from_epoch(pl.col("datetimes_us")),
        pl.from_epoch(pl.col("dates")).dt.date(),
        pl.from_epoch(pl.col("times")).dt.time(),
        pl.col("strings").cast(pl.Categorical).alias("categorical"),
    )
    print(df)
    Path("data").mkdir(exist_ok=True)
    df.write_parquet("data/example_parquet.parquet")


if __name__ == "__main__":
    main()
