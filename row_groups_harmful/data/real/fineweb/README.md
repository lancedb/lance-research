# FineWeb Dataset

## Overview
FineWeb is a large-scale, high-quality web text dataset created by HuggingFace containing over 15 trillion tokens of cleaned and deduplicated English web data. It is derived from 96 CommonCrawl snapshots spanning from summer 2013 to April 2024.

## Key Characteristics
- **Scale**: 15+ trillion tokens (GPT-2 tokenizer)
- **Source**: 96 CommonCrawl dumps processed and filtered
- **Language**: Primarily English
- **Quality**: Extensively filtered and deduplicated using the datatrove library

## Data Processing
The dataset underwent comprehensive filtering including:
- URL filtering to remove malicious and NSFW websites
- Trafilatura text extraction from raw HTML
- FastText language filtering (English score ≥ 0.65)
- Deduplication and quality filtering

## Performance
FineWeb outperforms other open pretraining datasets including C4, Dolma-v1.6, The Pile, SlimPajama, and RedPajama when used for LLM pretraining.

## License
Released under ODC-By 1.0 license.

## File in this directory
- `data_CC-MAIN-2024-51_000_00000.parquet`: Sample file from the FineWeb dataset containing 1,004,971 rows of web text data.

## File Statistics
- **Number of rows**: 1,004,971
- **Number of columns**: 9
- **File size**: 2.15 GB (2,152,578,441 bytes)
- **Average compressed size per row**: 2,141.93 bytes