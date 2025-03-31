import os
import json
import pandas as pd
import soundfile as sf
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
from math import ceil

# === Configuration ===
#metadata_path = "metadata.txt"
#audio_dir = "audio_files"

metadata_path  = "G:\\...\\metaall.csv" # meta format  file_id|text
audio_dir  = "G:\\...\\audio"
#output_parquet = "orpheus_tr_bigarray.parquet"


output_prefix = "karay_orpheus_tr"  # Parquet prefix: dataset-00000.parquet
manifest_path = "dataset_manifest.json"
samples_per_shard = 10000

# === Load Metadata ===
texts = []
audio_bytes_list = []
audio_paths = []
sampling_rates = []

with open(metadata_path, "r", encoding="utf-8") as f:
    for line in tqdm(f, desc="Reading metadata"):
        line = line.strip()
        if not line or "|" not in line:
            continue

        file_id, text = line.split("|", 1)
        text = text.strip()
        audio_path = os.path.join(audio_dir, f"{file_id}.wav")

        if not os.path.exists(audio_path):
            print(f"Warning: Audio file not found for {file_id}")
            continue

        try:
            with open(audio_path, "rb") as af:
                audio_bytes = af.read()

            _, sr = sf.read(audio_path)

            texts.append(text)
            audio_bytes_list.append(audio_bytes)
            audio_paths.append(audio_path)
            sampling_rates.append(sr)

        except Exception as e:
            print(f"Error processing {file_id}: {e}")

# === Shard and Save Parquet Files ===
total_samples = len(texts)
num_shards = ceil(total_samples / samples_per_shard)
print(f"Saving {total_samples} records across {num_shards} shards...")

shard_files = []

for shard_idx in range(num_shards):
    start = shard_idx * samples_per_shard
    end = min((shard_idx + 1) * samples_per_shard, total_samples)

    shard_texts = texts[start:end]
    shard_audio_bytes = audio_bytes_list[start:end]
    shard_audio_paths = audio_paths[start:end]
    shard_sampling_rates = sampling_rates[start:end]

    audio_struct_array = pa.StructArray.from_arrays(
        [
            pa.array(shard_audio_bytes, type=pa.binary()),
            pa.array(shard_audio_paths, type=pa.string())
        ],
        fields=[
            pa.field("bytes", pa.binary()),
            pa.field("path", pa.string())
        ]
    )

    table = pa.table({
        "text": pa.array(shard_texts, type=pa.string()),
        "audio": audio_struct_array,
        "sampling_rate": pa.array(shard_sampling_rates, type=pa.int32())
    })

    shard_filename = f"{output_prefix}-{shard_idx:05d}.parquet"
    pq.write_table(table, shard_filename)
    print(f"Written {shard_filename} with {end - start} samples.")

    shard_files.append({"file": shard_filename})

# === Write Manifest File ===
with open(manifest_path, "w", encoding="utf-8") as f:
    json.dump({"files": shard_files}, f, indent=2)

print(f"Manifest written to {manifest_path}")
