#my_original_dataset_name = "canopylabs/zac-sample-dataset"
#import locale
#locale.getpreferredencoding = lambda: "UTF-8"
#!pip install datasets
#!pip install snac
#from torch import torch
from snac import SNAC
from datasets import load_dataset
from huggingface_hub import snapshot_download

## CHANGE TO YOUR NAMESPACE
name_to_push_dataset_to = "Karayakar/karayTurkish"

ds = load_dataset("parquet",data_files="G:\\OPENAI\\Turkish_VoiceDatasets\\PARQUET\\Orpheus_TTS_KA_60HRS_24000Khz.parquet", split="train")

#ds = load_dataset("Karayakar/OrpheusTTS100K", split="train")
ds_sample_rate =24000# ds[0]["audio"]["sampling_rate"]

model = SNAC.from_pretrained("hubertsiuzdak/snac_24khz")
model = model.to("cuda")

import torchaudio.transforms as T
def tokenise_audio(waveform):
  waveform = torch.from_numpy(waveform).unsqueeze(0)
  waveform = waveform.to(dtype=torch.float32)
  resample_transform = T.Resample(orig_freq=ds_sample_rate, new_freq=24000)
  waveform = resample_transform(waveform)

  waveform = waveform.unsqueeze(0).to("cuda")

  #generate the codes from snac
  with torch.inference_mode():
    codes = model.encode(waveform)

  all_codes = []
  for i in range(codes[0].shape[1]):
    all_codes.append(codes[0][0][i].item()+128266)
    all_codes.append(codes[1][0][2*i].item()+128266+4096)
    all_codes.append(codes[2][0][4*i].item()+128266+(2*4096))
    all_codes.append(codes[2][0][(4*i)+1].item()+128266+(3*4096))
    all_codes.append(codes[1][0][(2*i)+1].item()+128266+(4*4096))
    all_codes.append(codes[2][0][(4*i)+2].item()+128266+(5*4096))
    all_codes.append(codes[2][0][(4*i)+3].item()+128266+(6*4096))


  return all_codes

import torch
import io
import soundfile as sf
import random

def add_codes(example):
    codes_list = None
    try:
        audio_bytes = example.get("audio")  # Adjusted to match flat column name
        if audio_bytes:
            # Debug: check the type
            if isinstance(audio_bytes, str):
                print("audio_bytes is str, trying to decode...")
                try:
                    # Try decoding assuming it was encoded as latin1 during save
                    audio_bytes = audio_bytes.encode("latin1")
                except Exception as e:
                    print(f"Failed to encode string to bytes: {e}")
                    return example

            elif not isinstance(audio_bytes, (bytes, bytearray)):
                print(f"Unsupported type for audio_bytes: {type(audio_bytes)}")
                return example

            with io.BytesIO(audio_bytes) as audio_buf:
                audio_array, _ = sf.read(audio_buf)
            codes_list = tokenise_audio(audio_array)

    except Exception as e:
        print(f"Skipping row due to error: {e}")

    example["codes_list"] = codes_list
    return example

ds = ds.map(add_codes, remove_columns=["audio"])


tokeniser_length = 128256
start_of_text = 128000
end_of_text = 128009

start_of_speech = tokeniser_length + 1
end_of_speech = tokeniser_length + 2

start_of_human = tokeniser_length + 3
end_of_human = tokeniser_length + 4

start_of_ai = tokeniser_length + 5
end_of_ai =  tokeniser_length + 6
pad_token = tokeniser_length + 7

audio_tokens_start = tokeniser_length + 10

tokenizer_name = "canopylabs/orpheus-3b-0.1-pretrained"

from transformers import AutoTokenizer
import os
tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
num_proc = 1#os.cpu_count() - 2

ds = ds.filter(lambda x: x["codes_list"] is not None)
ds = ds.filter(lambda x: len(x["codes_list"]) > 0)

def remove_duplicate_frames(example):
    vals = example["codes_list"]
    if len(vals) % 7 != 0:
        raise ValueError("Input list length must be divisible by 7")

    result = vals[:7]

    removed_frames = 0

    for i in range(7, len(vals), 7):
        current_first = vals[i]
        previous_first = result[-7]

        if current_first != previous_first:
            result.extend(vals[i:i+7])
        else:
            removed_frames += 1

    example["codes_list"] = result

    return example

ds = ds.map(remove_duplicate_frames, num_proc=num_proc)
#ds = ds.map(remove_duplicate_frames)
tok_info = '''*** HERE you can modify the text prompt
i.e. if you wanted a multispeaker model like canopylabs/orpheus-3b-0.1-ft, you can pass:
f"{example["source"]}:  {example["text"]}", as is passed.
'''
print(tok_info)

def create_input_ids(example):
    text_ids = tokenizer.encode(example["text"],  add_special_tokens=True)
    text_ids.append(end_of_text)
    example["text_tokens"] = text_ids
    input_ids = (
        [start_of_human]
        + example["text_tokens"]
        + [end_of_human]
        + [start_of_ai]
        + [start_of_speech]
        + example["codes_list"]
        + [end_of_speech]
        + [end_of_ai]
    )
    example["input_ids"] = input_ids
    example["labels"] = input_ids
    example["attention_mask"] = [1] * len(input_ids)

    return example

ds = ds.map(create_input_ids, num_proc=num_proc, remove_columns=["text", "codes_list"])
#ds = ds.map(create_input_ids,   remove_columns=["text", "codes_list"])



#@title Remove unnecessary columns
columns_to_keep = ["input_ids", "labels", "attention_mask"]
columns_to_remove = [col for col in ds.column_names if col not in columns_to_keep]

ds = ds.remove_columns(columns_to_remove)

ds.save_to_disk("G:\\OPENAI\\oprheus_train_dataset\\Orpheus_TTS_KA_60HRS_24000Khz")
#ds.push_to_hub(name_to_push_dataset_to)
