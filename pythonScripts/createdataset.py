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


## CHANGE TO YOUR HUGGINGFACE TOKEN
#huggingface-cli login --token=""

#wandb login =""


#from datasets import load_dataset

#dsn = my_original_dataset_name
#
#snapshot_download(
#    repo_id=dsn,
#    repo_type="dataset",
#    revision="main",
#    max_workers=64,
#)


#import glob
#shard_files = sorted(glob.glob("karay_oprheus_tr-*.parquet"))
#ds = load_dataset("parquet", data_files={"train": shard_files}, split="train", num_proc=16)
#ds = load_dataset("parquet", data_files="G:\\...\\dataset_manifest.json", split="train", num_proc=16)
ds = load_dataset("parquet",data_files="G:\\...\\orpheus_tr_10k.parquet", split="train")

#ds = load_dataset("parquet", data_files={
#    "train": [
#        "karay_oprheus_tr-00000.parquet",
#        "karay_oprheus_tr-00001.parquet",
#        "karay_oprheus_tr-00002.parquet",
#        "karay_oprheus_tr-00003.parquet",
#        "karay_oprheus_tr-00004.parquet",
#        "karay_oprheus_tr-00005.parquet",
#        "karay_oprheus_tr-00006.parquet",
#        "karay_oprheus_tr-00007.parquet",
#        "karay_oprheus_tr-00008.parquet",
#        "karay_oprheus_tr-00009.parquet",
#        "karay_oprheus_tr-00010.parquet",
#        "karay_oprheus_tr-00011.parquet",
#        "karay_oprheus_tr-00012.parquet",
#        "karay_oprheus_tr-00013.parquet",
#        "karay_oprheus_tr-00014.parquet",
#        "karay_oprheus_tr-00015.parquet",
#        "karay_oprheus_tr-00016.parquet",
#        "karay_oprheus_tr-00017.parquet",
#        "karay_oprheus_tr-00018.parquet",
#        "karay_oprheus_tr-00019.parquet",
#        "karay_oprheus_tr-00020.parquet",
#        "karay_oprheus_tr-00021.parquet",
#        "karay_oprheus_tr-00022.parquet",
#        "karay_oprheus_tr-00023.parquet",
#        "karay_oprheus_tr-00024.parquet",
#        "karay_oprheus_tr-00025.parquet",
#        "karay_oprheus_tr-00026.parquet",
#        "karay_oprheus_tr-00027.parquet",
#        "karay_oprheus_tr-00028.parquet",
#        "karay_oprheus_tr-00029.parquet",
#        "karay_oprheus_tr-00030.parquet",
#        "karay_oprheus_tr-00031.parquet",
#        "karay_oprheus_tr-00032.parquet",
#        "karay_oprheus_tr-00033.parquet",
#        "karay_oprheus_tr-00034.parquet",
#        "karay_oprheus_tr-00035.parquet",
#        "karay_oprheus_tr-00036.parquet",
#        "karay_oprheus_tr-00037.parquet",
#        "karay_oprheus_tr-00038.parquet",
#        "karay_oprheus_tr-00039.parquet",
#        "karay_oprheus_tr-00040.parquet",
#        "karay_oprheus_tr-00041.parquet",
#        "karay_oprheus_tr-00042.parquet",
#        "karay_oprheus_tr-00043.parquet",
#        "karay_oprheus_tr-00044.parquet",
#        "karay_oprheus_tr-00045.parquet",
#        "karay_oprheus_tr-00046.parquet",
#        "karay_oprheus_tr-00047.parquet",
#        "karay_oprheus_tr-00048.parquet",
#        "karay_oprheus_tr-00049.parquet",
#        "karay_oprheus_tr-00050.parquet",
#        "karay_oprheus_tr-00051.parquet",
#        "karay_oprheus_tr-00052.parquet",
#        "karay_oprheus_tr-00053.parquet",
#        "karay_oprheus_tr-00054.parquet",
#        "karay_oprheus_tr-00055.parquet",
#        "karay_oprheus_tr-00056.parquet",
#        "karay_oprheus_tr-00057.parquet",
#        "karay_oprheus_tr-00058.parquet",
#        "karay_oprheus_tr-00059.parquet",
#        "karay_oprheus_tr-00060.parquet",
#        "karay_oprheus_tr-00061.parquet",
#        "karay_oprheus_tr-00062.parquet",
#        "karay_oprheus_tr-00063.parquet",
#        "karay_oprheus_tr-00064.parquet",
#        "karay_oprheus_tr-00065.parquet",
#        "karay_oprheus_tr-00066.parquet",
#        "karay_oprheus_tr-00067.parquet",
#        "karay_oprheus_tr-00068.parquet",
#        "karay_oprheus_tr-00069.parquet",
#        "karay_oprheus_tr-00070.parquet",
#        "karay_oprheus_tr-00071.parquet",
#        "karay_oprheus_tr-00072.parquet",
#        "karay_oprheus_tr-00073.parquet",
#        "karay_oprheus_tr-00074.parquet",
#        "karay_oprheus_tr-00075.parquet",
#        "karay_oprheus_tr-00076.parquet",
#        "karay_oprheus_tr-00077.parquet",
#        "karay_oprheus_tr-00078.parquet",
#        "karay_oprheus_tr-00079.parquet",
#        "karay_oprheus_tr-00080.parquet",
#        "karay_oprheus_tr-00081.parquet",
#        "karay_oprheus_tr-00082.parquet",
#        "karay_oprheus_tr-00083.parquet",
#        "karay_oprheus_tr-00084.parquet",
#        "karay_oprheus_tr-00085.parquet",
#        "karay_oprheus_tr-00086.parquet",
#        "karay_oprheus_tr-00087.parquet",
#        "karay_oprheus_tr-00088.parquet",
#        "karay_oprheus_tr-00089.parquet",
#        "karay_oprheus_tr-00090.parquet",
#        "karay_oprheus_tr-00091.parquet",
#        "karay_oprheus_tr-00092.parquet",
#        "karay_oprheus_tr-00093.parquet",
#        "karay_oprheus_tr-00094.parquet",
#        "karay_oprheus_tr-00095.parquet",
#        "karay_oprheus_tr-00096.parquet"]
#}, split="train", num_proc=40)




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
#def add_codes(example):
#    # Always initialize codes_list to None
#    codes_list = None
#
#    try:
#        answer_audio = example.get("audio")
#        # If there's a valid audio array, tokenise it
#        if answer_audio and "array" in answer_audio:
#            audio_array = answer_audio["array"]
#            codes_list = tokenise_audio(audio_array)
#    except Exception as e:
#        print(f"Skipping row due to error: {e}")
#        # Keep codes_list as None if we fail
#    example["codes_list"] = codes_list
#    print(codes_list)
#    return example

def add_codes(example):
    codes_list = None
    try:
        answer_audio = example.get("audio")
        if answer_audio and "bytes" in answer_audio:
            audio_bytes = answer_audio["bytes"]
            with io.BytesIO(audio_bytes) as audio_buf:
                audio_array, _ = sf.read(audio_buf)
            codes_list = tokenise_audio(audio_array)
    except Exception as e:
        print(f"Skipping row due to error: {e}")
    example["codes_list"] = codes_list
    print(codes_list)
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

ds.save_to_disk("G:\\....\\Karay_orpheus_tr_10k")
#ds.push_to_hub(name_to_push_dataset_to)
