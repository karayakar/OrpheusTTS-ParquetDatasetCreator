## **📦 Orpheus-TTS Dataset Preparation**

This repository provides tools, including a console app and Python scripts, to create datasets for Orpheus TTS. Follow the steps below to prepare your dataset efficiently.

---

## Prerequisites

- **Metadata File:**  
  Prepare a `meta.csv` file with the following format:
  ```
  1 | text of audio 1
  2 | text of audio 2
  ...
  ```

- **Audio Files:**  
  Organize your audio files in the `audio` folder with filenames matching the metadata:
  ```
  1.wav
  2.wav
  ...
  ```

---

## Steps to Create the Parquet File

1. **Adjust Paths in Scripts:**  
   Modify the following paths in the chosen script:
   ```python
   metadata_path = \"G:..*meta*.csv\"
   audio_dir = \"G:..*audio*\"
   output_parquet = \"orpheus_tr_10k.parquet\"
   ```

2. **Run One of the Below Scripts:**  
   Use any of the scripts based on your preference:
   - [parquet_createData2.py](https://github.com/karayakar/ParquetDatasetCreator/blob/master/pythonScripts/parquet_createData2.py)
   - [parquet_createDataNoShard.py](https://github.com/karayakar/ParquetDatasetCreator/blob/master/pythonScripts/parquet_createDataNoShard.py)

   Alternatively, you can use the `.NET Console App` to create the Parquet file.

---

## Creating the Dataset

1. **Run the Dataset Creation Script:**  
   Use the [createdataset.py](https://github.com/karayakar/ParquetDatasetCreator/blob/master/pythonScripts/createdataset.py) script.  
   Update the script to reference the Parquet file:
   ```python
   ds = load_dataset(\"parquet\", data_files=\"G:...*orpheus_tr_10k*.parquet\", split=\"train\")
   ```

2. **Save the Dataset:**  
   Adjust the save path in the script:
   ```python
   ds.save_to_disk(\"G:....*testKarayData*\")
   ```

---

## Outputs

- **Parquet File:** `orpheus_tr_10k.parquet`
- **Saved Dataset Directory:** `testKarayData`

---

Feel free to contribute or report issues for improvements to the dataset creation process! 🚀
