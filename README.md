This console app and python scripts for creating dataset for Orpheus TTS.


https://github.com/canopyai/Orpheus-TTS




First you need to prepare metadata file ,

**meta**.csv
1 | text of audio 1
2 | text of audio 2
.........

**audio** folder
1.wav
2.wav

Run one of the below scripts to create parquet file.

in the scripts adjust the folders and parquet file name.

metadata_path = "G:\..\**meta**.csv"

audio_dir = "G:\..\**audio**"

output_parquet = "**orpheus_tr_10k**.parquet"


https://github.com/karayakar/ParquetDatasetCreator/blob/master/pythonScripts/parquet_createData2.py
or
https://github.com/karayakar/ParquetDatasetCreator/blob/master/pythonScripts/parquet_createDataNoShard.py
or
you can run console app, .net to create parquet file.

Then run ;
https://github.com/karayakar/ParquetDatasetCreator/blob/master/pythonScripts/createdataset.py
change the name of parquet file in the script.

ds = load_dataset("parquet",data_files="G:\...\**orpheus_tr_10k**.parquet", split="train")

....


ds.save_to_disk("G:\....\**testKarayData**") <-- change this folder name

script will save dataset
