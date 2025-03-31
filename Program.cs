//using Parquet.Schema;
//using Parquet;
//using Parquet.Data;
using System.Collections;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using System.Text;
using Humanizer;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using System.Diagnostics;
using System;
using NAudio.Wave;
//using Parquet.Meta;
using System.Xml.Linq;
using ParquetSharp.Schema;
using ParquetSharp;
using System.Collections.Generic;
using Parquet;
using Parquet.Schema;
using Parquet.Data;

namespace ConsoleApp1
{
    public class AudioStruct
    {
        public byte[] array;
        public string path;
        public int sampling_rate;
        public float duration;

        public AudioStruct(byte[] array, string path, int sampling_rate, float duration)
        {
            this.array = array;
            this.path = path;
            this.sampling_rate = sampling_rate;
            this.duration = duration;
        }
    }
    public static class ParquetSchemaHelpers
    {
        // Creates a properly annotated Parquet LIST node
        public static GroupNode CreateListNode(string name, Node elementNode)
        {
            return new GroupNode(
                name,
                Repetition.Optional,
                new[]
                {
                new GroupNode(
                    "list",
                    Repetition.Repeated,
                    new[] { new PrimitiveNode("element", Repetition.Required, LogicalType.None(), PhysicalType.Float) },
                    LogicalType.None()
                )
                },
                LogicalType.List()
            );
        }
    }

    internal class Program
    {
        private static string csvPath = "G:\\..\\metaall.csv";
        //AppDomain.CurrentDomain.BaseDirectory + "metaall.csv"; // format:  file_id | text
        private static string audioFolder = "G:\\..\\validated";
        //AppDomain.CurrentDomain.BaseDirectory + "audio\\"; //   file_id.wav , ...
        private static string parquetOutput = "G:\\...\\test2.parquet";
        //AppDomain.CurrentDomain.BaseDirectory + "test.parquet";

        static void Main(string[] args)
        {
            try
            {


                //ParquetParser(args);
                var writerOk = ParquetWriters(args).Result;
            }
            catch (Exception ex)
            {


            }
        }


        private async static Task<bool> ParquetWriters(string[] args)
        {
            var texts = new List<string>();
            var audioArrays = new List<byte[]>();
            var audioPaths = new List<string>();
            var samplingRates = new List<int>();
            var durations = new List<float>();

            var alllines = File.ReadLines(csvPath).ToList().Take(100);

            foreach (var line in alllines)
            {
                var parts = line.Split('|');
                if (parts.Length != 2) continue;

                var fileNumber = parts[0].Trim();
                var text = parts[1].Trim();
                var audioPath = Path.Combine(audioFolder, $"{fileNumber}.wav");

                if (!File.Exists(audioPath))
                {
                    Console.WriteLine($"Missing: {audioPath}");
                    continue;
                }

                try
                {
                    var bytes = File.ReadAllBytes(audioPath);

                    int samplingRate;
                    float duration;
                    //var bytes = ReadWavAsFloatList(audioPath, out samplingRate, out duration);
                    GetAudioMetadata(audioPath, out samplingRate, out duration);

                    texts.Add(text);
                    audioArrays.Add(bytes);
                    audioPaths.Add(audioPath);
                    samplingRates.Add(samplingRate);
                    durations.Add(duration);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error reading {audioPath}: {e.Message}");
                }
            }

            // Define schema
            var textField = new PrimitiveNode("text", Repetition.Required, LogicalType.String(), PhysicalType.ByteArray);


            var arrayField = new PrimitiveNode("bytes", Repetition.Required, LogicalType.None(), PhysicalType.ByteArray);
            var pathField = new PrimitiveNode("path", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray);
            var samplingRateField = new PrimitiveNode("sampling_rate", Repetition.Optional, LogicalType.Null(), PhysicalType.Int32);
            var durationField = new PrimitiveNode("duration", Repetition.Optional, LogicalType.None(), PhysicalType.Float);

            var audioGroup = new GroupNode("audio", Repetition.Required, new Node[]
            {
            arrayField,
            pathField,
            samplingRateField,
            durationField
            });


            var schema = new GroupNode("schema", Repetition.Required, new Node[]
            {
            textField,
            audioGroup
            });

            // Write Parquet file
            var writerProperties = new WriterPropertiesBuilder().Compression(Compression.Snappy).Build();
            using var fileWriter = new ParquetFileWriter(parquetOutput, schema, writerProperties);
            using var rowGroupWriter = fileWriter.AppendRowGroup();

            using var textWriter = rowGroupWriter.NextColumn().LogicalWriter<string>();
            textWriter.WriteBatch(texts.ToArray());
            using var arrayWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<byte[]>>();
            arrayWriter.WriteBatch(WrapNested(audioArrays));

            using var pathWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<string>>();
            pathWriter.WriteBatch(WrapNested(audioPaths));
            using var srWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<int?>>();
            srWriter.WriteBatch(WrapNestedNullable(samplingRates));
            using var durWriter = rowGroupWriter.NextColumn().LogicalWriter<Nested<float?>>();
            durWriter.WriteBatch(WrapNestedNullable(durations));



            fileWriter.Close();

            Console.WriteLine("✅ Parquet file written successfully with raw audio bytes.");


            return true;

        }

        static byte[][] convertListToArray(List<byte[]> byteArrayList)
        {
            byte[][] result = new byte[byteArrayList.Count()][];
            for (int i = 0; i < byteArrayList.Count(); i++)
            {
                result[i] = byteArrayList.ToArray()[i];
            }
            return result;
        }
        static Nested<T?>[] WrapNestedNullable<T>(List<T> values) where T : struct
        {
            var result = new Nested<T?>[values.Count];
            for (int i = 0; i < values.Count; i++)
            {
                result[i] = new Nested<T?>(values[i]);
            }
            return result;
        }

        static Nested<T>[] WrapNested<T>(List<T>? values)
        {
            if (values == null) return new Nested<T>[0];
            var result = new Nested<T>[values.Count];
            for (int i = 0; i < values.Count; i++)
            {
                result[i] = new Nested<T>(values[i]);
            }
            return result;
        }
        static void GetAudioMetadata(string path, out int samplingRate, out float duration)
        {
            using var reader = new AudioFileReader(path);
            samplingRate = reader.WaveFormat.SampleRate;
            duration = (float)reader.TotalTime.TotalSeconds;
        }

        static List<float> ReadWavAsFloatList(string path, out int samplingRate, out float duration)
        {
            using var reader = new AudioFileReader(path);
            samplingRate = reader.WaveFormat.SampleRate;
            duration = (float)reader.TotalTime.TotalSeconds;

            var samples = new List<float>();
            float[] buffer = new float[samplingRate * reader.WaveFormat.Channels]; // buffer for 1 second of audio
            int read;
            while ((read = reader.Read(buffer, 0, buffer.Length)) > 0)
            {
                for (int i = 0; i < read; i++)
                    samples.Add(buffer[i]);
            }
            return samples;
        }



        private static void ParquetParser(string[] args)
        {
            Console.WriteLine("Hello, World!");
            var datapath = "G:\\parquet";
            var transpath = "G:\\parquet";
            if (!Directory.Exists(datapath))
            {
                System.IO.Directory.CreateDirectory(datapath);
            }
            if (!Directory.Exists(transpath))
            {
                System.IO.Directory.CreateDirectory(transpath);
            }

            var fcnt = 0;
            foreach (var file in Directory.GetFiles("G:\\OPENAI\\oprheus_train_dataset\\", "trainorg.parquet"))
            {

                using (Stream fs = System.IO.File.OpenRead(file))
                {
                    using (ParquetReader reader = ParquetReader.CreateAsync(fs).Result)
                    {
                        var rwgr = 0;
                        for (int i = 0; i < reader.RowGroupCount; i++)
                        {
                            using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i))
                            {


                                DataField columnAudioData = reader.Schema.GetDataFields()[1];
                                DataColumn columnAData = rowGroupReader.ReadColumnAsync(columnAudioData).Result;

                                DataField columnTranscribe = reader.Schema.GetDataFields()[0];
                                DataColumn columnTData = rowGroupReader.ReadColumnAsync(columnTranscribe).Result;

                                var bytearrayA = (byte[][])columnAData.Data;
                                var fnameArr = columnTData.Data;


                                foreach (var bt in bytearrayA)
                                {

                                    System.IO.File.WriteAllBytes(datapath + "\\sound_" + fcnt + "_.wav", (byte[])bt);
                                    System.IO.File.WriteAllText(transpath + "\\sound_" + fcnt + "_.txt", fnameArr.GetValue(fcnt - rwgr).ToString());
                                    fcnt++;
                                }

                                //foreach (var tr in fnameArr)
                                //{

                                //    System.IO.File.WriteAllText(transpath + "\\sound_" + fcnt + "_.txt", tr.ToString());

                                //}



                            }

                            rwgr = rwgr + 100;
                        }
                    }
                }
                fcnt++;
            }
        }


    }
}
