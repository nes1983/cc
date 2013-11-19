using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace PLINQ
{
    /// <summary>
    /// Performs a wordcount benchmark with PLINQ means. 
    /// Data is ~250mb of textbooks from Project Gutenberg - http://www.gutenberg.org/
    /// </summary>
    public static class WordCountBenchmark
    {
        private const int TIMES = 5;

        private sealed class WordCount
        {
            internal WordCount(string word, int count)
            {
                Word = word;
                Count = count;
            }

            public string Word { get; private set; }
            public int Count { get; private set; }

            public override string ToString()
            {
                return string.Format("{0}: {1}", Word, Count);
            }
        }

        static void Main(string[] args)
        {
            const string input = @"..\data\";
            const int iterations = 1;

            var stopwatch = new Stopwatch();
            var timings = new double[TIMES];
            
            for (int i = 0; i < timings.Length; i++)
            {
                stopwatch.Reset();
                stopwatch.Start();

                for (int j = 0; j < iterations; j++)
                {
                    var wordCounts = TestWordCount(input);
                    WordCount[] materialized = wordCounts.ToArray();
                    if (materialized.Length == 0)
                    {
                        Console.WriteLine();
                    }
                }

                stopwatch.Stop();
                timings[i] = stopwatch.Elapsed.TotalMilliseconds;
                Console.WriteLine(timings[i]);
            }
            Console.WriteLine("Median: {0}", Median(timings));
            Console.WriteLine("Min: {0}", Min(timings));
            Console.ReadLine();
        }

        private static IEnumerable<WordCount> TestWordCount(string dir)
        {
            var books = Directory.EnumerateFiles(dir).Select(File.ReadAllText).AsParallel(); // PLINQ entry point.
            return books
                .SelectMany(WordParser)
                .GroupBy(word => word)
                .SelectMany(WordCounter);
        }

        private static IEnumerable<WordCount> WordParser(string book)
        {
            var wordCountHash = new Dictionary<string, int>();

            foreach (string word in Regex.Split(book, @"\s"))
            {
                if (wordCountHash.ContainsKey(word))
                {
                    wordCountHash[word]++;
                }
                else
                {
                    wordCountHash.Add(word, 1);
                }
            }

            return wordCountHash.Select(kv => new WordCount(kv.Key, kv.Value));
        }

        private static IEnumerable<WordCount> WordCounter(IGrouping<WordCount, WordCount> group)
        {
            var ret = new List<WordCount>();
            int count = 0;

            foreach (var wc in group)
            {
                count += wc.Count;
            }

            ret.Add(new WordCount(group.Key.Word, count));
            return ret;
        }

        private static double Median(double[] d)
        {
            Array.Sort(d);
            return d[d.Length / 2];
        }

        private static double Min(double[] d)
        {
            if (d == null || d.Length == 0)
            {
                throw new ArgumentException();
            }

            double min = d[0];
            for (int i = 1; i < d.Length; i++)
            {
                if (d[i] < min)
                {
                    min = d[i];
                }
            }

            return min;
        }
    }
}
