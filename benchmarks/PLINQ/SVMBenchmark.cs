using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace PLINQ
{
    public class SVMBenchmark
    {
        private const int TIMES = 5;

        private sealed class TrainingInstanceLine 
        {
		    public TrainingInstanceLine(String line, int shard) {
			    Shard = shard;
			    Line = line;
		    }

            public String Line { get; private set; }
            public int Shard { get; private set; }
	    }

		static void Main(string[] args)
		{
			const string input = @"..\svmdata\";
			const int iterations = 1;

			var stopwatch = new Stopwatch();
			var timings = new double[TIMES];
			
			for (int i = 0; i < timings.Length; i++)
			{
				stopwatch.Reset();
				stopwatch.Start();

				for (int j = 0; j < iterations; j++)
				{
					var svms = TestSVMTrain(input);
					String[] materialized = svms.ToArray();
					if (materialized.Length == 0) // to avoid too eager clr optimizations
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

		private static IEnumerable<String> TestSVMTrain(string dir)
		{
			var files = Directory.EnumerateFiles(dir).Select(File.ReadAllText).AsParallel(); // PLINQ entry point.
			return files
				.SelectMany(DataDistributor)
				.GroupBy(word => word.Shard)
				.SelectMany(SVMTrainer);
		}

		private static IEnumerable<TrainingInstanceLine> DataDistributor(string fileContent)
		{
		    const int outputs = 80;
			int i = 0;

            var ret = new List<TrainingInstanceLine>();
            var sr = new StringReader(fileContent);
		    String line;  
		    while ((line = sr.ReadLine()) != null)
		    {
		        if (i == outputs) 
                {
                    i = 0;
                }

                ret.Add(new TrainingInstanceLine(line, i));
                i++;
            }

		    return ret;
		}

		private static IEnumerable<String> SVMTrainer(IGrouping<int, TrainingInstanceLine> group)
		{
			var ret = new List<String>();

			var trainingSet = new List<SVM.TrainingInstance>();

			foreach (var line in group) {
				var instance = new SVM.TrainingInstance(line.Line);
				trainingSet.Add(instance);
			}

			SVM model = SVM.TrainSVM(trainingSet, 10000);
			ret.Add(model.ToString());
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
