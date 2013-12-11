using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace PLINQ
{
/** Support Vector Machine. */
public class SVM {
	/** Implements the simple vector logic where each component is a real number. */

    public class RealVector {
		internal readonly double[] w;

		public RealVector(double[] w) {
			this.w = w;
		}

		public RealVector(RealVector other) {
			w = new double[other.GetDimension()];

			for (int i = 0; i < w.Length; i++) {
				w[i] = other.w[i];
			}
		}

		public RealVector(int dim) {
			w = new double[dim];
		}

		private RealVector(double fill, int count) {
			w = new double[count];

			for (int i = 0; i < count; i++) {
				w[i] = fill;
			}
		}

		/** Adds a vector to this vector. Assumes equal dimensions. */
		public void Add(RealVector other) {
			double[] u = other.GetFeatures();
			for (int i = 0; i < u.Length; ++i) {
				w[i] += u[i];
			}
		}

		public RealVector Add(double value) {
			Add(new RealVector(value, GetDimension()));
			return this;
		}

		/** Subtracts a vector from this vector. Assumes equal dimensions. */
		public void Subtract(RealVector other) {
			double[] u = other.GetFeatures();
			for (int i = 0; i < u.Length; ++i) {
				w[i] -= u[i];
			}
		}

		/** Dot-product between two vectors. Assumes equal dimensions. */
		public double DotProduct(RealVector other) {
			double result = 0.0;
			double[] u = other.GetFeatures();
			for (int i = 0; i < u.Length; ++i) {
				result += u[i] * this.w[i];
			}
			return result;
		}

		/** Scales the coefficients of this vector by some real factor. */
		public RealVector ScaleThis(double factor) {
			for (int i = 0; i < this.w.Length; ++i) {
				this.w[i] *= factor;
			}
			return this;
		}

		/**  L2 norm of the vector. */
		public double GetNorm() {
			return Math.Sqrt(DotProduct(this));
		}

		public double[] GetFeatures() {
			return this.w;
		}

		public int GetDimension() {
			return this.w.Length;
		}


		public override String ToString() {
			var sb = new StringBuilder();
			for (int i = 0; i < this.w.Length; ++i) {
				sb.Append(w[i] + " ");
			}
			return sb.ToString();
		}

		public double Average() {
			double ret = 0;

			for (int i = 0; i < w.Length; i++) {
				ret += w[i];
			}

			int total = Math.Max(1, w.Length);
			return ret / total;
		}
	}

	/** Represents a training instance. */

    public class TrainingInstance {
		private readonly RealVector features;
		private readonly int label;

		public TrainingInstance(RealVector features, int label) {
			this.features = features;
			this.label = label;
		}

		/**
		 * Instantiates the training instance from a string.
		 * Supposes that the instance is given as a series of doubles and
		 * that the last element is the label. To avoid precision problems,
		 * the label is considered 1 if the last coefficient is > 0.5, -1 otherwise.
		 */
		public TrainingInstance(String s)
		{
		    List<double> parsedInput = s.Split(' ').Select(Double.Parse).ToList();

		    // Last element is always the label.
			int n = parsedInput.Count - 1;

			// Convert the tokens to feature vector and label.
			double [] coef = new double[n];
			int cnt = 0;

			foreach (Double c in parsedInput) {
				if (cnt < n) {
					coef[cnt++] = c;
				} else {
					this.label = c > 0.5 ? 1 : -1;
				}
			}

			features = new RealVector(coef);
		}

		public RealVector GetFeatures() {
			return features;
		}

		public int GetLabel() {
			return label;
		}

		public int GetFeatureCount() {
			return features.GetDimension();
		}
	}

	 class BatchSVM {
		//some reasonable start points, gathered from simpler svm classifier
		private readonly double[] defaultWeights = new[] 	{
				-1.1466,   -2.2231,   -0.2428 ,  -0.8678,    2.0886,   0.0985,
				-2.0055,   -0.0099,   -0.0609,   -0.9270,   -1.5213,    0.5135,
				-0.2026,   -0.4987,    0.5676,   -0.8285,    0.6187,   -0.0617,
				-0.2748,   -2.0780,   -1.1740,    2.2460,    0.8605,   -0.1293,
				-0.0787,    1.1157,   -0.4567,    1.1253,   -2.4983,    0.6353,
				 1.4149,   -0.7522,    0.8811,   -2.4835,    4.6290,   -0.8594
		};

		private readonly List<TrainingInstance> trainingSet;
	    readonly double kSmall;
	    readonly int subsampleSize;
	    private const double epsilon = 0.0001;
	    readonly Random rand = new Random();

		public BatchSVM( List<TrainingInstance> trainingSet) {
			this.trainingSet = trainingSet;

			kSmall = 0.02;
			subsampleSize = (int)Math.Round(kSmall * trainingSet.Count);
		}

		public RealVector Train(int maxIter) {
			var w = new RealVector(defaultWeights);

			for (int i = 0; i < maxIter; i++) {
				TrainingInstance[] batch = CreateBatch(SelectSubset(subsampleSize), w);
				RealVector grad = BatchGradient(LogisticLoss(batch, w), batch);

				w.Subtract(grad);

				if(grad.GetNorm() < epsilon) {
					break;
				}
			}

			return w;
		}

		private TrainingInstance[] CreateBatch(TrainingInstance[] subset, RealVector weights) {
			TrainingInstance[] misclassified = Misclassified(subset, weights);
			var ret = new TrainingInstance[subset.Length + 3* misclassified.Length];

			for (int i = 0; i < ret.Length; i++) {
				if (i < subset.Length) {
					ret[i] = subset[i];
				} else {
					ret[i] = misclassified[(i - subset.Length) % misclassified.Length];
				}
			}

			return ret;
		}

		/** Calculates the logloss for every object in batch. */
		private static double[] LogisticLoss(TrainingInstance[] batch, RealVector weights)
		{
			double[] result = new double[batch.Length];
			double[] raw = new double[batch.Length];

			for (int i = 0; i < batch.Length; i++) {
				RealVector cur = batch[i].GetFeatures();
				raw[i] = cur.DotProduct(weights);
			}

			for (int i = 0; i < result.Length; i++) {
				double exp = Math.Exp(raw[i]);
				result[i] = exp / (1 + exp);
			}

			return (result);
		}

		private static RealVector BatchGradient(double[] logloss, TrainingInstance[] batch)
		{
			int dimensions = batch[0].GetFeatureCount();
			int batchSize = batch.Length;

			RealVector toReplicate =  new RealVector(logloss);

			RealVector labels = new RealVector(batchSize);

			for (int i = 0 ; i < batchSize; i++)
			{
				labels.GetFeatures()[i] = batch[i].GetLabel();
			}

			labels.Add(1).ScaleThis(0.5);
			toReplicate.Subtract(labels);

			RealVector[] repmat = new RealVector[dimensions];
			for (int i = 0; i < dimensions; i++) {
				repmat[i] = new RealVector(toReplicate);
			}

			for (int i = 0; i < dimensions; i++) {
				for(int j = 0; j < batchSize; j++) {
					repmat[i].w[j] *= batch[j].GetFeatures().w[i];
				}
			}

			RealVector result = new RealVector(dimensions);

			for (int i = 0; i < dimensions;i++) {
				result.w[i] = repmat[i].Average();
			}
			return result;
		}

		private TrainingInstance[] Misclassified(IEnumerable<TrainingInstance> subset, RealVector weights) {
			var result = new List<TrainingInstance>();

			foreach (TrainingInstance ti in subset) {
				int predictedClass = classify(ti, weights);

				if ((predictedClass == +1) && (ti.GetLabel() == -1)) {
					result.Add(ti);
				}
			}

			return result.ToArray();
		}

		private int classify(TrainingInstance ti, RealVector w) {
			RealVector features = ti.GetFeatures();
			double result = features.DotProduct(w);

			if (result >= 0) {
				return 1;
			}
			return -1;
		 }

		private TrainingInstance[] SelectSubset(int k) {
			var result = new TrainingInstance[k];
			var visited = new HashSet<int>();

			for (int i = 0; i < k; i++) {
				while (true)
				{
				    int nextIdx = rand.Next(0, k);

					if (visited.Add(nextIdx)) {
						result[i] = trainingSet[nextIdx];
						break;
					}
				}
			}

			return result;
		}
	}

	// Hyperplane weights.
    readonly RealVector weights;

	private SVM(RealVector weights) {
		this.weights = weights;
	}

	/** Trains SVM with a list of training instances, and with given maximum number of iterations. */

    public static SVM TrainSVM(List<TrainingInstance> trainingSet, int iterations) {
		return new SVM(new BatchSVM(trainingSet).Train(iterations));
	}

	/** Instantiates SVM from weights given as a string. */
	SVM(String input) {
		var parsedInput = input.Split(' ').Select(Double.Parse).ToList();

		double[] w = new double[parsedInput.Count];
		int cnt = 0;
		foreach (double coef in parsedInput) {
			w[cnt++] = coef;
		}

		this.weights = new RealVector(w);
	}

	/** Instantiates the SVM model as the average model of the input SVMs. */
	SVM(List<SVM> svmList) {
		int dim = svmList[0].GetDimension();
		var w = new RealVector(dim);

		foreach (SVM svm in svmList) {
			w.Add(svm.getWeights());
		}

		this.weights = w.ScaleThis(1.0 / svmList.Count);
	}

	int GetDimension() {
		return weights.GetDimension();
	}

	/** Given a training instance it returns the result of sign(weights'instanceFeatures). */
	int Classify(TrainingInstance ti) {
		RealVector features = ti.GetFeatures();
		double result = features.DotProduct(weights);
		if (result >= 0) {
			return 1;
		}
		return -1;
	}

	RealVector getWeights() {
		return this.weights;
	}

	
	public override String ToString() {
		return weights.ToString();
	}
}
}