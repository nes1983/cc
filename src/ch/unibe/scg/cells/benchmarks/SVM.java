package ch.unibe.scg.cells.benchmarks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

/** Support Vector Machine. */
public class SVM {
	/** Implements the simple vector logic where each component is a real number. */
	private static class RealVector {
		double[] w;

		RealVector(double[] w) {
			this.w = w;
		}

		RealVector(RealVector other) {
			this.w = new double[other.getDimension()];

			for (int i = 0; i < w.length; i++) {
				w[i] = other.w[i];
			}
		}

		RealVector(int dim) {
			this.w = new double[dim];
		}

		RealVector(double fill, int count) {
			this.w = new double[count];

			for (int i = 0; i < count; i++) {
				w[i] = fill;
			}
		}

		/** Adds a vector to this vector. Assumes equal dimensions. */
		void add(RealVector other) {
			double[] u = other.getFeatures();
			for (int i = 0; i < u.length; ++i) {
				w[i] += u[i];
			}
		}

		RealVector add(double value) {
			add(new RealVector(value, getDimension()));
			return this;
		}

		/** Subtracts a vector from this vector. Assumes equal dimensions. */
		void subtract(RealVector other) {
			double[] u = other.getFeatures();
			for (int i = 0; i < u.length; ++i) {
				w[i] -= u[i];
			}
		}

		/** Dot-product between two vectors. Assumes equal dimensions. */
		double dotProduct(RealVector other) {
			double result = 0.0;
			double[] u = other.getFeatures();
			for (int i = 0; i < u.length; ++i) {
				result += u[i] * this.w[i];
			}
			return result;
		}

		/** Scales the coefficients of this vector by some real factor. */
		RealVector scaleThis(double factor) {
			for (int i = 0; i < this.w.length; ++i) {
				this.w[i] *= factor;
			}
			return this;
		}

		/**  L2 norm of the vector. */
		double getNorm() {
			return Math.sqrt(dotProduct(this));
		}

		double[] getFeatures() {
			return this.w;
		}

		int getDimension() {
			return this.w.length;
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < this.w.length; ++i) {
				sb.append(w[i] + " ");
			}
			return sb.toString();
		}

		double average() {
			double ret = 0;

			for (int i = 0; i < w.length; i++) {
				ret += w[i];
			}

			int total = Math.max(1, w.length);
			return ret / total;
		}
	}

	/** Represents a training instance. */
	static class TrainingInstance {
		private RealVector features;
		private int label;

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
		TrainingInstance(String s) {

			List<Double> parsedInput = new LinkedList<>();
			try (Scanner sc = new Scanner(s)) {
				while (sc.hasNextDouble()) {
					parsedInput.add(sc.nextDouble());
				}
			}

			// Last element is always the label.
			int n = parsedInput.size() - 1;

			// Convert the tokens to feature vector and label.
			double [] coef = new double[n];
			int cnt = 0;

			for (Double c : parsedInput) {
				if (cnt < n) {
					coef[cnt++] = c;
				} else {
					this.label = c > 0.5 ? 1 : -1;
				}
			}

			this.features = new RealVector(coef);
		}

		RealVector getFeatures() {
			return features;
		}

		int getLabel() {
			return label;
		}

		int getFeatureCount() {
			return features.getDimension();
		}
	}

	static class BatchSVM {
		//some reasonable start points, gathered from simpler svm classifier
		final private double[] defaultWeights = new double[] 	{
				-1.1466,   -2.2231,   -0.2428 ,  -0.8678,    2.0886,   0.0985,
				-2.0055,   -0.0099,   -0.0609,   -0.9270,   -1.5213,    0.5135,
				-0.2026,   -0.4987,    0.5676,   -0.8285,    0.6187,   -0.0617,
				-0.2748,   -2.0780,   -1.1740,    2.2460,    0.8605,   -0.1293,
				-0.0787,    1.1157,   -0.4567,    1.1253,   -2.4983,    0.6353,
				 1.4149,   -0.7522,    0.8811,   -2.4835,    4.6290,   -0.8594
		};

		final private List<TrainingInstance> trainingSet;
		double kSmall;
		int subsampleSize;
		double epsilon = 0.0001;
		Random rand = new Random();

		BatchSVM( List<TrainingInstance> trainingSet) {
			this.trainingSet = trainingSet;

			kSmall = 0.02;
			subsampleSize = (int)Math.round(kSmall * trainingSet.size());
		}

		RealVector train(int maxIter) {
			RealVector w = new RealVector(defaultWeights);

			for (int i = 0; i < maxIter; i++) {
				TrainingInstance[] batch = createBatch(selectSubset(subsampleSize), w);
				RealVector grad = batchGradient(logisticLoss(batch, w), batch);

				w.subtract(grad);

				if(grad.getNorm() < epsilon) {
					break;
				}
			}

			return w;
		}

		private TrainingInstance[] createBatch(TrainingInstance[] subset, RealVector weights) {
			TrainingInstance[] misclassified = misclassified(subset, weights);
			TrainingInstance[] ret = new TrainingInstance[subset.length + 3* misclassified.length];

			for (int i = 0; i < ret.length; i++) {
				if (i < subset.length) {
					ret[i] = subset[i];
				} else {
					ret[i] = misclassified[(i - subset.length) % misclassified.length];
				}
			}

			return ret;
		}

		/** Calculates the logloss for every object in batch. */
		private double[] logisticLoss(TrainingInstance[] batch, RealVector weights)
		{
			double[] result = new double[batch.length];
			double[] raw = new double[batch.length];

			for (int i = 0; i < batch.length; i++) {
				RealVector cur = batch[i].getFeatures();
				raw[i] = cur.dotProduct(weights);
			}

			for (int i = 0; i < result.length; i++) {
				double exp = Math.exp(raw[i]);
				result[i] = exp / (1 + exp);
			}

			return (result);
		}

		private RealVector batchGradient(double[] logloss, TrainingInstance[] batch)
		{
			int dimensions = batch[0].getFeatureCount();
			int batchSize = batch.length;

			RealVector toReplicate =  new RealVector(logloss);

			RealVector labels = new RealVector(batchSize);

			for (int i = 0 ; i < batchSize; i++)
			{
				labels.getFeatures()[i] = batch[i].getLabel();
			}

			labels.add(1).scaleThis(0.5);
			toReplicate.subtract(labels);

			RealVector[] repmat = new RealVector[dimensions];
			for (int i = 0; i < dimensions; i++) {
				repmat[i] = new RealVector(toReplicate);
			}

			for (int i = 0; i < dimensions; i++) {
				for(int j = 0; j < batchSize; j++) {
					repmat[i].w[j] *= batch[j].getFeatures().w[i];
				}
			}

			RealVector result = new RealVector(dimensions);

			for (int i = 0; i < dimensions;i++) {
				result.w[i] = repmat[i].average();
			}
			return result;
		}

		private TrainingInstance[] misclassified(TrainingInstance[] subset, RealVector weights) {
			List<TrainingInstance> result = new ArrayList<>();

			for (TrainingInstance ti : subset) {
				int predictedClass = classify(ti, weights);

				if ((predictedClass == +1) && (ti.getLabel() == -1)) {
					result.add(ti);
				}
			}

			return result.toArray(new TrainingInstance[result.size()]);
		}

		private int classify(TrainingInstance ti, RealVector w) {
			RealVector features = ti.getFeatures();
			double result = features.dotProduct(w);

			if (result >= 0) {
				return 1;
			}
			return -1;
		 }

		private TrainingInstance[] selectSubset(int k) {
			TrainingInstance[] result = new TrainingInstance[k];
			Set<Integer> visited = new HashSet<>();

			for (int i = 0; i < k; i++) {
				while (true) {
					int nextIdx = rand.nextInt(k);

					if (visited.add(nextIdx)) {
						result[i] = trainingSet.get(nextIdx);
						break;
					}
				}
			}

			return result;
		}
	}

	// Hyperplane weights.
	RealVector weights;

	private SVM(RealVector weights) {
		this.weights = weights;
	}

	/** Trains SVM with a list of training instances, and with given maximum number of iterations. */
	static SVM trainSVM(List<TrainingInstance> trainingSet, int iterations) {
		return new SVM(new BatchSVM(trainingSet).train(iterations));
	}

	/** Instantiates SVM from weights given as a string. */
	SVM(String input) {
		List<Double> ll = new LinkedList<>();
		try (Scanner sc = new Scanner(input)) {
			while(sc.hasNext()) {
				double coef = sc.nextDouble();
				ll.add(coef);
			}
		}

		double[] w = new double[ll.size()];
		int cnt = 0;
		for (Double coef : ll) {
			w[cnt++] = coef;
		}

		this.weights = new RealVector(w);
	}

	/** Instantiates the SVM model as the average model of the input SVMs. */
	SVM(List<SVM> svmList) {
		int dim = svmList.get(0).getDimension();
		RealVector w = new RealVector(dim);

		for (SVM svm : svmList) {
			w.add(svm.getWeights());
		}

		this.weights = w.scaleThis(1.0 / svmList.size());
	}

	int getDimension() {
		return weights.getDimension();
	}

	/** Given a training instance it returns the result of sign(weights'instanceFeatures). */
	int classify(TrainingInstance ti) {
		RealVector features = ti.getFeatures();
		double result = features.dotProduct(weights);
		if (result >= 0) {
			return 1;
		}
		return -1;
	}

	RealVector getWeights() {
		return this.weights;
	}

	@Override
	public String toString() {
		return weights.toString();
	}
}
