use std::error::Error;
use crate::helix_engine::vector_core::vector::HVector;
// TODO: use nalgebra here instead for more efficient calcs

pub struct PCA {
    components: Vec<Vec<f64>>,
    mean: Vec<f64>,
    n_components: usize,
}

impl PCA {
    pub fn new_empty() -> Self {
        PCA {
            components: Vec::new(),
            mean: Vec::new(),
            n_components: 0,
        }
    }

    pub fn fit(&mut self, vectors: &Vec<HVector>, n_components: usize) -> Result<(), Box<dyn Error>> {
        if vectors.is_empty() {
            return Err("No vectors provided".into());
        }
        let n_dims = vectors[0].get_data().len();
        if vectors.iter().any(|v| v.get_data().len() != n_dims) {
            return Err("All vectors must have the same dimensionality".into());
        }
        if n_components > n_dims {
            return Err("n_components must be <= original dimensions".into());
        }

        let n_samples = vectors.len();

        // Compute mean
        let mut mean = vec![0.0; n_dims];
        for v in vectors {
            for (i, &x) in v.get_data().iter().enumerate() {
                mean[i] += x / n_samples as f64;
            }
        }

        // Center the data and compute covariance matrix
        let mut cov = vec![vec![0.0; n_dims]; n_dims];
        for v in vectors {
            let centered: Vec<f64> = v.get_data().iter().zip(&mean).map(|(&x, &m)| x - m).collect();
            for i in 0..n_dims {
                for j in 0..n_dims {
                    cov[i][j] += centered[i] * centered[j] / (n_samples as f64 - 1.0);
                }
            }
        }

        // Power iteration to find top n_components eigenvectors
        let mut components = Vec::with_capacity(n_components);
        for _ in 0..n_components {
            let eigenvector = power_iteration(&cov, n_dims);
            components.push(eigenvector.clone());

            // Deflate covariance matrix
            let magnitude = eigenvector.iter().map(|x| x * x).sum::<f64>().sqrt();
            let normalized: Vec<f64> = eigenvector.iter().map(|x| x / magnitude).collect();
            for i in 0..n_dims {
                for j in 0..n_dims {
                    cov[i][j] -= normalized[i] * normalized[j] * magnitude;
                }
            }
        }

        // Update the struct's fields
        self.components = components;
        self.mean = mean;
        self.n_components = n_components;

        Ok(())
    }

    // Method to transform vectors using the current state
    pub fn transform(&self, vectors: &[HVector]) -> Result<Vec<Vec<f64>>, Box<dyn Error>> {
        if vectors.is_empty() {
            return Err("No vectors provided".into());
        }
        let n_dims = vectors[0].get_data().len();
        if vectors.iter().any(|v| v.get_data().len() != n_dims) {
            return Err("All vectors must have the same dimensionality".into());
        }
        if self.components.is_empty() {
            return Err("PCA object has no components".into());
        }
        if self.mean.len() != n_dims {
            return Err("Mean length does not match vector dimensionality".into());
        }

        let mut reduced_vecs = Vec::with_capacity(vectors.len());
        for v in vectors {
            let centered: Vec<f64> = v.get_data().iter().zip(&self.mean).map(|(&x, &m)| x - m).collect();
            let mut reduced = vec![0.0; self.n_components];
            for (i, component) in self.components.iter().enumerate() {
                reduced[i] = centered.iter().zip(component).map(|(&x, &c)| x * c).sum();
            }
            reduced_vecs.push(reduced);
        }

        Ok(reduced_vecs)
    }

    // Method to transform a single vector
    pub fn transform_single(&self, vector: &HVector) -> Vec<f64> {
        if self.components.is_empty() {
            return vec![]; // Return empty vec if PCA is not initialized
        }
        let centered: Vec<f64> = vector.get_data().iter().zip(&self.mean).map(|(&x, &m)| x - m).collect();
        let mut reduced = vec![0.0; self.n_components];
        for (i, component) in self.components.iter().enumerate() {
            reduced[i] = centered.iter().zip(component).map(|(&x, &c)| x * c).sum();
        }
        reduced
    }
}

fn power_iteration(matrix: &[Vec<f64>], n_dims: usize) -> Vec<f64> {
    let mut v = vec![1.0; n_dims]; // Initial guess
    const MAX_ITER: usize = 100;
    const TOLERANCE: f64 = 1e-6;

    for _ in 0..MAX_ITER {
        let mut new_v = vec![0.0; n_dims];
        for i in 0..n_dims {
            for j in 0..n_dims {
                new_v[i] += matrix[i][j] * v[j];
            }
        }

        let norm = new_v.iter().map(|x| x * x).sum::<f64>().sqrt();
        for i in 0..n_dims {
            new_v[i] /= norm;
        }

        let diff = v.iter().zip(&new_v).map(|(&old, &new)| (old - new).abs()).sum::<f64>();
        v = new_v;
        if diff < TOLERANCE {
            break;
        }
    }

    v
}
