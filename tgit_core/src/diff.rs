use crate::storage::TGitManifest;
use std::collections::HashSet;

#[derive(Debug)]
pub struct TensorDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub modified: Vec<String>,
    pub unchanged: Vec<String>,
}

#[derive(Debug)]
pub struct ManifestComparison {
    pub tensor_diff: TensorDiff,
    pub size_change: i64,
    pub storage_savings: StorageSavings,
}

#[derive(Debug)]
pub struct StorageSavings {
    pub total_tensors_old: usize,
    pub total_tensors_new: usize,
    pub shared_blobs: usize,
    pub unique_blobs_old: usize,
    pub unique_blobs_new: usize,
    pub deduplication_ratio: f64,
}

impl TGitManifest {
    /// Compare two manifests and show differences
    pub fn diff(&self, other: &TGitManifest) -> ManifestComparison {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut modified = Vec::new();
        let mut unchanged = Vec::new();

        let self_keys: HashSet<_> = self.tensors.keys().collect();
        let other_keys: HashSet<_> = other.tensors.keys().collect();

        // Find added tensors
        for key in other_keys.difference(&self_keys) {
            added.push((*key).clone());
        }

        // Find removed tensors
        for key in self_keys.difference(&other_keys) {
            removed.push((*key).clone());
        }

        // Find modified and unchanged tensors
        for key in self_keys.intersection(&other_keys) {
            let self_tensor = &self.tensors[*key];
            let other_tensor = &other.tensors[*key];

            if self_tensor.hash != other_tensor.hash {
                modified.push((*key).clone());
            } else {
                unchanged.push((*key).clone());
            }
        }

        // Calculate storage savings
        let storage_savings = calculate_storage_savings(self, other);

        let size_change = other.total_size as i64 - self.total_size as i64;

        ManifestComparison {
            tensor_diff: TensorDiff {
                added,
                removed,
                modified,
                unchanged,
            },
            size_change,
            storage_savings,
        }
    }

    /// Print a human-readable diff
    pub fn print_diff(&self, other: &TGitManifest) {
        let comparison = self.diff(other);
        let diff = &comparison.tensor_diff;

        println!("\nManifest Comparison:");
        println!("==================");
        
        if !diff.added.is_empty() {
            println!("\nAdded Tensors ({}):", diff.added.len());
            for name in &diff.added {
                let tensor = &other.tensors[name];
                println!("  + {} [shape: {:?}, dtype: {}, hash: {}]", 
                    name, tensor.shape, tensor.dtype, &tensor.hash[..8]);
            }
        }

        if !diff.removed.is_empty() {
            println!("\nRemoved Tensors ({}):", diff.removed.len());
            for name in &diff.removed {
                let tensor = &self.tensors[name];
                println!("  - {} [shape: {:?}, dtype: {}, hash: {}]", 
                    name, tensor.shape, tensor.dtype, &tensor.hash[..8]);
            }
        }

        if !diff.modified.is_empty() {
            println!("\nModified Tensors ({}):", diff.modified.len());
            for name in &diff.modified {
                let old_tensor = &self.tensors[name];
                let new_tensor = &other.tensors[name];
                println!("  ~ {} [shape: {:?} -> {:?}, dtype: {}, hash: {} -> {}]",
                    name, 
                    old_tensor.shape, 
                    new_tensor.shape,
                    new_tensor.dtype,
                    &old_tensor.hash[..8],
                    &new_tensor.hash[..8]
                );
            }
        }

        println!("\nUnchanged Tensors: {}", diff.unchanged.len());
        
        let sign = if comparison.size_change >= 0 { "+" } else { "" };
        println!("Total Size Change: {}{} bytes", sign, comparison.size_change);

        println!("\nStorage Efficiency:");
        println!("  Old manifest: {} tensors, {} unique blobs",
            comparison.storage_savings.total_tensors_old,
            comparison.storage_savings.unique_blobs_old);
        println!("  New manifest: {} tensors, {} unique blobs",
            comparison.storage_savings.total_tensors_new,
            comparison.storage_savings.unique_blobs_new);
        println!("  Shared blobs: {}", comparison.storage_savings.shared_blobs);
        println!("  Deduplication ratio: {:.2}x", comparison.storage_savings.deduplication_ratio);
    }
}

fn calculate_storage_savings(old: &TGitManifest, new: &TGitManifest) -> StorageSavings {
    let mut old_hashes = HashSet::new();
    let mut new_hashes = HashSet::new();

    for tensor in old.tensors.values() {
        old_hashes.insert(&tensor.hash);
    }

    for tensor in new.tensors.values() {
        new_hashes.insert(&tensor.hash);
    }

    let shared_count = old_hashes.intersection(&new_hashes).count();
    let unique_old = old_hashes.len();
    let unique_new = new_hashes.len();

    let total_unique_blobs = old_hashes.union(&new_hashes).count();
    let total_tensor_count = old.tensors.len() + new.tensors.len();
    
    let dedup_ratio = if total_unique_blobs > 0 {
        total_tensor_count as f64 / total_unique_blobs as f64
    } else {
        1.0
    };

    StorageSavings {
        total_tensors_old: old.tensors.len(),
        total_tensors_new: new.tensors.len(),
        shared_blobs: shared_count,
        unique_blobs_old: unique_old,
        unique_blobs_new: unique_new,
        deduplication_ratio: dedup_ratio,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use indexmap::IndexMap;
    use crate::storage::ManifestTensor;

    fn create_test_tensor(hash: &str, shape: Vec<usize>) -> ManifestTensor {
        ManifestTensor {
            shape,
            dtype: "F32".to_string(),
            hash: hash.to_string(),
            index: 0,
            extra: IndexMap::new(),
        }
    }

    #[test]
    fn test_diff_added_tensors() {
        let mut old_tensors = BTreeMap::new();
        old_tensors.insert("t1".to_string(), create_test_tensor("hash1", vec![1, 2]));

        let mut new_tensors = BTreeMap::new();
        new_tensors.insert("t1".to_string(), create_test_tensor("hash1", vec![1, 2]));
        new_tensors.insert("t2".to_string(), create_test_tensor("hash2", vec![3, 4]));

        let old_manifest = TGitManifest {
            tensors: old_tensors,
            version: "1.0".to_string(),
            total_size: 100,
        };

        let new_manifest = TGitManifest {
            tensors: new_tensors,
            version: "1.0".to_string(),
            total_size: 200,
        };

        let comparison = old_manifest.diff(&new_manifest);
        assert_eq!(comparison.tensor_diff.added.len(), 1);
        assert_eq!(comparison.tensor_diff.removed.len(), 0);
        assert_eq!(comparison.tensor_diff.unchanged.len(), 1);
    }
}
