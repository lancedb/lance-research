use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub path: PathBuf,
    pub description: Option<String>,
}

impl Dataset {
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            description: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

#[derive(Debug, Default)]
pub struct DatasetRegistry {
    datasets: HashMap<String, Dataset>,
}

impl DatasetRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_dataset(&mut self, dataset: Dataset) {
        self.datasets.insert(dataset.name.clone(), dataset);
    }

    pub fn get_dataset(&self, name: &str) -> Option<&Dataset> {
        self.datasets.get(name)
    }

    pub fn list_datasets(&self) -> Vec<&Dataset> {
        self.datasets.values().collect()
    }

    pub fn dataset_names(&self) -> Vec<&str> {
        self.datasets.keys().map(|s| s.as_str()).collect()
    }

    pub fn default_registry() -> Self {
        let mut registry = Self::new();
        
        // Register fineweb dataset
        let fineweb = Dataset::new(
            "fineweb", 
            "data/real/fineweb/data_CC-MAIN-2024-51_000_00000.parquet"
        ).with_description("FineWeb dataset - high-quality web text from CommonCrawl (1,004,971 rows)");
        
        registry.register_dataset(fineweb);
        
        registry
    }
}