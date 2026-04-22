use fastembed::{EmbeddingModel as FEModel, InitOptions, TextEmbedding};
use std::sync::Mutex;

pub struct EmbeddingModel {
    model: Mutex<TextEmbedding>,
}

impl EmbeddingModel {
    /// Initialize the embedding model. Downloads weights on first run (~100MB) and caches them.
    pub fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let opts = InitOptions::new(FEModel::BGESmallZHV15).with_show_download_progress(true);
        let model = TextEmbedding::try_new(opts)?;
        Ok(Self {
            model: Mutex::new(model),
        })
    }

    /// Generate embeddings for a batch of texts.
    pub fn embed(
        &self,
        texts: Vec<String>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        let mut model = self.model.lock().expect("lock embedding model");
        let embeddings = model.embed(texts, None)?;
        Ok(embeddings)
    }

    /// Generate an embedding for a single text.
    pub fn embed_one(&self, text: &str) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        let mut results = self.embed(vec![text.to_string()])?;
        results
            .pop()
            .ok_or_else(|| "embedding returned empty result".into())
    }

    /// Model output dimension.
    pub fn dimension(&self) -> usize {
        512
    }
}
