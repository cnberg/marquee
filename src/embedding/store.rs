use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use lancedb::connect;
use lancedb::query::{ExecutableQuery, QueryBase};
use std::sync::Arc;

pub struct EmbeddingStore {
    db: lancedb::Connection,
    dimension: usize,
}

impl EmbeddingStore {
    /// Connect to LanceDB (data directory: data/lancedb/).
    pub async fn new(
        data_dir: &str,
        dimension: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let db = connect(data_dir).execute().await?;
        Ok(Self { db, dimension })
    }

    /// Create or open the `movies` table with the expected schema.
    pub async fn ensure_table(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table_names = self.db.table_names().execute().await?;
        if !table_names.contains(&"movies".to_string()) {
            let schema = self.make_schema();
            let batch = self.make_empty_batch(&schema);
            let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));
            let reader: Box<dyn RecordBatchReader + Send> = Box::new(batches);
            self.db.create_table("movies", reader).execute().await?;
            tracing::info!("LanceDB movies table created");
        }
        Ok(())
    }

    fn make_schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("movie_id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    self.dimension as i32,
                ),
                false,
            ),
        ])
    }

    fn make_empty_batch(&self, schema: &Schema) -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let texts: ArrayRef = Arc::new(StringArray::from(Vec::<String>::new()));
        let values = Float32Array::from(Vec::<f32>::new());
        let vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            self.dimension as i32,
            Arc::new(values) as ArrayRef,
            None,
        )
        .expect("empty fixed size list");
        let vectors: ArrayRef = Arc::new(vectors);

        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![ids, texts, vectors],
        )
        .unwrap()
    }

    /// Batch upsert movie embeddings.
    /// Params: Vec<(movie_id, text_used_for_embedding, embedding_vector)>
    pub async fn upsert_movies(
        &self,
        data: Vec<(i64, String, Vec<f32>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if data.is_empty() {
            return Ok(());
        }

        let schema = self.make_schema();
        let ids: ArrayRef =
            Arc::new(Int64Array::from(data.iter().map(|(id, _, _)| *id).collect::<Vec<_>>()));
        let texts: ArrayRef =
            Arc::new(StringArray::from(data.iter().map(|(_, t, _)| t.clone()).collect::<Vec<_>>()));

        let flat_values: Vec<f32> = data.iter().flat_map(|(_, _, v)| v.clone()).collect();
        let values = Float32Array::from(flat_values);
        let vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            self.dimension as i32,
            Arc::new(values) as ArrayRef,
            None,
        )?;
        let vectors: ArrayRef = Arc::new(vectors);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![ids, texts, vectors],
        )?;
        let batches = RecordBatchIterator::new(vec![Ok(batch)], Arc::new(schema));
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(batches);

        let table = self.db.open_table("movies").execute().await?;
        let mut builder = table.merge_insert(&["movie_id"]);
        builder.when_matched_update_all(None);
        builder.when_not_matched_insert_all();
        builder.execute(reader).await?;

        Ok(())
    }

    /// Vector similarity search, returning top-K movie ids with distance.
    pub async fn search(
        &self,
        query_vector: &[f32],
        top_k: usize,
    ) -> Result<Vec<(i64, f32)>, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.db.open_table("movies").execute().await?;
        let stream = table
            .vector_search(query_vector.to_vec())?
            .limit(top_k)
            .execute()
            .await?;

        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut hits = Vec::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("movie_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .expect("movie_id column");
            let distances = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .expect("_distance column");

            for i in 0..batch.num_rows() {
                hits.push((ids.value(i), distances.value(i)));
            }
        }
        Ok(hits)
    }

    /// Return the set of movie_ids already indexed.
    pub async fn get_indexed_movie_ids(
        &self,
    ) -> Result<std::collections::HashSet<i64>, Box<dyn std::error::Error + Send + Sync>> {
        let table = self.db.open_table("movies").execute().await?;
        let stream = table
            .query()
            .select(lancedb::query::Select::Columns(vec!["movie_id".to_string()]))
            .execute()
            .await?;

        use futures::TryStreamExt;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut ids = std::collections::HashSet::new();
        for batch in &batches {
            let col = batch
                .column_by_name("movie_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .expect("movie_id column");
            for i in 0..batch.num_rows() {
                ids.insert(col.value(i));
            }
        }
        Ok(ids)
    }
}
