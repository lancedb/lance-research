use random_take_bench::util::RandomIndices;

const NUM_CHUNKS: usize = 1024;
const PAGE_SIZE: usize = 8 * 1024;

/// This is easily the most difficult way to come up with this number („• ֊ •„)
///
/// Still, it's simple to understand and we're pretty much just empirically verifying
/// a basic math formula.
#[tokio::main]
async fn main() {
    println!("num_values,pages_hit_4b,pages_hit_3k");
    for rows_per_chunk in [
        128,
        256,
        512,
        1024,
        2 * 1024,
        4 * 1024,
        8 * 1024,
        16 * 1024,
        32 * 1024,
        64 * 1024,
        128 * 1024,
        256 * 1024,
        512 * 1024,
        1024 * 1024,
        2048 * 1024,
        4096 * 1024,
    ] {
        let indices = RandomIndices::new(rows_per_chunk, NUM_CHUNKS).await;
        let num_pages_4b = rows_per_chunk * NUM_CHUNKS / (PAGE_SIZE / 4);
        let mut used_pages_4b = vec![false; num_pages_4b];

        let num_pages_3k = rows_per_chunk * NUM_CHUNKS / (PAGE_SIZE / (3 * 1024));
        let mut used_pages_3k = vec![false; num_pages_3k];

        assert!(indices.all_indices().len() > 100 * 1024);

        for index in &indices.all_indices().slice(0, 100 * 1024) {
            let page_4b = *index as usize / (PAGE_SIZE / 4);
            used_pages_4b[page_4b] = true;

            let page_3k = *index as usize / (PAGE_SIZE / (3 * 1024));
            used_pages_3k[page_3k] = true;
        }

        let used_pages_4b = used_pages_4b.iter().filter(|x| **x).count();
        let used_pages_3k = used_pages_3k.iter().filter(|x| **x).count();

        println!(
            "{},{},{}",
            rows_per_chunk * NUM_CHUNKS,
            used_pages_4b,
            used_pages_3k
        );
    }
}
