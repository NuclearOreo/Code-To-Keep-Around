/**
 * Runs batched jobs with #workers asynchronous calls per iteration
 * @param {string} title the name of the job for logging purposes
 * @param {T[]} items list of args for the batch calls
 * @param {number} batchSize number of items per batch
 * @param {number} workers number of parallel batches
 * @param {Function} job the job to be run in batches
 */
export const asyncBatches = async <T, K>(
  title: string,
  items: T[],
  batchSize: number,
  workers: number,
  job: (items: T[]) => Promise<K[]>
) => {
  const batches = Math.floor(items.length / batchSize) + 1
  const timeLabel = `${title} job (${items.length} items -> ${batches} batches)`
  let results: K[] = []
  console.time(timeLabel)
  // paginate calls in batches of $batch_size
  for (let i = 0; i < batches; i += workers) {
    // make up to #workers async calls at a time to maximize performance
    const data = await Promise.all(
      [...Array(workers).keys()]
        .map(j => j + i)
        .filter(j => j < batches)
        .map(j => {
          const offset = j * batchSize
          const status = items.length
            ? Math.min(items.length, offset + batchSize) / items.length
            : 1
          console.log(`${title}: ${(status * 100).toFixed(2)}%`)
          const subset = items.slice(offset, offset + batchSize)
          return job(subset)
        })
    )
    // flatten the batch results
    data
      .filter(group => group)
      .forEach(group => (results = results.concat(group)))
  }
  console.timeEnd(timeLabel)
  return results
}
