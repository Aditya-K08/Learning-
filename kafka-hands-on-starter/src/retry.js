// LAB 4 / TODO 10
// Return capped exponential backoff in milliseconds.
// attempt=1 => baseMs; attempt=2 => 2*baseMs; etc.
// Add jitter in your actual retry loop; this pure function is deterministic
// so it is easy to test. Reject attempt < 1.
export function retryDelayMs(attempt, { baseMs = 200, capMs = 5000 } = {}) {
  throw new Error('TODO 10: implement retryDelayMs');
}
