import { test } from 'node:test';
import assert from 'node:assert/strict';
import { retryDelayMs } from '../src/retry.js';

test('exponential and capped backoff', () => {
  assert.equal(retryDelayMs(1), 200);
  assert.equal(retryDelayMs(2), 400);
  assert.equal(retryDelayMs(3), 800);
  assert.equal(retryDelayMs(10), 5000);
  assert.equal(retryDelayMs(4, { baseMs: 100, capMs: 300 }), 300);
});

test('invalid attempts are rejected', () => {
  assert.throws(() => retryDelayMs(0));
  assert.throws(() => retryDelayMs(-1));
});
