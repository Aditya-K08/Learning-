import { readdirSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
for (const dir of ['src', 'scripts']) {
  for (const file of readdirSync(dir).filter(x => x.endsWith('.js'))) {
    const path = join(dir, file);
    const result = spawnSync(process.execPath, ['--check', path], { stdio: 'inherit' });
    if (result.status !== 0) process.exit(result.status || 1);
    console.log('syntax OK:', path);
  }
}
