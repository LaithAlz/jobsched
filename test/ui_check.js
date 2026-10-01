// Verify the demo UI's timeline geometry without a browser.
//
// timeline() is pure: job object in, HTML string out. So the claim the whole page exists to make
// -- that each backoff span is drawn twice as wide as the previous one -- can be checked by
// parsing the inline styles it emits.
const fs = require('fs');
const vm = require('vm');

const path = require('path');
const html = fs.readFileSync(path.join(__dirname, '..', 'internal', 'api', 'ui', 'index.html'), 'utf8');
const js = html.slice(html.lastIndexOf('<script>') + 8, html.lastIndexOf('</script>'));

// Minimal DOM so the script's top-level wiring can run.
const el = () => new Proxy({}, {
  get: (t, k) => k === 'addEventListener' || k === 'remove' || k === 'insertBefore'
    ? () => {} : (k in t ? t[k] : el()),
  set: (t, k, v) => { t[k] = v; return true; },
});
const ctx = {
  console,
  document: {
    getElementById: el, createElement: el, querySelector: () => null,
    querySelectorAll: () => [], addEventListener: () => {},
  },
  fetch: () => Promise.reject(new Error('no network in check')),
  location: { hash: '' },
  history: { replaceState: () => {} },
  setInterval: () => 0, setTimeout: () => 0, clearTimeout: () => {},
  Date, Math, JSON, Set, Map, Promise, parseInt, parseFloat, String, Number, Array, Object,
};
vm.createContext(ctx);
vm.runInContext(js, ctx, { filename: 'index.html<script>' });
console.log('✓ javascript parses and top-level wiring runs');

// Real payload shape, mirroring what GET /jobs returns for a job that failed four times.
const t = (ms) => new Date(1770000000000 + ms).toISOString();
const job = {
  id: 'job_deadbeef', status: 'failed', attempts_used: 4,
  spec: { command: 'exit 1', image: 'alpine:3.20', cpus: 0.5, memory_mb: 64,
          max_attempts: 4, priority: 0, timeout_seconds: 60 },
  attempts: [
    { number: 1, started_at: t(0),     finished_at: t(400),   exit_code: 1, error: 'x', logs: 'boom' },
    { number: 2, started_at: t(1400),  finished_at: t(1800),  exit_code: 1, error: 'x', logs: 'boom' },
    { number: 3, started_at: t(3800),  finished_at: t(4200),  exit_code: 1, error: 'x', logs: 'boom' },
    { number: 4, started_at: t(8200),  finished_at: t(8600),  exit_code: 1, error: 'x', logs: 'boom' },
  ],
};
// backoffs are 1000ms, 2000ms, 4000ms; total span 8600ms.

const out = ctx.timeline(job);
const widths = [...out.matchAll(/class="gap" style="left:([\d.]+)%;width:([\d.]+)%"/g)]
  .map(m => ({ left: +m[1], width: +m[2] }));

console.log('gap spans drawn:', widths.map(w => w.width.toFixed(2) + '%').join('  '));
if (widths.length !== 3) { console.error('✗ expected 3 backoff spans, got ' + widths.length); process.exit(1); }

const expect = [1000, 2000, 4000].map(ms => (ms / 8600) * 100);
widths.forEach((w, i) => {
  if (Math.abs(w.width - expect[i]) > 0.01) {
    console.error(`✗ gap ${i + 1} width ${w.width}% != expected ${expect[i].toFixed(2)}%`); process.exit(1);
  }
});
console.log('✓ each gap is scaled to real elapsed time');

// The claim that makes the picture worth looking at.
const r1 = widths[1].width / widths[0].width, r2 = widths[2].width / widths[1].width;
if (Math.abs(r1 - 2) > 0.02 || Math.abs(r2 - 2) > 0.02) {
  console.error(`✗ spans do not double: ratios ${r1.toFixed(3)}, ${r2.toFixed(3)}`); process.exit(1);
}
console.log(`✓ each backoff span is exactly 2x the previous (${r1.toFixed(2)}x, ${r2.toFixed(2)}x)`);

// Bars must sit inside the axis, in order, and be labelled with the measured wait.
const bars = [...out.matchAll(/class="bar" style="--st:([^;]+);left:([\d.]+)%;width:([\d.]+)%/g)]
  .map(m => ({ color: m[1], left: +m[2], width: +m[3] }));
if (bars.length !== 4) { console.error('✗ expected 4 attempt bars, got ' + bars.length); process.exit(1); }
for (let i = 1; i < bars.length; i++) {
  if (bars[i].left <= bars[i - 1].left) { console.error('✗ bars not left-to-right in time order'); process.exit(1); }
}
if (bars.some(b => b.left + b.width > 100.01)) { console.error('✗ a bar overflows the axis'); process.exit(1); }
console.log('✓ 4 attempt bars, ordered, all within the axis');

for (const lbl of ['backoff <b>1.00s</b>', 'backoff <b>2.00s</b>', 'backoff <b>4.00s</b>']) {
  if (!out.includes(lbl)) { console.error('✗ missing label: ' + lbl); process.exit(1); }
}
console.log('✓ labels show measured backoff: 1.00s / 2.00s / 4.00s');

// A job still waiting must draw the pending gap and a countdown.
const pending = { ...job, status: 'retrying', attempts: job.attempts.slice(0, 2),
                  next_attempt_at: new Date(Date.now() + 3000).toISOString() };
const p = ctx.timeline(pending);
if (!p.includes('retry in')) { console.error('✗ retrying job has no countdown'); process.exit(1); }
console.log('✓ retrying job renders a pending gap with a countdown');

// A job that has been accepted but never run must not blow up.
if (!ctx.timeline({ ...job, status: 'queued', attempts: [] }).includes('waiting for first attempt')) {
  console.error('✗ empty-attempt job not handled'); process.exit(1);
}
console.log('✓ queued job with no attempts renders safely');

// escaping
if (ctx.timeline({ ...job, attempts: [{ ...job.attempts[0], exit_code: '<script>x</script>' }] }).includes('<script>x')) {
  console.error('✗ attempt fields are not escaped'); process.exit(1);
}
console.log('✓ job-controlled values are html-escaped');
console.log('\nall ui checks passed');
