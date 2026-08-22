// Executes a page's inline <script> under a stub DOM and reports whether it
// survives LOAD. `node --check` only parses — it cannot see a temporal dead zone,
// which is how `let feedTab = activeTab;` (declared 29 lines later) shipped and
// froze the whole dashboard at "Loading…" with every tile showing an em-dash.
//
// Usage: node js_load_harness.js <file.js>
// Exits 0 if the script loads, 1 with the error if it throws.
const fs = require('fs');
const vm = require('vm');

const src = fs.readFileSync(process.argv[2], 'utf8');

// Chainable no-op: any property access returns another stub, any call returns one.
// Real enough for load-time wiring (addEventListener, getElementById().style, …)
// without pretending to be a browser.
const stub = new Proxy(function () {}, {
  get: (t, k) => {
    if (k === Symbol.toPrimitive || k === 'toString') return () => '';
    if (k === 'length') return 0;
    if (k === Symbol.iterator) return function* () {};
    return stub;
  },
  set: () => true,
  apply: () => stub,
  construct: () => stub,
  has: () => true,
});

const ctx = {
  document: stub, window: stub, localStorage: stub, sessionStorage: stub,
  navigator: stub, location: stub, console: { log(){}, warn(){}, error(){}, debug(){} },
  fetch: () => new Promise(() => {}),          // never resolves: load must not depend on it
  setTimeout: () => 0, setInterval: () => 0,
  clearTimeout: () => {}, clearInterval: () => {},
  requestAnimationFrame: () => 0,
  Chart: stub, LightweightCharts: stub, alert: () => {}, confirm: () => true,
  // Third-party globals loaded via <script src> — absent here, so stub them or the
  // harness reports a missing CDN library instead of the page's own bugs.
  flatpickr: stub, Papa: stub, marked: stub, hljs: stub,
  addEventListener: () => {}, removeEventListener: () => {}, matchMedia: () => stub,
  URLSearchParams, URL, TextEncoder, TextDecoder,
};
ctx.window = ctx;
ctx.globalThis = ctx;
ctx.self = ctx;

try {
  vm.createContext(ctx);
  new vm.Script(src, { filename: process.argv[2] }).runInContext(ctx, { timeout: 5000 });
  process.exit(0);
} catch (e) {
  // ReferenceError here is the signal we care about: an identifier used before it
  // exists. Report and fail.
  console.error(`${e.name}: ${e.message}`);
  process.exit(1);
}
