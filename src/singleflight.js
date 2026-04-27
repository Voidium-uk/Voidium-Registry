export function createSingleFlight() {
  const flights = new Map();
  return async function singleFlight(key, fn) {
    if (flights.has(key)) return flights.get(key);
    const promise = Promise.resolve()
      .then(fn)
      .finally(() => flights.delete(key));
    flights.set(key, promise);
    return promise;
  };
}

export function createLimiter(limit) {
  const max = Math.max(1, Number(limit) || 1);
  let active = 0;
  const queue = [];

  const pump = () => {
    if (active >= max) return;
    const next = queue.shift();
    if (!next) return;
    active += 1;
    next();
  };

  const run = function run(fn) {
    return new Promise((resolve, reject) => {
      const start = () => {
        Promise.resolve()
          .then(fn)
          .then(resolve, reject)
          .finally(() => {
            active -= 1;
            pump();
          });
      };

      if (active < max) {
        active += 1;
        start();
      } else {
        queue.push(start);
      }
    });
  };

  run.stats = () => ({
    limit: max,
    active,
    queued: queue.length,
    available: Math.max(0, max - active),
  });

  return run;
}
