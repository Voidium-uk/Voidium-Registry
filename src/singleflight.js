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