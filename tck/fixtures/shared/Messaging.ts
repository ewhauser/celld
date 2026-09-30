// Shared inputs for the messaging fixtures. The driver builds the expected
// observation from the same factories, so fixture and oracle cannot drift.

// Structured-clone payloads posted through a MessagePort, in order. Transfer
// lists are deliberately absent: workerd rejects them on MessagePort.
export const channelMessages = (): ReadonlyArray<unknown> => [
  "first",
  { n: 2, set: new Set(["a"]), bytes: new Uint8Array([0, 255]) },
  [3, null],
];

// A server-sent event stream exercising retry, id, a named event, and a
// multi-line data field.
export const sseBody = () =>
  "retry: 5000\n\nid: 1\nevent: greeting\ndata: hello λ\n\ndata: line one\ndata: line two\n\n";
