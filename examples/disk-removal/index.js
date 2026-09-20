export class Ledger {
  constructor(state) { this.state = state; }
  async fetch(request) {
    const id = new URL(request.url).searchParams.get("id");
    if (request.method === "PUT") await this.state.storage.put(id, id);
    return Response.json({ id, value: await this.state.storage.get(id) ?? null });
  }
}
export default {
  fetch(request, env) {
    const cell = new URL(request.url).searchParams.get("cell") ?? "ledger";
    return env.LEDGER.get(env.LEDGER.idFromName(cell)).fetch(request);
  }
};
