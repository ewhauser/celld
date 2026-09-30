import type { SiteModel } from "./Model.js";
// The build emits escaped JSON. Render report content as text, never as HTML.
const model: SiteModel = JSON.parse(
  document.getElementById("site-data")!.textContent!,
);
const search = document.getElementById("search") as HTMLInputElement;
const status = document.getElementById("status") as HTMLSelectElement;
const tableRows = [
  ...document.querySelectorAll<HTMLTableRowElement>("tr[data-row]"),
];
const tabs = [...document.querySelectorAll<HTMLButtonElement>("[data-group]")];
let group = "all";
const filter = () => {
  let visible = 0;
  const query = search.value.trim().toLowerCase();
  for (const [index, element] of tableRows.entries()) {
    const row = model.rows[index]!;
    const values = [row.reference.status, row.candidate.status];
    const matchesStatus =
      status.value === "all" ||
      (status.value === "attention"
        ? values.some((value) =>
            [
              "fail",
              "reference-error",
              "infrastructure-error",
              "missing",
            ].includes(value),
          )
        : status.value === "exceptions"
          ? values.some((value) => ["known-bug", "divergence"].includes(value))
          : values.includes(status.value as (typeof values)[number]));
    element.hidden = !(
      (group === "all" || row.group === group) &&
      row.id.toLowerCase().includes(query) &&
      matchesStatus
    );
    if (!element.hidden) visible++;
  }
  document.getElementById("visible-count")!.textContent =
    `${visible} of ${model.rows.length} cases`;
  document.getElementById("empty")!.hidden = visible !== 0;
};
for (const tab of tabs)
  tab.addEventListener("click", () => {
    group = tab.dataset.group!;
    for (const other of tabs)
      other.setAttribute("aria-pressed", String(other === tab));
    filter();
  });
search.addEventListener("input", filter);
status.addEventListener("change", filter);
document.getElementById("clear")!.addEventListener("click", () => {
  search.value = "";
  status.value = "all";
  group = "all";
  for (const tab of tabs)
    tab.setAttribute("aria-pressed", String(tab.dataset.group === "all"));
  filter();
  search.focus();
});
const dialog = document.getElementById("evidence") as HTMLDialogElement;
let opener: HTMLElement | null = null;
for (const button of document.querySelectorAll<HTMLButtonElement>(
  "[data-result]",
))
  button.addEventListener("click", () => {
    const [index, side] = button.dataset.result!.split(":");
    const row = model.rows[Number(index)]!;
    const cell = side === "reference" ? row.reference : row.candidate;
    const result = cell.result!;
    document.getElementById("evidence-title")!.textContent = row.id;
    document.getElementById("evidence-suite")!.textContent =
      `${cell.suite} · case evidence`;
    document.getElementById("evidence-meta")!.textContent =
      `${result.status} · ${(result.durationMs / 1000).toFixed(2)}s`;
    (document.getElementById("evidence-download") as HTMLAnchorElement).href =
      `./reports/${cell.suite}.json`;
    const content = document.getElementById("evidence-content")!;
    content.replaceChildren();
    const section = (heading: string, value: unknown) => {
      if (value === undefined) return;
      const wrapper = document.createElement("section");
      const title = document.createElement("h3");
      title.textContent = heading;
      const pre = document.createElement("pre");
      pre.textContent =
        typeof value === "string" ? value : JSON.stringify(value, null, 2);
      wrapper.append(title, pre);
      content.append(wrapper);
    };
    section("Failure detail", result.error);
    section("Documented divergence", result.divergence);
    section("Known bugs", result.knownBugs);
    section("Reference observation", result.reference);
    section("Candidate observation", result.candidate);
    if (!content.childElementCount) section("Result", result);
    opener = button;
    dialog.showModal();
  });
document
  .getElementById("close")!
  .addEventListener("click", () => dialog.close());
dialog.addEventListener("click", (event) => {
  if (event.target === dialog) {
    const box = dialog.getBoundingClientRect();
    if (
      event.clientX < box.left ||
      event.clientX > box.right ||
      event.clientY < box.top ||
      event.clientY > box.bottom
    )
      dialog.close();
  }
});
dialog.addEventListener("close", () => opener?.focus());
