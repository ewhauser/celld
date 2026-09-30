// Shared markup escaper. The four common replacements are identical for HTML and
// XML; only the apostrophe entity differs, because junit consumers are stricter
// about numeric character references than HTML parsers are.
export const escapeMarkup = (value: string, apostrophe: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", apostrophe);
