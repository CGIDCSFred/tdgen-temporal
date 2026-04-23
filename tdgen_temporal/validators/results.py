"""
Shared result types for all validators.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


class Severity(str, Enum):
    ERROR   = "ERROR"    # definite integrity violation
    WARNING = "WARNING"  # plausibility issue, not strictly wrong


@dataclass
class Check:
    name:        str       # unique machine-readable key
    category:    str       # "referential" | "temporal" | "state"
    severity:    Severity
    table:       str       # primary table being checked
    description: str       # one-line human description


@dataclass
class Finding:
    check:     Check
    count:     int          # number of violating rows
    examples:  list = field(default_factory=list)   # up to 5 example PKs

    @property
    def severity(self) -> Severity:
        return self.check.severity


def print_report(
    checks: list[Check],
    findings: list[Finding],
    verbose: bool = False,
) -> None:
    finding_map = {f.check.name: f for f in findings}

    categories = ["referential", "temporal", "state"]
    cat_labels  = {
        "referential": "Referential Integrity",
        "temporal":    "Temporal Integrity",
        "state":       "State Consistency",
    }

    total_checks  = len(checks)
    total_pass    = total_checks - len(findings)
    total_errors  = sum(1 for f in findings if f.severity == Severity.ERROR)
    total_warnings = sum(1 for f in findings if f.severity == Severity.WARNING)
    total_rows    = sum(f.count for f in findings)

    W_name = 48
    W_sev  = 8
    W_tbl  = 30

    print()
    print("=" * 100)
    print(f"  TDGEN-TEMPORAL  --  Data Integrity Report")
    print("=" * 100)

    for cat in categories:
        cat_checks = [c for c in checks if c.category == cat]
        if not cat_checks:
            continue

        print()
        print(f"  {cat_labels[cat]}")
        print(f"  {'-' * 97}")
        print(f"  {'Check':<{W_name}}  {'Sev':<{W_sev}}  {'Table':<{W_tbl}}  {'Status':>8}  {'Rows':>6}")
        print(f"  {'-' * W_name}  {'-' * W_sev}  {'-' * W_tbl}  {'-' * 8}  {'-' * 6}")

        for chk in cat_checks:
            f = finding_map.get(chk.name)
            if f:
                status = "FAIL"
                rows   = str(f.count)
            else:
                status = "PASS"
                rows   = "-"

            sev = chk.severity.value
            print(f"  {chk.name:<{W_name}}  {sev:<{W_sev}}  {chk.table:<{W_tbl}}  {status:>8}  {rows:>6}")

            if verbose and f and f.examples:
                ex = ", ".join(str(e) for e in f.examples[:5])
                print(f"  {'':>{W_name + 2}}  example PKs: {ex}")

    print()
    print("=" * 100)
    print(f"  Summary")
    print(f"  {'-' * 97}")
    print(f"  Total checks   : {total_checks}")
    print(f"  Passed         : {total_pass}")
    print(f"  Failed (ERROR) : {total_errors}")
    print(f"  Failed (WARN)  : {total_warnings}")
    print(f"  Violating rows : {total_rows:,}")
    print("=" * 100)
    print()


def generate_html_report(
    checks: list[Check],
    findings: list[Finding],
    db_path: Path,
    output_path: Path,
) -> None:
    finding_map = {f.check.name: f for f in findings}

    total_checks   = len(checks)
    total_pass     = total_checks - len(findings)
    total_errors   = sum(1 for f in findings if f.severity == Severity.ERROR)
    total_warnings = sum(1 for f in findings if f.severity == Severity.WARNING)
    total_rows     = sum(f.count for f in findings)
    generated_at   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    overall_class = "ok" if total_errors == 0 and total_warnings == 0 else \
                    "warn" if total_errors == 0 else "fail"
    overall_label = "ALL CLEAR" if overall_class == "ok" else \
                    "WARNINGS" if overall_class == "warn" else "ERRORS FOUND"

    categories = ["referential", "temporal", "state"]
    cat_labels = {
        "referential": "Referential Integrity",
        "temporal":    "Temporal Integrity",
        "state":       "State Consistency",
    }

    def _rows_html(cat: str) -> str:
        cat_checks = [c for c in checks if c.category == cat]
        rows = []
        for chk in cat_checks:
            f = finding_map.get(chk.name)
            if f:
                status_cell = f'<td class="status fail">FAIL</td>'
                row_cell    = f'<td class="num fail-num">{f.count:,}</td>'
                examples    = f'<div class="examples">example PKs: {", ".join(str(e) for e in f.examples[:5])}</div>' if f.examples else ""
                desc_extra  = examples
                row_class   = "row-fail"
            else:
                status_cell = f'<td class="status pass">PASS</td>'
                row_cell    = f'<td class="num">-</td>'
                desc_extra  = ""
                row_class   = ""

            sev_class = "sev-error" if chk.severity == Severity.ERROR else "sev-warn"
            rows.append(
                f'<tr class="{row_class}">'
                f'<td class="check-name"><code>{chk.name}</code></td>'
                f'<td class="desc">{chk.description}{desc_extra}</td>'
                f'<td><span class="badge {sev_class}">{chk.severity.value}</span></td>'
                f'<td class="tbl-name">{chk.table}</td>'
                f'{status_cell}'
                f'{row_cell}'
                f'</tr>'
            )
        return "\n".join(rows)

    sections_html = ""
    for cat in categories:
        cat_checks = [c for c in checks if c.category == cat]
        if not cat_checks:
            continue
        cat_pass  = sum(1 for c in cat_checks if c.name not in finding_map)
        cat_fail  = len(cat_checks) - cat_pass
        sec_class = "section-fail" if any(
            finding_map[c.name].severity == Severity.ERROR
            for c in cat_checks if c.name in finding_map
        ) else "section-warn" if cat_fail else "section-ok"
        sections_html += f"""
        <section class="{sec_class}">
          <h2>{cat_labels[cat]}
            <span class="cat-summary">{cat_pass}/{len(cat_checks)} passing</span>
          </h2>
          <table>
            <thead>
              <tr>
                <th style="width:22%">Check</th>
                <th>Description</th>
                <th style="width:8%">Severity</th>
                <th style="width:14%">Table</th>
                <th style="width:7%">Status</th>
                <th style="width:6%">Rows</th>
              </tr>
            </thead>
            <tbody>
              {_rows_html(cat)}
            </tbody>
          </table>
        </section>
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TDGEN-TEMPORAL &mdash; Data Integrity Report</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      font-size: 14px;
      background: #f4f6f9;
      color: #1a1a2e;
      padding: 32px 24px;
    }}
    header {{
      max-width: 1200px;
      margin: 0 auto 32px;
    }}
    header h1 {{
      font-size: 26px;
      font-weight: 700;
      letter-spacing: -0.3px;
      color: #0f172a;
    }}
    header .meta {{
      margin-top: 6px;
      color: #64748b;
      font-size: 13px;
    }}
    .summary-cards {{
      max-width: 1200px;
      margin: 0 auto 32px;
      display: grid;
      grid-template-columns: repeat(5, 1fr);
      gap: 16px;
    }}
    .card {{
      background: #fff;
      border-radius: 10px;
      padding: 18px 20px;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      text-align: center;
    }}
    .card .val {{
      font-size: 32px;
      font-weight: 700;
      line-height: 1.1;
    }}
    .card .lbl {{
      font-size: 12px;
      color: #64748b;
      margin-top: 4px;
      text-transform: uppercase;
      letter-spacing: .5px;
    }}
    .card.ok   .val {{ color: #16a34a; }}
    .card.fail .val {{ color: #dc2626; }}
    .card.warn .val {{ color: #d97706; }}
    .card.neutral .val {{ color: #0f172a; }}
    .overall-banner {{
      max-width: 1200px;
      margin: 0 auto 28px;
      border-radius: 10px;
      padding: 14px 22px;
      font-weight: 600;
      font-size: 15px;
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .overall-banner.ok   {{ background: #dcfce7; color: #15803d; border: 1px solid #86efac; }}
    .overall-banner.warn {{ background: #fef9c3; color: #92400e; border: 1px solid #fde047; }}
    .overall-banner.fail {{ background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }}
    section {{
      max-width: 1200px;
      margin: 0 auto 28px;
      background: #fff;
      border-radius: 10px;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      overflow: hidden;
    }}
    section h2 {{
      font-size: 15px;
      font-weight: 600;
      padding: 14px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid #e2e8f0;
    }}
    .section-ok   h2 {{ background: #f0fdf4; color: #15803d; }}
    .section-warn h2 {{ background: #fffbeb; color: #92400e; }}
    .section-fail h2 {{ background: #fff1f2; color: #9f1239; }}
    .cat-summary {{
      font-size: 12px;
      font-weight: 500;
      color: #64748b;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    thead th {{
      background: #f8fafc;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: .5px;
      color: #64748b;
      padding: 8px 14px;
      text-align: left;
      border-bottom: 1px solid #e2e8f0;
    }}
    tbody tr {{
      border-bottom: 1px solid #f1f5f9;
    }}
    tbody tr:last-child {{
      border-bottom: none;
    }}
    tbody tr:hover {{
      background: #f8fafc;
    }}
    tbody tr.row-fail {{
      background: #fff8f8;
    }}
    tbody tr.row-fail:hover {{
      background: #fff1f2;
    }}
    td {{
      padding: 9px 14px;
      vertical-align: top;
    }}
    td code {{
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      font-size: 12px;
      color: #374151;
    }}
    td.desc {{
      color: #475569;
      font-size: 13px;
    }}
    td.tbl-name {{
      font-size: 12px;
      color: #64748b;
      font-family: monospace;
    }}
    td.num {{
      text-align: right;
      color: #94a3b8;
      font-variant-numeric: tabular-nums;
    }}
    td.num.fail-num {{
      color: #dc2626;
      font-weight: 600;
    }}
    .badge {{
      display: inline-block;
      border-radius: 4px;
      padding: 2px 7px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: .3px;
    }}
    .sev-error {{ background: #fee2e2; color: #b91c1c; }}
    .sev-warn  {{ background: #fef3c7; color: #92400e; }}
    .status {{
      font-weight: 700;
      font-size: 12px;
      letter-spacing: .5px;
      text-align: center;
    }}
    .status.pass {{ color: #16a34a; }}
    .status.fail {{ color: #dc2626; }}
    .examples {{
      margin-top: 4px;
      font-size: 11px;
      color: #dc2626;
      font-family: monospace;
    }}
    footer {{
      max-width: 1200px;
      margin: 32px auto 0;
      text-align: center;
      font-size: 12px;
      color: #94a3b8;
    }}
  </style>
</head>
<body>

<header>
  <h1>TDGEN-TEMPORAL &mdash; Data Integrity Report</h1>
  <div class="meta">Database: <strong>{db_path}</strong> &nbsp;&bull;&nbsp; Generated: <strong>{generated_at}</strong></div>
</header>

<div class="overall-banner {overall_class}">
  {'&#10003;' if overall_class == 'ok' else '&#9888;'}&nbsp; {overall_label} &mdash;
  {total_pass} of {total_checks} checks passing
  {f', {total_errors} error(s)' if total_errors else ''}
  {f', {total_warnings} warning(s)' if total_warnings else ''}
</div>

<div class="summary-cards">
  <div class="card neutral"><div class="val">{total_checks}</div><div class="lbl">Total Checks</div></div>
  <div class="card ok"><div class="val">{total_pass}</div><div class="lbl">Passed</div></div>
  <div class="card {'fail' if total_errors else 'ok'}"><div class="val">{total_errors}</div><div class="lbl">Failed (Error)</div></div>
  <div class="card {'warn' if total_warnings else 'ok'}"><div class="val">{total_warnings}</div><div class="lbl">Failed (Warning)</div></div>
  <div class="card {'fail' if total_rows else 'ok'}"><div class="val">{total_rows:,}</div><div class="lbl">Violating Rows</div></div>
</div>

{sections_html}

<footer>TDGEN-TEMPORAL &mdash; Synthetic Test Data Generator &bull; Report generated {generated_at}</footer>

</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
    print(f"  Report written to {output_path}")
