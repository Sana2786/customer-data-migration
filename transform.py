
from pathlib import Path
import re
import unicodedata
import pandas as pd
from pyspark.sql.functions import regexp_replace, col

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

SOURCE_CSV = DATA_DIR / "CustomerExtract.csv"
SPEC_XLSX = DATA_DIR / "CustomerSpec.xlsx"
OUTPUT_CSV = OUTPUT_DIR / "CustomerLoad.csv"
DQ_CSV = OUTPUT_DIR / "data_quality_report.csv"

def load_source():
    # The supplied CSV has the actual header on the first row.
    return pd.read_csv(SOURCE_CSV, dtype=str, keep_default_na=False)

def load_spec():
    return pd.read_excel(SPEC_XLSX, sheet_name="CustomerSpec")

def is_y(series):
    return series.fillna("").astype(str).str.strip().str.upper().eq("Y")

def clean_text(value):
    """Remove punctuation/special characters while retaining Unicode letters/numbers."""
    value = "" if value is None else str(value)
    value = unicodedata.normalize("NFKC", value)
    value = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in value)
    return re.sub(r"\s+", " ", value).strip()

def build_output(df, spec):
    used = spec[is_y(spec["Field Used"])].copy()
    out = pd.DataFrame(index=df.index)

    # Default: blank. This prevents unapproved legacy/source fields from
    # leaking into the SAP load file.
    for field in used["SAP FIELD"].dropna().astype(str).str.strip():
        out[field] = ""

    # Explicit constants from the transformation specification.
    out["BUKRS"] = "G100"
    out["VKORG"] = "G100"
    out["VTWEG"] = "20"
    out["SPART"] = "10"

    # Direct-copy transformations explicitly stated in the specification.
    copy_fields = [
        "KUNNR", "KTOKD", "LAND1", "ORT01", "PSTLZ", "REGIO",
        "SORTL", "STRAS", "TELF1", "AUFSD", "LIFSD", "LOEVM",
        "STCD1", "STCD2", "LZONE", "XZEMP", "VBUND", "STCEG",
        "KATR2", "KATR8", "KATR9", "TXJCD"
    ]
    for field in copy_fields:
        if field in df.columns and field in out.columns:
            out[field] = df[field].astype(str)

    # Name 1: clean punctuation/special characters as required by the
    # assignment while preserving Unicode letters such as Ñ.
    out["NAME1"] = df["NAME1"].map(clean_text)

    # Fields explicitly described as "Copy Existing".
    for field in ["NAME2", "NAME3", "NAME4", "SPERR"]:
        if field in df.columns and field in out.columns:
            out[field] = df[field].astype(str)

    # Both systems use EN; do not pass through the legacy code.
    out["SPRAS"] = "EN"

    # Created on/by are defined by SAP, so they intentionally remain blank.
    out["ERDAT"] = ""
    out["ERNAM"] = ""

    # Vendor mapping is unresolved in the specification ("Pending outcome").
    # Keep it blank until the business decision is approved.
    out["LIFNR"] = ""

    # Condition groups and other new-system fields without an approved
    # source mapping remain blank. This is deliberate and traceable.
    for field in ["KATR1", "KATR3", "KATR4", "KATR5", "KATR6", "KATR7",
                  "KATR10", "KDKG1", "KDKG2", "KDKG3", "KDKG4", "KDKG5",
                  "STCD5"]:
        if field in out.columns:
            out[field] = ""

    # Ensure output follows the specification order.
    ordered = used["SAP FIELD"].dropna().astype(str).str.strip().tolist()
    return out[ordered]

def profile_and_checks(df, spec):
    legacy = set(
        spec.loc[is_y(spec["Field Utilized in LEGACY System"]), "SAP FIELD"]
        .dropna().astype(str).str.strip()
    )
    used = spec[is_y(spec["Field Used"])].copy()

    populated = {
        c: int(df[c].astype(str).str.strip().ne("").sum())
        for c in df.columns
    }

    checks = []

    def add(check, status, count, details):
        checks.append({
            "check": check,
            "status": status,
            "count": int(count),
            "details": details
        })

    add("Row count", "PASS" if len(df) > 0 else "FAIL", len(df),
        f"Loaded {len(df):,} customer records.")

    dupes = int(df["KUNNR"].duplicated(keep=False).sum())
    add("Duplicate KUNNR", "PASS" if dupes == 0 else "FAIL", dupes,
        "Customer Account Number must be unique.")

    missing_kunnr = int(df["KUNNR"].astype(str).str.strip().eq("").sum())
    add("Missing KUNNR", "PASS" if missing_kunnr == 0 else "FAIL", missing_kunnr,
        "Customer Account Number is required.")

    # Required legacy fields based on REQ / REQ PROC in the spec.
    required_rows = spec[
        is_y(spec["Field Utilized in LEGACY System"]) &
        spec["LEGACY REQ"].fillna("").astype(str).str.upper().str.contains("REQ")
    ]
    for _, r in required_rows.iterrows():
        field = str(r["SAP FIELD"]).strip()
        rule = str(r["Selection/Transformation Logic"]).strip().lower()
        if field not in df.columns:
            # A required target field may legitimately be absent from the
            # source when the spec explicitly defines it as a constant or
            # system-generated value.
            if rule.startswith("use company code") or rule.startswith("use sales organization") or "def by system" in rule:
                add(f"Required target field source availability: {field}", "PASS", 0,
                    "Source field is not required because the specification supplies the target value.")
            else:
                add(f"Required source field present: {field}", "FAIL", len(df),
                    "Required by the legacy specification but missing from extract.")
        else:
            miss = int(df[field].astype(str).str.strip().eq("").sum())
            add(f"Required source completeness: {field}",
                "PASS" if miss == 0 else "FAIL", miss,
                f"{miss} blank value(s) found.")

    # Source fields populated but not flagged as legacy-utilized.
    unexpected = [c for c in df.columns if populated[c] > 0 and c not in legacy]
    unexpected_count = sum(populated[c] for c in unexpected)
    add("Populated fields not flagged as LEGACY-utilized",
        "WARN" if unexpected else "PASS", len(unexpected),
        f"{len(unexpected)} source columns contain data although they are not "
        f"flagged 'Y' in the legacy-utilization column. Fields: {', '.join(unexpected)}")

    # Length checks for fields used in the target.
    length_issues = []
    for _, r in used.iterrows():
        field = str(r["SAP FIELD"]).strip()
        if field in df.columns and pd.notna(r["LENGTH"]):
            max_len = int(r["LENGTH"])
            bad = int(df[field].astype(str).str.len().gt(max_len).sum())
            if bad:
                length_issues.append(f"{field}: {bad} value(s) > {max_len}")
    add("Target field length checks",
        "PASS" if not length_issues else "FAIL",
        len(length_issues),
        "No target length violations." if not length_issues else "; ".join(length_issues))

    # Name 1 special-character check.
    name_special = int(df["NAME1"].map(lambda x: bool(re.search(r"[^A-Za-z0-9\s\u00C0-\u024F]", str(x)))).sum())
    add("NAME1 special-character check",
        "WARN" if name_special else "PASS", name_special,
        f"{name_special} NAME1 value(s) contain punctuation/special characters and will be cleaned.")

    # Language check against explicit spec rule.
    non_en = int(df["SPRAS"].astype(str).str.upper().ne("EN").sum())
    add("SPRAS matches target rule EN",
        "WARN" if non_en else "PASS", non_en,
        "Specification states both systems use EN; target is set to EN.")

    # Vendor unresolved.
    vendor_pop = int(df["LIFNR"].astype(str).str.strip().ne("").sum())
    add("Vendor LIFNR unresolved business rule",
        "WARN" if vendor_pop else "PASS", vendor_pop,
        "Vendor mapping is pending a business decision, so populated legacy values are not loaded.")

    # Explicit source/spec conflicts for fields populated but not legacy flagged.
    conflict_fields = []
    for _, r in used.iterrows():
        field = str(r["SAP FIELD"]).strip()
        legacy_flag = str(r["Field Utilized in LEGACY System"]).strip().upper()
        if field in df.columns and populated.get(field, 0) > 0 and legacy_flag != "Y":
            conflict_fields.append(field)
    add("Field Used Y but not LEGACY-utilized and populated",
        "WARN" if conflict_fields else "PASS", len(conflict_fields),
        f"Review before production load: {', '.join(conflict_fields)}" if conflict_fields else "None.")

    return pd.DataFrame(checks)

def main():
    df = load_source()
    spec = load_spec()

    dq = profile_and_checks(df, spec)
    dq.to_csv(DQ_CSV, index=False)

    output = build_output(df, spec)
    output.to_csv(OUTPUT_CSV, index=False)

    return df, spec, dq, output

if __name__ == "__main__":
    df, spec, dq, output = main()
    print("Transformation complete.")
    print(f"Input rows: {len(df):,}")
    print(f"Output rows: {len(output):,}")
    print(f"Output columns: {len(output.columns):,}")
    print(f"CustomerLoad.csv: {OUTPUT_CSV}")
    print(f"Data quality report: {DQ_CSV}")
